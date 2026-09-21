"""เทสต์ระบบบัญชีผู้ใช้และการแยกข้อมูลระหว่างผู้ใช้

เน้นสองเรื่อง: รหัสผ่านถูกเก็บเป็น hash จริง และผู้ใช้คนหนึ่งเข้าถึงงาน
ของอีกคนไม่ได้ไม่ว่าทางใด
"""

import unittest
import uuid

from fastapi.testclient import TestClient

import auth
import pipeline
from app import app
from db import session_scope
from models import Task, User


def _email(prefix: str) -> str:
  return f"{prefix}-{uuid.uuid4().hex[:8]}@example.com"


def _client() -> TestClient:
  """TestClient หนึ่งตัว = หนึ่งเบราว์เซอร์ (เก็บ cookie แยกกัน)"""
  return TestClient(app)


AOI = {
    "min_lat": 14.0700,
    "min_lng": 100.6020,
    "max_lat": 14.0740,
    "max_lng": 100.6070,
    "zoom": 18,
}


class AuthTestCase(unittest.TestCase):
  def tearDown(self):
    with session_scope() as session:
      for task in session.query(Task).all():
        pipeline.delete_task_files(task.tid)
        session.delete(task)
      for user in session.query(User).all():
        session.delete(user)

  @staticmethod
  def register(client, prefix="user", password="correct-horse-1"):
    email = _email(prefix)
    response = client.post(
        "/auth/register",
        json={"name": "ผู้ใช้ทดสอบ", "email": email, "password": password},
    )
    assert response.status_code == 200, response.text
    return email, password, response.json()


class TestPasswordHashing(AuthTestCase):
  def test_password_is_not_stored_in_plain_text(self):
    raw = "s3cret-password"
    email, _, user = self.register(_client(), password=raw)

    with session_scope() as session:
      stored = session.get(User, user["uid"]).passwd

    self.assertNotIn(raw, stored)
    self.assertTrue(stored.startswith("$2"), "ควรเป็น hash ของ bcrypt")
    self.assertTrue(auth.verify_password(raw, stored))
    self.assertFalse(auth.verify_password(raw + "x", stored))

  def test_long_and_thai_passwords_work(self):
    """bcrypt ตัดที่ 72 ไบต์ โค้ดจึงย่อด้วย sha256 ก่อน — ต้องไม่ชนเพดาน"""
    thai = "รหัสผ่านภาษาไทยที่ยาวมากเกินเจ็ดสิบสองไบต์แน่นอนเลยทีเดียว"
    self.assertGreater(len(thai.encode("utf-8")), 72)

    hashed = auth.hash_password(thai)
    self.assertTrue(auth.verify_password(thai, hashed))
    # ถ้าถูกตัดที่ 72 ไบต์ สองค่านี้จะถือว่าเท่ากัน ซึ่งต้องไม่เกิดขึ้น
    self.assertFalse(auth.verify_password(thai + "เพิ่ม", hashed))

  def test_verify_rejects_garbage_hash(self):
    self.assertFalse(auth.verify_password("anything", "ไม่ใช่ hash"))
    self.assertFalse(auth.verify_password("anything", ""))


class TestRegistrationAndLogin(AuthTestCase):
  def test_register_logs_the_user_in(self):
    client = _client()
    _, _, user = self.register(client)
    me = client.get("/auth/me")
    self.assertEqual(me.status_code, 200)
    self.assertEqual(me.json()["uid"], user["uid"])

  def test_duplicate_email_is_rejected(self):
    client = _client()
    email, password, _ = self.register(client)
    again = client.post(
        "/auth/register",
        json={"name": "คนอื่น", "email": email, "password": password},
    )
    self.assertEqual(again.status_code, 409)

  def test_email_is_case_insensitive(self):
    client = _client()
    email, password, _ = self.register(client)
    client.post("/auth/logout")

    response = client.post(
        "/auth/login", json={"email": email.upper(), "password": password}
    )
    self.assertEqual(response.status_code, 200)

  def test_wrong_password_is_rejected(self):
    client = _client()
    email, _, _ = self.register(client)
    response = client.post(
        "/auth/login", json={"email": email, "password": "wrong-password"}
    )
    self.assertEqual(response.status_code, 401)

  def test_unknown_email_gives_the_same_error(self):
    """ข้อความต้องไม่บอกว่าอีเมลนี้มีอยู่จริงหรือไม่"""
    response = _client().post(
        "/auth/login",
        json={"email": _email("ghost"), "password": "whatever-123"},
    )
    self.assertEqual(response.status_code, 401)
    self.assertIn("อีเมลหรือรหัสผ่าน", response.json()["detail"])

  def test_short_password_is_rejected(self):
    response = _client().post(
        "/auth/register",
        json={"name": "สั้น", "email": _email("short"), "password": "1234"},
    )
    self.assertEqual(response.status_code, 422)

  def test_logout_clears_the_session(self):
    client = _client()
    self.register(client)
    self.assertEqual(client.post("/auth/logout").status_code, 200)
    self.assertEqual(client.get("/auth/me").status_code, 401)


class TestTaskIsolation(AuthTestCase):
  def test_task_endpoints_require_login(self):
    client = _client()
    self.assertEqual(client.get("/tasks").status_code, 401)
    self.assertEqual(client.post("/tasks", json=dict(AOI, title="x")).status_code, 401)
    self.assertEqual(client.get("/tasks/whatever").status_code, 401)
    self.assertEqual(client.get("/tasks/whatever/progress").status_code, 401)
    self.assertEqual(client.delete("/tasks/whatever").status_code, 401)

  def test_task_is_owned_by_its_creator(self):
    client = _client()
    _, _, user = self.register(client)

    tid = client.post("/tasks", json=dict(AOI, title="งานของฉัน")).json()["tid"]
    with session_scope() as session:
      self.assertEqual(session.get(Task, tid).uid, user["uid"])

  def test_list_shows_only_own_tasks(self):
    alice, bob = _client(), _client()
    self.register(alice, "alice")
    self.register(bob, "bob")

    alice.post("/tasks", json=dict(AOI, title="งานของ alice"))
    bob.post("/tasks", json=dict(AOI, title="งานของ bob"))

    self.assertEqual(
        [t["title"] for t in alice.get("/tasks").json()], ["งานของ alice"]
    )
    self.assertEqual(
        [t["title"] for t in bob.get("/tasks").json()], ["งานของ bob"]
    )

  def test_other_users_task_looks_missing(self):
    """ตอบ 404 ไม่ใช่ 403 เพื่อไม่ให้เดาได้ว่า tid นี้มีอยู่จริง"""
    alice, bob = _client(), _client()
    self.register(alice, "alice")
    self.register(bob, "bob")

    tid = alice.post("/tasks", json=dict(AOI, title="ความลับ")).json()["tid"]

    for path in (
        f"/tasks/{tid}",
        f"/tasks/{tid}/progress",
        f"/tasks/{tid}/geojson",
        f"/tasks/{tid}/satellite",
        f"/tasks/{tid}/overlay",
        f"/tasks/{tid}/overlay/meta",
    ):
      self.assertEqual(bob.get(path).status_code, 404, path)

    self.assertEqual(bob.delete(f"/tasks/{tid}").status_code, 404)

    # งานของ alice ต้องยังอยู่ครบหลังจาก bob พยายามลบ
    self.assertEqual(alice.get(f"/tasks/{tid}").status_code, 200)

  def test_session_rejected_after_user_is_deleted(self):
    client = _client()
    _, _, user = self.register(client)

    with session_scope() as session:
      session.delete(session.get(User, user["uid"]))

    self.assertEqual(client.get("/auth/me").status_code, 401)
    self.assertEqual(client.get("/tasks").status_code, 401)


class TestPublicEndpoints(AuthTestCase):
  def test_dashboard_and_health_stay_public(self):
    client = _client()
    self.assertEqual(client.get("/").status_code, 200)
    self.assertEqual(client.get("/health").status_code, 200)


if __name__ == "__main__":
  unittest.main()
