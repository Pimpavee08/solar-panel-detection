"""เทสต์ REST API และตัวเดิน pipeline

ใช้ฐานข้อมูลชั่วคราวแยกต่างหาก จึงไม่แตะข้อมูลจริงใน data/solar_jobs.db
ต้องตั้ง SOLAR_DATABASE_URL ก่อน import โมดูลของโปรเจกต์ เพราะ db.py
อ่านค่านี้ตอน import เพื่อสร้าง engine
"""

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

_TMP_DIR = tempfile.mkdtemp(prefix="solar_test_")
os.environ["SOLAR_DATABASE_URL"] = "sqlite:///" + os.path.join(
    _TMP_DIR, "test.db"
).replace("\\", "/")

from fastapi.testclient import TestClient  # noqa: E402

import pipeline  # noqa: E402
from app import app  # noqa: E402
from db import session_scope  # noqa: E402
from models import (  # noqa: E402
    MAX_RETRY,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_PENDING,
    STEP_GENERATING_CONFIG,
    STEP_NAMES,
    Task,
    TaskResult,
    TaskStep,
)

VALID_PAYLOAD = {
    "title": "งานทดสอบ SIIT",
    "min_lat": 14.0700,
    "min_lng": 100.6020,
    "max_lat": 14.0740,
    "max_lng": 100.6070,
    "zoom": 18,
}


def _clear_tasks():
  """ลบ Task ทั้งหมดที่เทสต์สร้างไว้ พร้อมโฟลเดอร์บนดิสก์"""
  with session_scope() as session:
    for task in session.query(Task).all():
      pipeline.delete_task_files(task.tid)
      session.delete(task)


class TestSolarAPI(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.client = TestClient(app)

  def tearDown(self):
    _clear_tasks()

  @classmethod
  def tearDownClass(cls):
    shutil.rmtree(_TMP_DIR, ignore_errors=True)

  # ---------- หน้าเว็บ ----------
  def test_read_root(self):
    """รูทพาท (/) ต้องคืนหน้า Dashboard เป็น HTML"""
    response = self.client.get("/")
    self.assertEqual(response.status_code, 200)
    self.assertIn("text/html", response.headers["content-type"])
    self.assertIn('id="map"', response.text)

  # ---------- สร้าง Task ----------
  @patch("fastapi.BackgroundTasks.add_task")
  def test_create_task_success(self, mock_add_task):
    """สร้าง Task แล้วต้องได้ tid กลับมา และสั่งรัน pipeline เบื้องหลัง"""
    response = self.client.post("/tasks", json=VALID_PAYLOAD)
    self.assertEqual(response.status_code, 202)

    data = response.json()
    self.assertEqual(data["title"], VALID_PAYLOAD["title"])
    self.assertEqual(data["status"], "generating_config:pending")
    self.assertEqual(len(data["tid"]), 36)  # uuid4 ในรูปแบบ string
    mock_add_task.assert_called_once()

  @patch("fastapi.BackgroundTasks.add_task")
  def test_create_task_makes_four_pending_steps(self, _mock):
    """ER ระบุ Task : Task_Step = 1 : 4 จึงต้องมี 4 แถวตั้งแต่ตอนสร้าง"""
    tid = self.client.post("/tasks", json=VALID_PAYLOAD).json()["tid"]

    response = self.client.get(f"/tasks/{tid}/progress")
    self.assertEqual(response.status_code, 200)

    body = response.json()
    self.assertEqual(body["percent"], 0)
    self.assertEqual([s["step_name"] for s in body["steps"]], STEP_NAMES)
    for step in body["steps"]:
      self.assertEqual(step["status"], STATUS_PENDING)
      self.assertEqual(step["retry_count"], 0)

  @patch("fastapi.BackgroundTasks.add_task")
  def test_create_task_rejects_bad_latitude(self, _mock):
    payload = dict(VALID_PAYLOAD, min_lat=14.0750, max_lat=14.0740)
    response = self.client.post("/tasks", json=payload)
    self.assertEqual(response.status_code, 400)
    self.assertIn("max_lat", response.json()["detail"])

  @patch("fastapi.BackgroundTasks.add_task")
  def test_create_task_rejects_bad_longitude(self, _mock):
    payload = dict(VALID_PAYLOAD, min_lng=100.6080, max_lng=100.6070)
    response = self.client.post("/tasks", json=payload)
    self.assertEqual(response.status_code, 400)
    self.assertIn("max_lng", response.json()["detail"])

  def test_create_task_rejects_empty_title(self):
    payload = dict(VALID_PAYLOAD, title="")
    self.assertEqual(self.client.post("/tasks", json=payload).status_code, 422)

  # ---------- อ่าน / ลบ ----------
  def test_list_tasks(self):
    response = self.client.get("/tasks")
    self.assertEqual(response.status_code, 200)
    self.assertIsInstance(response.json(), list)

  def test_get_nonexistent_task(self):
    response = self.client.get("/tasks/does-not-exist")
    self.assertEqual(response.status_code, 404)
    self.assertIn("ไม่พบ Task", response.json()["detail"])

  def test_progress_nonexistent_task(self):
    self.assertEqual(
        self.client.get("/tasks/does-not-exist/progress").status_code, 404
    )

  def test_delete_nonexistent_task(self):
    self.assertEqual(self.client.delete("/tasks/does-not-exist").status_code, 404)

  @patch("fastapi.BackgroundTasks.add_task")
  def test_delete_task_cascades(self, _mock):
    """ลบ Task แล้ว Task_Step ต้องหายตามไปด้วย"""
    tid = self.client.post("/tasks", json=VALID_PAYLOAD).json()["tid"]

    self.assertEqual(self.client.delete(f"/tasks/{tid}").status_code, 200)
    self.assertEqual(self.client.get(f"/tasks/{tid}").status_code, 404)

    with session_scope() as session:
      self.assertEqual(session.query(TaskStep).filter_by(tid=tid).count(), 0)

  @patch("fastapi.BackgroundTasks.add_task")
  def test_downloads_404_before_pipeline_runs(self, _mock):
    """ยังไม่ได้รัน pipeline จึงยังไม่มีไฟล์ให้ดาวน์โหลด"""
    tid = self.client.post("/tasks", json=VALID_PAYLOAD).json()["tid"]
    self.assertEqual(self.client.get(f"/tasks/{tid}/geojson").status_code, 404)
    self.assertEqual(self.client.get(f"/tasks/{tid}/satellite").status_code, 404)


class TestPipelineSteps(unittest.TestCase):
  """เทสต์กฎการ retry ตามเอกสาร AW3_Database_Design"""

  def tearDown(self):
    _clear_tasks()

  @staticmethod
  def _make_task(title="งานทดสอบ retry"):
    return pipeline.create_task(title, 14.070, 100.602, 14.074, 100.607, 18)

  def test_retries_five_times_then_fails(self):
    """ล้มเหลวซ้ำ ๆ ต้อง retry จนครบ 5 ครั้ง แล้วจึงหยุดและบันทึก error_msg"""
    tid = self._make_task()
    calls = []

    def always_fail(_tid, _task):
      calls.append(1)
      raise RuntimeError("จำลองความล้มเหลว")

    original = pipeline.STEP_FUNCTIONS[STEP_GENERATING_CONFIG]
    pipeline.STEP_FUNCTIONS[STEP_GENERATING_CONFIG] = always_fail
    try:
      pipeline.run_task(tid)
    finally:
      pipeline.STEP_FUNCTIONS[STEP_GENERATING_CONFIG] = original

    self.assertEqual(len(calls), MAX_RETRY)

    with session_scope() as session:
      task = session.get(Task, tid)
      self.assertEqual(task.status, f"{STEP_GENERATING_CONFIG}:{STATUS_FAILED}")

      step = (
          session.query(TaskStep)
          .filter_by(tid=tid, step_name=STEP_GENERATING_CONFIG)
          .one()
      )
      self.assertEqual(step.status, STATUS_FAILED)
      self.assertEqual(step.retry_count, MAX_RETRY)
      self.assertIn("จำลองความล้มเหลว", step.error_msg)

      # step ถัดไปต้องไม่ถูกแตะเลย เพราะ pipeline หยุดทันที
      later = (
          session.query(TaskStep).filter_by(tid=tid, step_name=STEP_NAMES[1]).one()
      )
      self.assertEqual(later.status, STATUS_PENDING)

      # ยังไม่สำเร็จ จึงต้องไม่มี Task_Result
      self.assertIsNone(
          session.query(TaskResult).filter_by(tid=tid).one_or_none()
      )

  def test_recovers_when_retry_succeeds(self):
    """ล้มเหลว 2 ครั้งแล้วผ่าน ต้องจบเป็น completed และเก็บจำนวนครั้งที่ลองไว้"""
    tid = self._make_task()
    attempts = {"n": 0}

    def fail_twice(_tid, _task):
      attempts["n"] += 1
      if attempts["n"] <= 2:
        raise RuntimeError("ล้มเหลวชั่วคราว")

    originals = dict(pipeline.STEP_FUNCTIONS)
    pipeline.STEP_FUNCTIONS[STEP_GENERATING_CONFIG] = fail_twice
    for name in STEP_NAMES[1:]:
      pipeline.STEP_FUNCTIONS[name] = lambda _tid, _task: None
    try:
      pipeline.run_task(tid)
    finally:
      pipeline.STEP_FUNCTIONS.update(originals)

    self.assertEqual(attempts["n"], 3)

    with session_scope() as session:
      task = session.get(Task, tid)
      self.assertEqual(task.status, f"{STEP_NAMES[-1]}:{STATUS_COMPLETED}")

      step = (
          session.query(TaskStep)
          .filter_by(tid=tid, step_name=STEP_GENERATING_CONFIG)
          .one()
      )
      self.assertEqual(step.status, STATUS_COMPLETED)
      self.assertEqual(step.retry_count, 2)
      self.assertIsNotNone(step.started_at)
      self.assertIsNotNone(step.completed_at)


if __name__ == "__main__":
  unittest.main()
