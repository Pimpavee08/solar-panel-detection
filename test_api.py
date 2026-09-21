"""เทสต์ REST API และตัวเดิน pipeline

ใช้ฐานข้อมูลชั่วคราวแยกต่างหาก จึงไม่แตะข้อมูลจริงใน data/solar_jobs.db
ต้องตั้ง SOLAR_DATABASE_URL ก่อน import โมดูลของโปรเจกต์ เพราะ db.py
อ่านค่านี้ตอน import เพื่อสร้าง engine
"""

import json
import os
import shutil
import tempfile
import unittest
import uuid
from unittest.mock import patch

# ปกติ conftest.py ตั้งค่านี้ให้แล้ว บรรทัดนี้เผื่อกรณีรันไฟล์นี้ตรง ๆ
_TMP_DIR = tempfile.mkdtemp(prefix="solar_test_")
os.environ.setdefault(
    "SOLAR_DATABASE_URL",
    "sqlite:///" + os.path.join(_TMP_DIR, "test.db").replace("\\", "/"),
)

from fastapi.testclient import TestClient  # noqa: E402

import overlay  # noqa: E402
import airflow_client  # noqa: E402
import pipeline  # noqa: E402
from app import app  # noqa: E402
from db import session_scope  # noqa: E402
from models import (  # noqa: E402
    User,
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
  """ลบ Task และผู้ใช้ทั้งหมดที่เทสต์สร้างไว้ พร้อมโฟลเดอร์บนดิสก์"""
  with session_scope() as session:
    for task in session.query(Task).all():
      pipeline.delete_task_files(task.tid)
      session.delete(task)
    for user in session.query(User).all():
      session.delete(user)


class TestSolarAPI(unittest.TestCase):
  def setUp(self):
    # ทุก endpoint ของ /tasks ต้องล็อกอินก่อน (ดู test_auth.py)
    # TestClient เก็บ session cookie ให้เอง
    self.client = TestClient(app)
    response = self.client.post(
        "/auth/register",
        json={
            "name": "ผู้ใช้ทดสอบ",
            "email": f"api-{uuid.uuid4().hex[:8]}@example.com",
            "password": "test-password-1",
        },
    )
    self.assertEqual(response.status_code, 200, response.text)

  def tearDown(self):
    _clear_tasks()

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
    self.assertEqual(self.client.get(f"/tasks/{tid}/overlay").status_code, 404)
    self.assertEqual(
        self.client.get(f"/tasks/{tid}/overlay/meta").status_code, 404
    )


class TestLogAndRerun(unittest.TestCase):
  """endpoint ที่ดึง log จาก Airflow และสั่งรันใหม่"""

  def setUp(self):
    self.client = TestClient(app)
    self.client.post(
        "/auth/register",
        json={
            "name": "ผู้ใช้ทดสอบ",
            "email": f"log-{uuid.uuid4().hex[:8]}@example.com",
            "password": "test-password-1",
        },
    )
    with patch("fastapi.BackgroundTasks.add_task"):
      self.tid = self.client.post("/tasks", json=VALID_PAYLOAD).json()["tid"]

  def tearDown(self):
    _clear_tasks()

  def test_unknown_step_name_is_404(self):
    response = self.client.get(f"/tasks/{self.tid}/steps/ไม่มีขั้นนี้/log")
    self.assertEqual(response.status_code, 404)

  def test_log_explains_there_is_none_without_airflow(self):
    """เทสต์รันในโหมดที่เว็บรัน pipeline เอง จึงไม่มี log แยกราย step"""
    response = self.client.get(f"/tasks/{self.tid}/steps/run_inference/log")
    self.assertEqual(response.status_code, 404)
    self.assertIn("โปรเซสของเว็บ", response.json()["detail"])

  def test_rerun_resets_steps_and_clears_result(self):
    # ทำให้ดูเหมือนงานที่รันจบแล้วและล้มเหลว
    pipeline.mark_step_completed(self.tid, STEP_NAMES[0])
    pipeline.mark_step_failed(self.tid, STEP_NAMES[1], "พังไปแล้ว")
    pipeline._save_result(
        self.tid, {"total_panels": 5, "total_surface_area_sqm": 1.0}, None
    )

    with patch("fastapi.BackgroundTasks.add_task") as add_task:
      response = self.client.post(f"/tasks/{self.tid}/rerun")

    self.assertEqual(response.status_code, 202)
    self.assertEqual(response.json()["executor"], "background")
    add_task.assert_called_once()

    with session_scope() as session:
      steps = session.query(TaskStep).filter_by(tid=self.tid).all()
      for step in steps:
        self.assertEqual(step.status, STATUS_PENDING)
        self.assertEqual(step.retry_count, 0)
        self.assertIsNone(step.error_msg)
        self.assertIsNone(step.started_at)
      # ตัวเลขของรอบที่แล้วต้องหายไป ไม่ค้างมาปนกับรอบใหม่
      self.assertIsNone(
          session.query(TaskResult).filter_by(tid=self.tid).one_or_none()
      )

  def test_rerun_refuses_while_running(self):
    pipeline.mark_step_running(self.tid, STEP_NAMES[1])
    response = self.client.post(f"/tasks/{self.tid}/rerun")
    self.assertEqual(response.status_code, 409)

  def test_rerun_requires_login(self):
    self.assertEqual(
        TestClient(app).post(f"/tasks/{self.tid}/rerun").status_code, 401
    )


class TestAirflowRunIds(unittest.TestCase):
  def test_first_run_id_is_stable_but_rerun_is_not(self):
    """ชื่อคงที่กันสั่งซ้ำ ส่วนรันใหม่ต้องได้ชื่อใหม่ ไม่งั้น Airflow จะตอบ 409"""
    tid = "abc-123"
    self.assertEqual(airflow_client.run_id_for(tid), f"task__{tid}")
    self.assertEqual(airflow_client.run_id_for(tid), f"task__{tid}")

    rerun = airflow_client.run_id_for(tid, rerun=True)
    self.assertNotEqual(rerun, f"task__{tid}")
    self.assertTrue(rerun.startswith(f"task__{tid}__r"))


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


class TestOverlay(unittest.TestCase):
  """เทสต์ overlay.py ด้วยภาพจำลอง จึงไม่ต้องดาวน์โหลดภาพจริง"""

  BBOX = (100.602, 14.070, 100.607, 14.074)  # west, south, east, north

  def setUp(self):
    self.dir = tempfile.mkdtemp(prefix="solar_overlay_")
    self.tif = os.path.join(self.dir, "satellite.tif")
    self.geojson = os.path.join(self.dir, "result.geojson")
    self.png = os.path.join(self.dir, "overlay.png")
    self._write_raster()

  def tearDown(self):
    shutil.rmtree(self.dir, ignore_errors=True)

  def _write_raster(self):
    """ภาพ 3 แบนด์สีเทาล้วน ใน EPSG:3857 ครอบ bbox ที่กำหนด"""
    import numpy as np
    import rasterio
    from rasterio.transform import from_bounds
    from rasterio.warp import transform_bounds

    west, south, east, north = transform_bounds(
        "EPSG:4326", "EPSG:3857", *self.BBOX
    )
    size = 100
    with rasterio.open(
        self.tif, "w", driver="GTiff", width=size, height=size, count=3,
        dtype="uint8", crs="EPSG:3857",
        transform=from_bounds(west, south, east, north, size, size),
    ) as dst:
      band = np.full((size, size), 128, dtype="uint8")
      for i in range(3):
        dst.write(band, i + 1)

  def _write_geojson(self, features):
    with open(self.geojson, "w", encoding="utf-8") as f:
      json.dump({"type": "FeatureCollection", "features": features}, f)

  @staticmethod
  def _square(west, south, east, north):
    ring = [[west, south], [east, south], [east, north], [west, north],
            [west, south]]
    return {"type": "Feature", "properties": {},
            "geometry": {"type": "Polygon", "coordinates": [ring]}}

  def test_draws_polygon_and_writes_sidecars(self):
    from PIL import Image

    self._write_geojson([self._square(100.6035, 14.0715, 100.6050, 14.0730)])
    meta = overlay.create_overlay(self.tif, self.geojson, self.png)

    self.assertEqual(meta["polygon_count"], 1)
    self.assertTrue(os.path.exists(self.png))
    self.assertTrue(os.path.exists(os.path.join(self.dir, "overlay.pgw")))
    self.assertTrue(os.path.exists(os.path.join(self.dir, "overlay.json")))

    # ขอบเขตที่คืนมาต้องอยู่ในรูปแบบของ Leaflet: [[south, west], [north, east]]
    (south, west), (north, east) = meta["bounds"]
    self.assertLess(south, north)
    self.assertLess(west, east)
    self.assertAlmostEqual(west, self.BBOX[0], places=4)
    self.assertAlmostEqual(north, self.BBOX[3], places=4)

    # พื้นหลังสีเทา 128 ล้วน ดังนั้นต้องมีพิกเซลที่เปลี่ยนไปจากการวาดทับ
    colors = {c for _, c in Image.open(self.png).getcolors(maxcolors=100000)}
    self.assertGreater(len(colors), 1)

  def test_handles_empty_detection(self):
    """ตรวจไม่พบแผงเลย ก็ยังต้องได้ภาพออกมา ไม่ใช่ error"""
    self._write_geojson([])
    meta = overlay.create_overlay(self.tif, self.geojson, self.png)
    self.assertEqual(meta["polygon_count"], 0)
    self.assertTrue(os.path.exists(self.png))

  def test_missing_raster_raises(self):
    self._write_geojson([])
    with self.assertRaises(FileNotFoundError):
      overlay.create_overlay(
          os.path.join(self.dir, "nope.tif"), self.geojson, self.png
      )


if __name__ == "__main__":
  unittest.main()
