import os
import shutil
import unittest
import sqlite3
from fastapi.testclient import TestClient
from unittest.mock import patch

from app import app, DB_PATH

class TestSolarAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)
        cls.test_job_name = "test_api_job"

    def setUp(self):
        # ทำความสะอาดก่อนรันแต่ละเทส
        self.clean_up_job()

    def tearDown(self):
        # ทำความสะอาดหลังรันเสร็จ
        self.clean_up_job()

    def clean_up_job(self):
        if os.path.exists(DB_PATH):
            try:
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute("DELETE FROM job_results WHERE job_name = ?", (self.test_job_name,))
                conn.commit()
                conn.close()
            except Exception:
                pass
        
        job_dir = os.path.join("data/inference", self.test_job_name)
        if os.path.exists(job_dir):
            try:
                shutil.rmtree(job_dir)
            except Exception:
                pass

    def test_read_root(self):
        """ตรวจสอบความถูกต้องของรูทพาท (/)"""
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "online")
        self.assertIn("endpoints", response.json())

    @patch("fastapi.BackgroundTasks.add_task")
    def test_create_job_success(self, mock_add_task):
        """ตรวจสอบการส่งพารามิเตอร์รัน Job และการทำงานใน Background Task"""
        payload = {
            "job_name": self.test_job_name,
            "min_lat": 14.0700,
            "min_lon": 100.6020,
            "max_lat": 14.0740,
            "max_lon": 100.6070,
            "zoom": 18
        }
        response = self.client.post("/jobs", json=payload)
        self.assertEqual(response.status_code, 202)
        data = response.json()
        self.assertEqual(data["job_name"], self.test_job_name)
        self.assertEqual(data["status"], "running")
        
        # ตรวจสอบว่าแอดงานเข้า background task สำเร็จ
        mock_add_task.assert_called_once()

    def test_create_job_invalid_coordinates(self):
        """ตรวจสอบว่าแอปจะปฏิเสธกรณีใส่ค่าพิกัดละติจูด max น้อยกว่าหรือเท่ากับ min"""
        payload = {
            "job_name": self.test_job_name,
            "min_lat": 14.0750,  # มากกว่า max
            "min_lon": 100.6020,
            "max_lat": 14.0740,
            "max_lon": 100.6070,
            "zoom": 18
        }
        response = self.client.post("/jobs", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertIn("max_lat", response.json()["detail"])

    def test_get_nonexistent_job(self):
        """ตรวจสอบความถูกต้องของ HTTP 404 เมื่อดึง Job ที่ไม่มีจริง"""
        response = self.client.get(f"/jobs/{self.test_job_name}")
        self.assertEqual(response.status_code, 404)
        self.assertIn("ไม่พบ Job ชื่อ", response.json()["detail"])

    def test_list_jobs(self):
        """ตรวจสอบลิสต์งานทั้งหมดว่าสามารถดึงได้ถูกต้องในรูปแบบลิสต์"""
        response = self.client.get("/jobs")
        self.assertEqual(response.status_code, 200)
        self.assertIsInstance(response.json(), list)

    def test_delete_nonexistent_job(self):
        """ตรวจสอบกรณีเรียกลบงานที่ไม่มีจริง"""
        response = self.client.delete(f"/jobs/nonexistent_job_{self.test_job_name}")
        self.assertEqual(response.status_code, 404)
