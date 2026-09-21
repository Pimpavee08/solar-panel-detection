"""ตั้งค่าที่ต้องทำก่อน import โมดูลของโปรเจกต์ในทุกไฟล์เทสต์

db.py อ่าน SOLAR_DATABASE_URL ตอน import เพื่อสร้าง engine ครั้งเดียว
ถ้าปล่อยให้แต่ละไฟล์เทสต์ตั้งค่าเอง ไฟล์ที่ถูก import ทีหลังจะไม่มีผล
จึงย้ายมาตั้งที่นี่ เพราะ pytest โหลด conftest.py ก่อนไฟล์เทสต์เสมอ
"""

import os
import shutil
import tempfile

TEST_DB_DIR = tempfile.mkdtemp(prefix="solar_pytest_")
os.environ.setdefault(
    "SOLAR_DATABASE_URL",
    "sqlite:///" + os.path.join(TEST_DB_DIR, "test.db").replace("\\", "/"),
)
# กันไม่ให้เทสต์ไปสั่งงาน Airflow จริงโดยบังเอิญ
os.environ.pop("SOLAR_AIRFLOW_URL", None)


import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_login_rate_limit():
  """เทสต์ทุกตัวยิงมาจาก IP เดียวกัน ถ้าไม่ล้างตัวนับจะไปชนเพดานของตัวถัดไป"""
  import auth

  with auth._attempts_lock:
    auth._attempts.clear()
  yield


def pytest_sessionfinish(session, exitstatus):
  shutil.rmtree(TEST_DB_DIR, ignore_errors=True)
