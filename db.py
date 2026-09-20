"""การเชื่อมต่อฐานข้อมูล

ตอนนี้ใช้ SQLite เพื่อให้รันได้ทันทีโดยไม่ต้องติดตั้งอะไรเพิ่ม
เมื่อย้ายไป PostgreSQL (ตอนที่ Airflow เข้ามาเป็นตัวรันจริง) ให้ตั้ง
environment variable ตัวเดียว โดยไม่ต้องแก้โค้ดส่วนอื่น:

    SOLAR_DATABASE_URL=postgresql+psycopg2://user:pass@localhost/solar
"""

from contextlib import contextmanager
import os
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from models import Base

DEFAULT_SQLITE_PATH = os.path.join("data", "solar_jobs.db")
DATABASE_URL = os.environ.get(
    "SOLAR_DATABASE_URL", f"sqlite:///{DEFAULT_SQLITE_PATH}"
)

_is_sqlite = DATABASE_URL.startswith("sqlite")

if _is_sqlite:
  os.makedirs(os.path.dirname(DEFAULT_SQLITE_PATH), exist_ok=True)

engine = create_engine(
    DATABASE_URL,
    future=True,
    # FastAPI BackgroundTasks รันคนละ thread กับตัวที่สร้าง connection
    connect_args={"check_same_thread": False} if _is_sqlite else {},
)

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)


if _is_sqlite:

  @event.listens_for(engine, "connect")
  def _sqlite_pragmas(dbapi_conn, _record):
    """WAL ให้อ่านพร้อมเขียนได้ และ busy_timeout กันโดน 'database is locked'"""
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def init_db() -> None:
  """สร้างตารางทั้งหมดถ้ายังไม่มี (ไม่แตะตาราง job_results ของเวอร์ชันเดิม)"""
  Base.metadata.create_all(bind=engine)


@contextmanager
def session_scope():
  """เปิด session พร้อม commit/rollback อัตโนมัติ"""
  session = SessionLocal()
  try:
    yield session
    session.commit()
  except Exception:
    session.rollback()
    raise
  finally:
    session.close()
