"""การเชื่อมต่อฐานข้อมูล

ตอนนี้ใช้ SQLite เพื่อให้รันได้ทันทีโดยไม่ต้องติดตั้งอะไรเพิ่ม
เมื่อย้ายไป PostgreSQL (ตอนที่ Airflow เข้ามาเป็นตัวรันจริง) ให้ตั้ง
environment variable ตัวเดียว โดยไม่ต้องแก้โค้ดส่วนอื่น:

    SOLAR_DATABASE_URL=postgresql+psycopg2://user:pass@localhost/solar
"""

from contextlib import contextmanager
import os
from sqlalchemy import create_engine, event, inspect, text
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


# คอลัมน์ที่เพิ่มเข้ามาหลังจากตารางถูกสร้างไปแล้วในบางเครื่อง
# create_all() สร้างได้เฉพาะตารางใหม่ ไม่เพิ่มคอลัมน์ให้ตารางเดิม
ADDED_COLUMNS = {
    "task": {"dag_run_id": "VARCHAR(250)"},
}


def _add_missing_columns() -> None:
  inspector = inspect(engine)
  with engine.begin() as conn:
    for table, columns in ADDED_COLUMNS.items():
      if not inspector.has_table(table):
        continue
      existing = {c["name"] for c in inspector.get_columns(table)}
      for name, sql_type in columns.items():
        if name not in existing:
          conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {name} {sql_type}"))
          print(f"[db] เพิ่มคอลัมน์ {table}.{name}")


def init_db() -> None:
  """สร้างตารางทั้งหมดถ้ายังไม่มี และเพิ่มคอลัมน์ที่เพิ่มมาทีหลัง"""
  Base.metadata.create_all(bind=engine)
  _add_missing_columns()


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
