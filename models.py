"""SQLAlchemy models ตาม ER Diagram ในเอกสาร AW3_Database_Design

ความสัมพันธ์:
  User  1 --create--  N  Task
  Task  1 --has--     4  Task_Step   (หนึ่งแถวต่อหนึ่ง step ไม่ append เพิ่ม)
  Task  1 --produce-- 0..1 Task_Result
"""

from datetime import datetime
import uuid
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


def new_uuid() -> str:
  """SQLite ไม่มีชนิด uuid ในตัว จึงเก็บเป็น string 36 ตัวอักษร"""
  return str(uuid.uuid4())


# -------------------------------------------------------------
# ค่าคงที่ของ Workflow (ตามเอกสารหน้า 1 และ note หน้า 2)
# -------------------------------------------------------------
STEP_GENERATING_CONFIG = "generating_config"
STEP_FETCH_IMAGE = "fetch_image"
STEP_RUN_INFERENCE = "run_inference"
STEP_PARSE_RESULT = "parse_result"

# ลำดับการทำงาน — สร้าง config ก่อนดึงภาพ เพราะ path ของ input folder อยู่ในไฟล์ config
STEP_NAMES = [
    STEP_GENERATING_CONFIG,
    STEP_FETCH_IMAGE,
    STEP_RUN_INFERENCE,
    STEP_PARSE_RESULT,
]

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"

MAX_RETRY = 5


def task_status(step_name: str, status: str) -> str:
  """Task.status = step_name + ":" + status  (เช่น "run_inference:running")"""
  return f"{step_name}:{status}"


# -------------------------------------------------------------
# ตาราง
# -------------------------------------------------------------
class User(Base):
  __tablename__ = "user"

  uid = Column(String(36), primary_key=True, default=new_uuid)
  # เก็บเฉพาะค่า hash เท่านั้น ห้ามเก็บรหัสผ่านดิบ
  passwd = Column(String(255), nullable=False)
  name = Column(String(120), nullable=False)
  email = Column(String(255), nullable=False, unique=True)
  created_at = Column(DateTime, nullable=False, default=datetime.now)

  tasks = relationship("Task", back_populates="user")


class Task(Base):
  __tablename__ = "task"

  tid = Column(String(36), primary_key=True, default=new_uuid)
  uid = Column(String(36), ForeignKey("user.uid"), nullable=True)
  title = Column(String(200), nullable=False)
  status = Column(
      String(60),
      nullable=False,
      default=task_status(STEP_GENERATING_CONFIG, STATUS_PENDING),
  )

  bbox_min_lat = Column(Float, nullable=False)
  bbox_min_lng = Column(Float, nullable=False)
  bbox_max_lat = Column(Float, nullable=False)
  bbox_max_lng = Column(Float, nullable=False)

  # ไม่มีในสเปก แต่หน้าเว็บให้ผู้ใช้เลือกระดับซูมได้ จึงต้องเก็บไว้ด้วย
  zoom = Column(Integer, nullable=False, default=18)

  created_at = Column(DateTime, nullable=False, default=datetime.now)
  updated_at = Column(
      DateTime, nullable=False, default=datetime.now, onupdate=datetime.now
  )

  user = relationship("User", back_populates="tasks")
  steps = relationship(
      "TaskStep",
      back_populates="task",
      cascade="all, delete-orphan",
      order_by="TaskStep.step_order",
  )
  result = relationship(
      "TaskResult",
      back_populates="task",
      cascade="all, delete-orphan",
      uselist=False,
  )


class TaskStep(Base):
  __tablename__ = "task_step"
  # ER ระบุความสัมพันธ์เป็น 4 พอดี จึงกันไม่ให้มี step_name ซ้ำใน task เดียวกัน
  __table_args__ = (UniqueConstraint("tid", "step_name", name="uq_task_step"),)

  sid = Column(String(36), primary_key=True, default=new_uuid)
  tid = Column(
      String(36), ForeignKey("task.tid", ondelete="CASCADE"), nullable=False
  )
  step_name = Column(String(40), nullable=False)
  status = Column(String(20), nullable=False, default=STATUS_PENDING)
  retry_count = Column(Integer, nullable=False, default=0)
  error_msg = Column(Text, nullable=True)
  started_at = Column(DateTime, nullable=True)
  completed_at = Column(DateTime, nullable=True)

  # ช่วยเรียงลำดับตอน query (ไม่ได้อยู่ในสเปก แต่ไม่ต้องเดาลำดับจากชื่อ)
  step_order = Column(Integer, nullable=False, default=0)

  task = relationship("Task", back_populates="steps")


class TaskResult(Base):
  __tablename__ = "task_result"

  rid = Column(String(36), primary_key=True, default=new_uuid)
  tid = Column(
      String(36),
      ForeignKey("task.tid", ondelete="CASCADE"),
      nullable=False,
      unique=True,  # ER ระบุ 0..1 ต่อหนึ่ง Task
  )
  surface_area = Column(Float, nullable=False, default=0.0)
  power_generation = Column(Float, nullable=False, default=0.0)
  panel_count = Column(Integer, nullable=False, default=0)
  overlay_image_path = Column(String(500), nullable=True)
  shapefile_path = Column(String(500), nullable=True)
  created_at = Column(DateTime, nullable=False, default=datetime.now)

  task = relationship("Task", back_populates="result")
