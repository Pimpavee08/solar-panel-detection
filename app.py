"""REST API ของ Solar Panel Detection Pipeline

โครงสร้างข้อมูลอิงตามเอกสาร AW3_Database_Design (User / Task / Task_Step / Task_Result)
ตัวเดิน pipeline อยู่ใน pipeline.py
"""

from datetime import datetime
import json
import os
from typing import List, Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import selectinload

import airflow_client
import pipeline
from db import init_db, session_scope
from models import (
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_RUNNING,
    STEP_NAMES,
    STEP_PARSE_RESULT,
    Task,
    TaskStep,
)

app = FastAPI(
    title="Solar Panel Detection Pipeline API",
    description=(
        "REST API สำหรับสร้าง Task ตรวจจับแผงโซลาร์เซลล์"
        " ดึงภาพดาวเทียม รันโมเดล และสรุปกำลังการผลิต"
    ),
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DASHBOARD_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "static", "dashboard.html"
)

init_db()


# -------------------------------------------------------------
# Schemas
# -------------------------------------------------------------
class TaskCreate(BaseModel):
  title: str = Field(..., min_length=1, max_length=200)
  min_lat: float = Field(..., ge=-85, le=85)
  min_lng: float = Field(..., ge=-180, le=180)
  max_lat: float = Field(..., ge=-85, le=85)
  max_lng: float = Field(..., ge=-180, le=180)
  zoom: int = Field(18, ge=1, le=21)


class StepResponse(BaseModel):
  step_name: str
  status: str
  retry_count: int
  error_msg: Optional[str] = None
  started_at: Optional[datetime] = None
  completed_at: Optional[datetime] = None


class ResultResponse(BaseModel):
  surface_area: float
  power_generation: float
  panel_count: int
  overlay_image_path: Optional[str] = None
  shapefile_path: Optional[str] = None


class TaskResponse(BaseModel):
  tid: str
  title: str
  status: str
  state: str  # running / completed / failed — ย่อจาก status ให้ UI ใช้ง่าย
  bbox_min_lat: float
  bbox_min_lng: float
  bbox_max_lat: float
  bbox_max_lng: float
  zoom: int
  created_at: datetime
  updated_at: datetime
  result: Optional[ResultResponse] = None


class ProgressResponse(BaseModel):
  tid: str
  status: str
  state: str
  percent: int
  steps: List[StepResponse]


# -------------------------------------------------------------
# Helpers
# -------------------------------------------------------------
def derive_state(task_status_value: str) -> str:
  """ย่อ "step_name:status" ให้เหลือ running / completed / failed สำหรับ UI"""
  step_name, _, tail = task_status_value.partition(":")
  if tail == STATUS_FAILED:
    return STATUS_FAILED
  if tail == STATUS_COMPLETED and step_name == STEP_PARSE_RESULT:
    return STATUS_COMPLETED
  return STATUS_RUNNING


def serialize_task(task: Task) -> dict:
  payload = {
      "tid": task.tid,
      "title": task.title,
      "status": task.status,
      "state": derive_state(task.status),
      "bbox_min_lat": task.bbox_min_lat,
      "bbox_min_lng": task.bbox_min_lng,
      "bbox_max_lat": task.bbox_max_lat,
      "bbox_max_lng": task.bbox_max_lng,
      "zoom": task.zoom,
      "created_at": task.created_at,
      "updated_at": task.updated_at,
      "result": None,
  }
  if task.result is not None:
    payload["result"] = {
        "surface_area": task.result.surface_area,
        "power_generation": task.result.power_generation,
        "panel_count": task.result.panel_count,
        "overlay_image_path": task.result.overlay_image_path,
        "shapefile_path": task.result.shapefile_path,
    }
  return payload


def get_task_or_404(session, tid: str) -> Task:
  task = session.get(Task, tid)
  if task is None:
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"ไม่พบ Task รหัส '{tid}'",
    )
  return task


# -------------------------------------------------------------
# หน้าเว็บ
# -------------------------------------------------------------
@app.get("/", response_class=HTMLResponse, tags=["General"])
def read_root():
  """หน้าเว็บ Dashboard (ไฟล์อยู่ที่ static/dashboard.html)"""
  if not os.path.exists(DASHBOARD_PATH):
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"ไม่พบไฟล์หน้าเว็บที่ {DASHBOARD_PATH}",
    )
  with open(DASHBOARD_PATH, encoding="utf-8") as f:
    return HTMLResponse(f.read())


@app.get("/health", tags=["General"])
def health():
  """บอกว่าตอนนี้ pipeline ถูกรันด้วยอะไร และ Airflow ติดต่อได้หรือไม่"""
  info = airflow_client.health()
  return {
      "executor": "airflow" if info["enabled"] else "background",
      "airflow": info,
  }


# -------------------------------------------------------------
# Tasks
# -------------------------------------------------------------
@app.post("/tasks", status_code=status.HTTP_202_ACCEPTED, tags=["Tasks"])
def create_task(payload: TaskCreate, background_tasks: BackgroundTasks):
  """สร้าง Task ใหม่ พร้อม Task_Step ครบ 4 แถว แล้วสั่งรัน pipeline เบื้องหลัง"""
  if payload.min_lat >= payload.max_lat:
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="ละติจูดสูงสุด (max_lat) ต้องมากกว่าละติจูดต่ำสุด (min_lat)",
    )
  if payload.min_lng >= payload.max_lng:
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="ลองจิจูดสูงสุด (max_lng) ต้องมากกว่าลองจิจูดต่ำสุด (min_lng)",
    )

  title = payload.title.strip()
  tid = pipeline.create_task(
      title=title,
      min_lat=payload.min_lat,
      min_lng=payload.min_lng,
      max_lat=payload.max_lat,
      max_lng=payload.max_lng,
      zoom=payload.zoom,
  )

  # ตั้ง SOLAR_AIRFLOW_URL ไว้ = ให้ Airflow เป็นคนรัน ไม่ได้ตั้ง = รันในโปรเซสนี้
  executor = "airflow"
  dag_run_id = None
  if airflow_client.is_enabled():
    try:
      dag_run_id = airflow_client.trigger_dag(tid)
    except airflow_client.AirflowError as exc:
      # สั่ง Airflow ไม่ได้ ถือว่างานนี้ล้มเหลวตั้งแต่ step แรก จะได้ไม่ค้าง pending
      pipeline.mark_step_failed(tid, STEP_NAMES[0], str(exc))
      raise HTTPException(
          status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
          detail=f"สั่งงาน Airflow ไม่สำเร็จ: {exc}",
      ) from exc
  else:
    executor = "background"
    background_tasks.add_task(pipeline.run_task, tid)

  return {
      "tid": tid,
      "title": title,
      "status": "generating_config:pending",
      "executor": executor,
      "dag_run_id": dag_run_id,
      "message": f"สร้าง Task '{title}' แล้ว ติดตามสถานะที่ GET /tasks/{tid}/progress",
  }


@app.get("/tasks", response_model=List[TaskResponse], tags=["Tasks"])
def list_tasks():
  with session_scope() as session:
    tasks = (
        session.query(Task)
        .options(selectinload(Task.result))
        .order_by(Task.created_at.desc())
        .all()
    )
    return [serialize_task(t) for t in tasks]


@app.get("/tasks/{tid}", response_model=TaskResponse, tags=["Tasks"])
def get_task(tid: str):
  with session_scope() as session:
    return serialize_task(get_task_or_404(session, tid))


@app.get(
    "/tasks/{tid}/progress", response_model=ProgressResponse, tags=["Tasks"]
)
def get_progress(tid: str):
  """ความคืบหน้าราย step อ่านจากตาราง Task_Step โดยตรง"""
  with session_scope() as session:
    task = get_task_or_404(session, tid)
    steps = (
        session.query(TaskStep)
        .filter_by(tid=tid)
        .order_by(TaskStep.step_order)
        .all()
    )
    done = sum(1 for s in steps if s.status == STATUS_COMPLETED)
    return {
        "tid": tid,
        "status": task.status,
        "state": derive_state(task.status),
        "percent": round(done / len(steps) * 100) if steps else 0,
        "steps": [
            {
                "step_name": s.step_name,
                "status": s.status,
                "retry_count": s.retry_count,
                "error_msg": s.error_msg,
                "started_at": s.started_at,
                "completed_at": s.completed_at,
            }
            for s in steps
        ],
    }


@app.delete("/tasks/{tid}", tags=["Tasks"])
def delete_task(tid: str):
  with session_scope() as session:
    task = get_task_or_404(session, tid)
    title = task.title
    session.delete(task)  # cascade ลบ Task_Step และ Task_Result ให้เอง

  disk_deleted = pipeline.delete_task_files(tid)
  return {
      "message": f"ลบ Task '{title}' เรียบร้อย",
      "database_deleted": True,
      "disk_files_deleted": disk_deleted,
  }


# -------------------------------------------------------------
# Downloads
# -------------------------------------------------------------
@app.get("/tasks/{tid}/geojson", tags=["Downloads"])
def download_geojson(tid: str):
  with session_scope() as session:
    get_task_or_404(session, tid)

  path = pipeline.geojson_path(tid)
  if not os.path.exists(path):
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="ยังไม่มีไฟล์ GeoJSON (Task อาจกำลังทำงานอยู่หรือล้มเหลว)",
    )
  return FileResponse(
      path=path,
      media_type="application/geo+json",
      filename=f"{tid}-result.geojson",
  )


@app.get("/tasks/{tid}/overlay", tags=["Downloads"])
def download_overlay(tid: str):
  """ภาพถ่ายดาวเทียมที่วาดขอบเขตแผงที่ตรวจพบทับไว้แล้ว"""
  with session_scope() as session:
    get_task_or_404(session, tid)

  path = pipeline.overlay_path(tid)
  if not os.path.exists(path):
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="ยังไม่มีภาพ overlay ของ Task นี้",
    )
  return FileResponse(
      path=path, media_type="image/png", filename=f"{tid}-overlay.png"
  )


@app.get("/tasks/{tid}/overlay/meta", tags=["Downloads"])
def overlay_meta(tid: str):
  """ขอบเขตของภาพ overlay ในพิกัด WGS84 สำหรับวางเป็น image layer บนแผนที่

  ขอบเขตนี้กว้างกว่า bbox ที่ผู้ใช้เลือกเล็กน้อย เพราะภาพถูกต่อจาก tile
  ที่ปัดขอบออกไป จึงใช้ค่าจากไฟล์ภาพจริง ไม่ใช่ค่าใน Task
  """
  with session_scope() as session:
    get_task_or_404(session, tid)

  path = pipeline.overlay_meta_path(tid)
  if not os.path.exists(path):
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="ยังไม่มีภาพ overlay ของ Task นี้",
    )
  with open(path, encoding="utf-8") as f:
    meta = json.load(f)
  return {"tid": tid, "bounds": meta["bounds"],
          "polygon_count": meta.get("polygon_count", 0)}


@app.get("/tasks/{tid}/satellite", tags=["Downloads"])
def download_satellite(tid: str):
  with session_scope() as session:
    get_task_or_404(session, tid)

  path = pipeline.satellite_path(tid)
  if not os.path.exists(path):
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="ยังไม่มีไฟล์ภาพถ่ายดาวเทียมของ Task นี้",
    )
  return FileResponse(
      path=path, media_type="image/tiff", filename=f"{tid}-satellite.tif"
  )
