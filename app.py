"""REST API ของ Solar Panel Detection Pipeline

โครงสร้างข้อมูลอิงตามเอกสาร AW3_Database_Design (User / Task / Task_Step / Task_Result)
ตัวเดิน pipeline อยู่ใน pipeline.py
"""

from datetime import datetime
import json
import os
from typing import List, Optional

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, EmailStr, Field
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy.orm import selectinload

import airflow_client
import auth
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

# session cookie เซ็นด้วย SOLAR_SECRET_KEY (ดู auth.secret_key)
app.add_middleware(
    SessionMiddleware,
    secret_key=auth.secret_key(),
    session_cookie="solar_session",
    same_site="lax",
    # ตั้ง SOLAR_HTTPS=1 เมื่อเสิร์ฟผ่าน HTTPS เพื่อบังคับ Secure flag บน cookie
    https_only=os.environ.get("SOLAR_HTTPS") == "1",
)

# ใช้ cookie แล้วจึงตั้ง allow_origins แบบเจาะจงไม่ได้ใช้ "*"
# (เบราว์เซอร์ปฏิเสธการส่ง cookie ข้ามโดเมนเมื่อ origin เป็น wildcard)
CORS_ORIGINS = [
    o.strip()
    for o in os.environ.get(
        "SOLAR_CORS_ORIGINS",
        "http://localhost:8008,http://localhost:8009,http://127.0.0.1:8008",
    ).split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
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
class RegisterRequest(BaseModel):
  name: str = Field(..., min_length=1, max_length=120)
  email: EmailStr
  password: str = Field(..., min_length=8, max_length=256)


class LoginRequest(BaseModel):
  email: EmailStr
  password: str = Field(..., min_length=1, max_length=256)


class UserResponse(BaseModel):
  uid: str
  name: str
  email: str


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


def get_task_or_404(session, tid: str, uid: str) -> Task:
  """หา Task ของผู้ใช้คนนี้

  ถ้า Task มีอยู่แต่เป็นของคนอื่น จะตอบ 404 เหมือนกับกรณีไม่มีเลย
  เพื่อไม่ให้เดาได้ว่า tid ไหนมีอยู่จริงในระบบ
  """
  task = session.get(Task, tid)
  if task is None or task.uid != uid:
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
# บัญชีผู้ใช้
# -------------------------------------------------------------
@app.post("/auth/register", response_model=UserResponse, tags=["Auth"])
def register(payload: RegisterRequest, request: Request):
  """สมัครสมาชิกแล้วเข้าสู่ระบบให้เลย"""
  try:
    user = auth.create_user(payload.name, payload.email, payload.password)
  except ValueError as exc:
    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT, detail=str(exc)
    ) from exc

  auth.login_session(request, user["uid"])
  return user


@app.post("/auth/login", response_model=UserResponse, tags=["Auth"])
def login(payload: LoginRequest, request: Request):
  user = auth.authenticate(payload.email, payload.password)
  if user is None:
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="อีเมลหรือรหัสผ่านไม่ถูกต้อง",
    )
  auth.login_session(request, user["uid"])
  return user


@app.post("/auth/logout", tags=["Auth"])
def logout(request: Request):
  auth.logout_session(request)
  return {"message": "ออกจากระบบแล้ว"}


@app.get("/auth/me", response_model=UserResponse, tags=["Auth"])
def me(user: dict = Depends(auth.current_user)):
  return user


# -------------------------------------------------------------
# Tasks
# -------------------------------------------------------------
@app.post("/tasks", status_code=status.HTTP_202_ACCEPTED, tags=["Tasks"])
def create_task(
    payload: TaskCreate,
    background_tasks: BackgroundTasks,
    user: dict = Depends(auth.current_user),
):
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
      uid=user["uid"],
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
def list_tasks(user: dict = Depends(auth.current_user)):
  with session_scope() as session:
    tasks = (
        session.query(Task)
        .filter_by(uid=user["uid"])
        .options(selectinload(Task.result))
        .order_by(Task.created_at.desc())
        .all()
    )
    return [serialize_task(t) for t in tasks]


@app.get("/tasks/{tid}", response_model=TaskResponse, tags=["Tasks"])
def get_task(tid: str, user: dict = Depends(auth.current_user)):
  with session_scope() as session:
    return serialize_task(get_task_or_404(session, tid, user["uid"]))


@app.get(
    "/tasks/{tid}/progress", response_model=ProgressResponse, tags=["Tasks"]
)
def get_progress(tid: str, user: dict = Depends(auth.current_user)):
  """ความคืบหน้าราย step อ่านจากตาราง Task_Step โดยตรง"""
  with session_scope() as session:
    task = get_task_or_404(session, tid, user["uid"])
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
def delete_task(tid: str, user: dict = Depends(auth.current_user)):
  with session_scope() as session:
    task = get_task_or_404(session, tid, user["uid"])
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
def download_geojson(tid: str, user: dict = Depends(auth.current_user)):
  with session_scope() as session:
    get_task_or_404(session, tid, user["uid"])

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
def download_overlay(tid: str, user: dict = Depends(auth.current_user)):
  """ภาพถ่ายดาวเทียมที่วาดขอบเขตแผงที่ตรวจพบทับไว้แล้ว"""
  with session_scope() as session:
    get_task_or_404(session, tid, user["uid"])

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
def overlay_meta(tid: str, user: dict = Depends(auth.current_user)):
  """ขอบเขตของภาพ overlay ในพิกัด WGS84 สำหรับวางเป็น image layer บนแผนที่

  ขอบเขตนี้กว้างกว่า bbox ที่ผู้ใช้เลือกเล็กน้อย เพราะภาพถูกต่อจาก tile
  ที่ปัดขอบออกไป จึงใช้ค่าจากไฟล์ภาพจริง ไม่ใช่ค่าใน Task
  """
  with session_scope() as session:
    get_task_or_404(session, tid, user["uid"])

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
def download_satellite(tid: str, user: dict = Depends(auth.current_user)):
  with session_scope() as session:
    get_task_or_404(session, tid, user["uid"])

  path = pipeline.satellite_path(tid)
  if not os.path.exists(path):
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="ยังไม่มีไฟล์ภาพถ่ายดาวเทียมของ Task นี้",
    )
  return FileResponse(
      path=path, media_type="image/tiff", filename=f"{tid}-satellite.tif"
  )
