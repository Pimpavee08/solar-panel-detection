"""ตัวเดิน Pipeline 4 step ตาม Workflow ในเอกสาร AW3_Database_Design

    generating_config -> fetch_image -> run_inference -> parse_result

กฎการ retry ตามเอกสาร:
  - เมื่อ step ล้มเหลว ให้บันทึก error_msg แล้ว retry ต่อ
  - retry ได้สูงสุด MAX_RETRY (5) ครั้ง
  - ระหว่างที่ยัง retry ได้ Task_Step.status ยังเป็น "running"
  - ครบ 5 ครั้งแล้วยังไม่ผ่าน ให้ status = "failed" และหยุดทั้ง Task
  - ล้างโฟลเดอร์ก่อนเริ่ม retry (ยกเว้น input folder)

ทุกครั้งที่ step เปลี่ยนสถานะ จะ sync กลับไปที่
Task.status = step_name + ":" + status
"""

from datetime import datetime
import os
import shutil
import traceback

from data_retrieval import download_satellite_image
from db import session_scope
from model_runner import generate_config, run_inference
from overlay import create_overlay
from models import (
    MAX_RETRY,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_RUNNING,
    STEP_FETCH_IMAGE,
    STEP_GENERATING_CONFIG,
    STEP_NAMES,
    STEP_PARSE_RESULT,
    STEP_RUN_INFERENCE,
    Task,
    TaskResult,
    TaskStep,
    task_status,
)
from post_processing import process_shapefile_to_geojson

BASE_DIR = "data/inference"

# โฟลเดอร์ผลลัพธ์ที่โมเดลสร้างขึ้น ต้องล้างทิ้งก่อน retry เพื่อไม่ให้ไฟล์เก่าปน
WORK_FOLDERS = ["output", "input_tile", "mask_tile", "mask_tile_raw"]


# -------------------------------------------------------------
# path helpers — ทุก path อิงกับ tid ตามสเปกข้อ 3
# -------------------------------------------------------------
def task_dir(tid: str) -> str:
  return os.path.join(BASE_DIR, tid)


def input_dir(tid: str) -> str:
  return os.path.join(task_dir(tid), "input")


def output_dir(tid: str) -> str:
  return os.path.join(task_dir(tid), "output")


def geojson_path(tid: str) -> str:
  return os.path.join(task_dir(tid), "result.geojson")


def satellite_path(tid: str) -> str:
  return os.path.join(input_dir(tid), "satellite.tif")


def overlay_path(tid: str) -> str:
  return os.path.join(task_dir(tid), "overlay.png")


def overlay_meta_path(tid: str) -> str:
  return os.path.join(task_dir(tid), "overlay.json")


def clean_work_dirs(tid: str, keep_input: bool = True) -> None:
  """ล้างผลลัพธ์ก่อน retry

  ปกติเก็บ input ไว้ตามเอกสาร แต่ถ้า step ที่ล้มเหลวคือ fetch_image เอง
  ต้องล้าง input ด้วย มิฉะนั้นไฟล์ภาพที่ดาวน์โหลดค้างไว้ไม่ครบจะตกค้าง
  """
  for folder in WORK_FOLDERS:
    path = os.path.join(task_dir(tid), folder)
    if os.path.exists(path):
      shutil.rmtree(path, ignore_errors=True)

  if not keep_input:
    path = input_dir(tid)
    if os.path.exists(path):
      shutil.rmtree(path, ignore_errors=True)

  for stale in (geojson_path(tid), overlay_path(tid), overlay_meta_path(tid),
                os.path.join(task_dir(tid), "overlay.pgw")):
    if os.path.exists(stale):
      os.remove(stale)


# -------------------------------------------------------------
# การสร้าง Task
# -------------------------------------------------------------
def create_task(
    title: str,
    min_lat: float,
    min_lng: float,
    max_lat: float,
    max_lng: float,
    zoom: int = 18,
    uid: str | None = None,
) -> str:
  """สร้าง Task พร้อม Task_Step ครบ 4 แถว (สถานะ pending) แล้วคืน tid"""
  with session_scope() as session:
    task = Task(
        uid=uid,
        title=title,
        status=task_status(STEP_GENERATING_CONFIG, STATUS_PENDING),
        bbox_min_lat=min_lat,
        bbox_min_lng=min_lng,
        bbox_max_lat=max_lat,
        bbox_max_lng=max_lng,
        zoom=zoom,
    )
    session.add(task)
    session.flush()  # ให้ได้ tid ก่อนสร้าง step

    for order, step_name in enumerate(STEP_NAMES):
      session.add(
          TaskStep(
              tid=task.tid,
              step_name=step_name,
              status=STATUS_PENDING,
              retry_count=0,
              step_order=order,
          )
      )

    tid = task.tid

  os.makedirs(input_dir(tid), exist_ok=True)
  return tid


# -------------------------------------------------------------
# การอัปเดตสถานะ
# -------------------------------------------------------------
def _update_step(
    tid: str,
    step_name: str,
    status: str,
    error_msg: str | None = None,
    retry_count: int | None = None,
    mark_started: bool = False,
    mark_completed: bool = False,
) -> None:
  with session_scope() as session:
    step = (
        session.query(TaskStep)
        .filter_by(tid=tid, step_name=step_name)
        .one_or_none()
    )
    if step is None:
      return

    step.status = status
    step.error_msg = error_msg
    if retry_count is not None:
      step.retry_count = retry_count
    if mark_started and step.started_at is None:
      step.started_at = datetime.now()
    if mark_completed:
      step.completed_at = datetime.now()

    task = session.get(Task, tid)
    if task is not None:
      task.status = task_status(step_name, status)
      task.updated_at = datetime.now()


def _save_result(tid: str, summary: dict, overlay: str | None) -> None:
  """เขียน Task_Result — ทำเฉพาะตอน parse_result สำเร็จเท่านั้น (ตามเอกสารข้อ 6)"""
  with session_scope() as session:
    result = session.query(TaskResult).filter_by(tid=tid).one_or_none()
    if result is None:
      result = TaskResult(tid=tid)
      session.add(result)

    result.surface_area = summary.get("total_surface_area_sqm", 0.0)
    result.power_generation = summary.get("total_annual_generation_kwh", 0.0)
    result.panel_count = summary.get("total_panels", 0)
    result.shapefile_path = summary.get("shapefile_path")
    result.overlay_image_path = overlay
    result.created_at = datetime.now()


# -------------------------------------------------------------
# ตัว step แต่ละขั้น — โยน exception เมื่อล้มเหลว เพื่อให้ตัว retry จับได้
# -------------------------------------------------------------
def _step_generating_config(tid: str, task: dict) -> None:
  generate_config(tid, base_dir=BASE_DIR)


def _step_fetch_image(tid: str, task: dict) -> None:
  os.makedirs(input_dir(tid), exist_ok=True)
  download_satellite_image(
      task["bbox_min_lat"],
      task["bbox_min_lng"],
      task["bbox_max_lat"],
      task["bbox_max_lng"],
      zoom=task["zoom"],
      output_tif_path=satellite_path(tid),
  )


def _step_run_inference(tid: str, task: dict) -> None:
  run_inference(tid, base_dir=BASE_DIR)


def _step_parse_result(tid: str, task: dict) -> None:
  summary = process_shapefile_to_geojson(
      output_dir(tid),
      output_geojson_path=geojson_path(tid),
      job_name=tid,
      db_path=None,  # Task_Result เป็นที่เก็บผลลัพธ์แทนตาราง job_results เดิม
  )
  if summary.get("status") != "completed":
    raise RuntimeError(summary.get("message", "แปลงผลลัพธ์ไม่สำเร็จ"))

  # ภาพ overlay เป็นส่วนแสดงผล ไม่ใช่ตัวเลขผลลัพธ์ ถ้าวาดไม่สำเร็จจึงไม่ควร
  # ทำให้ทั้ง Task ล้มเหลวแล้ว retry ใหม่ทั้งชุด — บันทึกเป็น None แล้วไปต่อ
  overlay = None
  try:
    create_overlay(satellite_path(tid), geojson_path(tid), overlay_path(tid))
    overlay = overlay_path(tid)
  except Exception as exc:  # noqa: BLE001
    print(f"[pipeline] สร้างภาพ overlay ไม่สำเร็จ: {type(exc).__name__}: {exc}")

  _save_result(tid, summary, overlay)


STEP_FUNCTIONS = {
    STEP_GENERATING_CONFIG: _step_generating_config,
    STEP_FETCH_IMAGE: _step_fetch_image,
    STEP_RUN_INFERENCE: _step_run_inference,
    STEP_PARSE_RESULT: _step_parse_result,
}


# -------------------------------------------------------------
# ตัวเดินงานหลัก
# -------------------------------------------------------------
def _run_step_with_retry(tid: str, step_name: str, task: dict) -> bool:
  """รัน step หนึ่งขั้นพร้อม retry คืน True เมื่อสำเร็จ"""
  fn = STEP_FUNCTIONS[step_name]
  retry_count = 0

  while True:
    _update_step(
        tid, step_name, STATUS_RUNNING, retry_count=retry_count, mark_started=True
    )
    try:
      fn(tid, task)
    except Exception as exc:  # noqa: BLE001 — ต้องจับทุกชนิดเพื่อบันทึกลง DB
      retry_count += 1
      message = f"{type(exc).__name__}: {exc}"
      print(f"[pipeline] {tid} step '{step_name}' ล้มเหลวครั้งที่ {retry_count}")
      traceback.print_exc()

      if retry_count >= MAX_RETRY:
        _update_step(
            tid,
            step_name,
            STATUS_FAILED,
            error_msg=message,
            retry_count=retry_count,
            mark_completed=True,
        )
        return False

      # ยัง retry ได้ — สถานะคงเป็น running ตามเอกสาร แต่เก็บ error ล่าสุดไว้ดู
      _update_step(
          tid,
          step_name,
          STATUS_RUNNING,
          error_msg=message,
          retry_count=retry_count,
      )
      clean_work_dirs(tid, keep_input=(step_name != STEP_FETCH_IMAGE))
      continue

    _update_step(
        tid,
        step_name,
        STATUS_COMPLETED,
        retry_count=retry_count,
        mark_completed=True,
    )
    return True


def run_task(tid: str) -> None:
  """เดิน Task ตั้งแต่ step แรกจนจบ หยุดทันทีเมื่อมี step ใด failed"""
  with session_scope() as session:
    task_row = session.get(Task, tid)
    if task_row is None:
      print(f"[pipeline] ไม่พบ Task {tid}")
      return
    task = {
        "bbox_min_lat": task_row.bbox_min_lat,
        "bbox_min_lng": task_row.bbox_min_lng,
        "bbox_max_lat": task_row.bbox_max_lat,
        "bbox_max_lng": task_row.bbox_max_lng,
        "zoom": task_row.zoom,
    }

  for step_name in STEP_NAMES:
    if not _run_step_with_retry(tid, step_name, task):
      print(f"[pipeline] Task {tid} หยุดที่ step '{step_name}'")
      return

  print(f"[pipeline] Task {tid} เสร็จสมบูรณ์")


def delete_task_files(tid: str) -> bool:
  """ลบโฟลเดอร์ของ Task ออกจากดิสก์"""
  path = task_dir(tid)
  if not os.path.exists(path):
    return False
  shutil.rmtree(path, ignore_errors=True)
  return not os.path.exists(path)
