"""Airflow DAG ของ Solar Panel Detection Pipeline

หนึ่ง Airflow task = หนึ่ง step ตามเอกสาร AW3_Database_Design

    generating_config -> fetch_image -> run_inference -> parse_result

การ retry ยกให้ Airflow จัดการ (retries = MAX_RETRY - 1 จึงได้ 5 ครั้งรวม
ครั้งแรก) ส่วนโค้ดนี้มีหน้าที่สะท้อนสถานะของ Airflow กลับไปเก็บในตาราง
Task_Step ผ่าน callback:

    เริ่มทำงาน  -> status = running, started_at
    ล้มเหลวแต่ยังลองต่อได้ -> retry_count += 1, error_msg, ล้างโฟลเดอร์
    ล้มเหลวจนหมดโควต้า     -> status = failed
    สำเร็จ                 -> status = completed, completed_at

DAG รอรับ trigger จากหน้าเว็บพร้อม conf = {"tid": "<uuid ของ Task>"}
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import pipeline
from models import MAX_RETRY, STEP_NAMES

DAG_ID = "solar_panel_detection_pipeline"

default_args = {
    "owner": "solar_team",
    "depends_on_past": False,
    # Airflow นับ retries เป็นจำนวนครั้งที่ลองใหม่ ไม่รวมครั้งแรก
    "retries": MAX_RETRY - 1,
    "retry_delay": timedelta(seconds=30),
}


# -------------------------------------------------------------
# helper ดึงค่าจาก context ของ Airflow
# -------------------------------------------------------------
def _tid(context) -> str:
  conf = (context.get("dag_run").conf if context.get("dag_run") else None) or {}
  tid = conf.get("tid")
  if not tid:
    raise ValueError(
        "ต้อง trigger DAG พร้อม conf เช่น {\"tid\": \"<uuid ของ Task>\"}"
    )
  return tid


def _attempt(context) -> int:
  """ครั้งที่กำลังลองอยู่ (เริ่มที่ 1) — ใช้ getattr กันความต่างระหว่างเวอร์ชัน"""
  return int(getattr(context.get("ti"), "try_number", 1) or 1)


def _step_name(context) -> str:
  # ตั้ง task_id ให้ตรงกับ step_name จึงหยิบมาใช้ได้ตรง ๆ
  return context["task"].task_id


def _error_text(context) -> str:
  exc = context.get("exception")
  return f"{type(exc).__name__}: {exc}" if exc else "ไม่ทราบสาเหตุ"


# -------------------------------------------------------------
# callback สะท้อนสถานะกลับไปที่ Task_Step
# -------------------------------------------------------------
def on_retry(context) -> None:
  tid, step = _tid(context), _step_name(context)
  pipeline.mark_step_retrying(
      tid, step, _error_text(context), retry_count=_attempt(context)
  )


def on_failure(context) -> None:
  try:
    tid, step = _tid(context), _step_name(context)
  except ValueError:
    return  # ไม่มี tid ให้บันทึก (เช่นถูก trigger มาโดยไม่ใส่ conf)
  pipeline.mark_step_failed(
      tid, step, _error_text(context), retry_count=_attempt(context)
  )


def make_step_callable(step_name: str):
  """สร้างฟังก์ชันของ Airflow task สำหรับ step ที่กำหนด"""

  def _run(**context):
    tid = _tid(context)
    retry_count = _attempt(context) - 1

    pipeline.mark_step_running(tid, step_name, retry_count=retry_count)
    pipeline.run_step(tid, step_name)  # โยน exception ออกมาให้ Airflow retry
    pipeline.mark_step_completed(tid, step_name, retry_count=retry_count)

    return {"tid": tid, "step": step_name}

  _run.__name__ = f"run_{step_name}"
  return _run


# -------------------------------------------------------------
# ประกาศ DAG
# -------------------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=(
        "ดึงภาพดาวเทียม รันโมเดลตรวจจับแผงโซลาร์เซลล์ และสรุปผลลงฐานข้อมูล"
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,  # ไม่ตั้งเวลา รอรับ trigger จากหน้าเว็บอย่างเดียว
    catchup=False,
    max_active_runs=4,
    tags=["solar", "geospatial", "ai"],
) as dag:

  previous = None
  for name in STEP_NAMES:
    current = PythonOperator(
        task_id=name,
        python_callable=make_step_callable(name),
        on_retry_callback=on_retry,
        on_failure_callback=on_failure,
    )
    if previous is not None:
      previous >> current
    previous = current
