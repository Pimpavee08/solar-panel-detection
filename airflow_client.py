"""สั่งให้ Airflow เริ่ม DAG run ผ่าน REST API

ฝั่งเว็บไม่ได้รัน pipeline เองแล้ว แค่บันทึก Task ลงฐานข้อมูลแล้วสั่ง Airflow
ให้เริ่มทำงาน ตาม Main Challenge เรื่อง Workflow Management ในเอกสาร Requirement

ตั้งค่าผ่าน environment variable:
    SOLAR_AIRFLOW_URL       เช่น http://localhost:8080  (ไม่ตั้ง = ปิดการใช้งาน)
    SOLAR_AIRFLOW_USER      ค่าเริ่มต้น airflow
    SOLAR_AIRFLOW_PASSWORD  ค่าเริ่มต้น airflow
    SOLAR_AIRFLOW_DAG_ID    ค่าเริ่มต้น solar_panel_detection_pipeline
    SOLAR_AIRFLOW_API       v1 (Airflow 2.x, ค่าเริ่มต้น) หรือ v2 (Airflow 3.x)

ถ้าไม่ได้ตั้ง SOLAR_AIRFLOW_URL ระบบจะถอยไปรัน pipeline ในโปรเซสของเว็บเอง
เพื่อให้เปิดเครื่องมาก็ใช้งานได้โดยไม่ต้องตั้ง Airflow ก่อน
"""

import os

import requests

DAG_ID = os.environ.get(
    "SOLAR_AIRFLOW_DAG_ID", "solar_panel_detection_pipeline"
)
TIMEOUT = float(os.environ.get("SOLAR_AIRFLOW_TIMEOUT", "10"))


class AirflowError(RuntimeError):
  """สั่ง Airflow ไม่สำเร็จ"""


def base_url() -> str:
  return (os.environ.get("SOLAR_AIRFLOW_URL") or "").rstrip("/")


def is_enabled() -> bool:
  return bool(base_url())


def _auth() -> tuple[str, str]:
  return (
      os.environ.get("SOLAR_AIRFLOW_USER", "airflow"),
      os.environ.get("SOLAR_AIRFLOW_PASSWORD", "airflow"),
  )


def _api_root() -> str:
  version = os.environ.get("SOLAR_AIRFLOW_API", "v1")
  return f"{base_url()}/api/{version}"


def trigger_dag(tid: str) -> str:
  """สั่งเริ่ม DAG run หนึ่งครั้งสำหรับ Task นี้ คืน dag_run_id ที่ Airflow ตั้งให้

  ใช้ tid เป็น dag_run_id ด้วย จึงกันไม่ให้ Task เดียวถูกสั่งรันซ้ำซ้อน
  (Airflow จะตอบ 409 ถ้ามี run id ซ้ำ)
  """
  if not is_enabled():
    raise AirflowError("ยังไม่ได้ตั้งค่า SOLAR_AIRFLOW_URL")

  url = f"{_api_root()}/dags/{DAG_ID}/dagRuns"
  payload = {"dag_run_id": f"task__{tid}", "conf": {"tid": tid}}

  try:
    response = requests.post(
        url, json=payload, auth=_auth(), timeout=TIMEOUT
    )
  except requests.RequestException as exc:
    raise AirflowError(f"ติดต่อ Airflow ที่ {base_url()} ไม่ได้: {exc}") from exc

  if response.status_code not in (200, 409):
    raise AirflowError(
        f"Airflow ตอบ {response.status_code}: {response.text[:300]}"
    )

  if response.status_code == 409:
    return f"task__{tid}"  # มี run นี้อยู่แล้ว ถือว่าสั่งสำเร็จ

  return response.json().get("dag_run_id", f"task__{tid}")


def health() -> dict:
  """ใช้ตรวจว่า Airflow พร้อมรับงานหรือยัง"""
  if not is_enabled():
    return {"enabled": False, "reachable": False, "detail": "ไม่ได้ตั้งค่า"}

  try:
    response = requests.get(
        f"{base_url()}/health", auth=_auth(), timeout=TIMEOUT
    )
    response.raise_for_status()
    body = response.json()
  except (requests.RequestException, ValueError) as exc:
    return {"enabled": True, "reachable": False, "detail": str(exc)}

  scheduler = (body.get("scheduler") or {}).get("status")
  return {
      "enabled": True,
      "reachable": True,
      "dag_id": DAG_ID,
      "scheduler": scheduler,
      "detail": body,
  }
