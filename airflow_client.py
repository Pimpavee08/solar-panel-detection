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
import time

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


def run_id_for(tid: str, rerun: bool = False) -> str:
  """ตั้งชื่อ dag_run

  ครั้งแรกใช้ชื่อคงที่ตาม tid เพื่อกันไม่ให้งานเดียวถูกสั่งซ้ำ (Airflow ตอบ 409)
  ส่วนการสั่งรันใหม่เป็นเจตนาของผู้ใช้ จึงต่อท้ายด้วยเวลาให้ชื่อไม่ชนของเดิม
  """
  if not rerun:
    return f"task__{tid}"
  return f"task__{tid}__r{int(time.time())}"


def trigger_dag(tid: str, rerun: bool = False) -> str:
  """สั่งเริ่ม DAG run หนึ่งครั้งสำหรับ Task นี้ คืน dag_run_id ที่ใช้"""
  if not is_enabled():
    raise AirflowError("ยังไม่ได้ตั้งค่า SOLAR_AIRFLOW_URL")

  run_id = run_id_for(tid, rerun)
  url = f"{_api_root()}/dags/{DAG_ID}/dagRuns"

  try:
    response = requests.post(
        url,
        json={"dag_run_id": run_id, "conf": {"tid": tid}},
        auth=_auth(),
        timeout=TIMEOUT,
    )
  except requests.RequestException as exc:
    raise AirflowError(f"ติดต่อ Airflow ที่ {base_url()} ไม่ได้: {exc}") from exc

  if response.status_code not in (200, 409):
    raise AirflowError(
        f"Airflow ตอบ {response.status_code}: {response.text[:300]}"
    )

  if response.status_code == 409:
    return run_id  # มี run นี้อยู่แล้ว ถือว่าสั่งสำเร็จ

  return response.json().get("dag_run_id", run_id)


def fetch_log(run_id: str, step_name: str, try_number: int = 1) -> str:
  """ดึง log ของ task instance หนึ่งครั้งที่ลอง

  try_number เริ่มที่ 1 การลองครั้งที่ 2 เป็นต้นไปมี log แยกไฟล์กัน
  """
  if not is_enabled():
    raise AirflowError("ยังไม่ได้ตั้งค่า SOLAR_AIRFLOW_URL")

  url = (
      f"{_api_root()}/dags/{DAG_ID}/dagRuns/{run_id}"
      f"/taskInstances/{step_name}/logs/{try_number}"
  )
  try:
    response = requests.get(
        url,
        auth=_auth(),
        timeout=TIMEOUT,
        params={"full_content": "true"},
        headers={"Accept": "text/plain"},
    )
  except requests.RequestException as exc:
    raise AirflowError(f"ติดต่อ Airflow ไม่ได้: {exc}") from exc

  if response.status_code == 404:
    raise AirflowError("ไม่พบ log ของขั้นตอนนี้ (อาจยังไม่ได้เริ่มทำงาน)")
  if response.status_code != 200:
    raise AirflowError(
        f"Airflow ตอบ {response.status_code}: {response.text[:200]}"
    )
  return response.text


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
