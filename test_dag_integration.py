"""เทสต์ Airflow DAG และตัวสั่งงาน Airflow

ไม่ต้องติดตั้ง Airflow จริง — mock แพ็กเกจ airflow ไว้ก่อน import DAG
สิ่งที่สนใจคือ callback สะท้อนสถานะลงตาราง Task_Step ถูกต้องหรือไม่
"""

import sys
import unittest
from unittest.mock import MagicMock, patch

# ต้อง mock ก่อน import solar_pipeline_dag เพราะ DAG ประกาศไว้ระดับโมดูล
for module in ("airflow", "airflow.operators", "airflow.operators.python"):
  sys.modules.setdefault(module, MagicMock())

import airflow_client  # noqa: E402
import pipeline  # noqa: E402
import solar_pipeline_dag as dag_module  # noqa: E402
from db import session_scope  # noqa: E402
from models import (  # noqa: E402
    MAX_RETRY,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_RUNNING,
    STEP_FETCH_IMAGE,
    STEP_GENERATING_CONFIG,
    STEP_NAMES,
    Task,
    TaskStep,
)


def _context(tid, step_name, try_number=1, exception=None):
  """context ของ Airflow เท่าที่ DAG ใช้จริง"""
  dag_run = MagicMock()
  dag_run.conf = {"tid": tid} if tid else {}

  task = MagicMock()
  task.task_id = step_name

  ti = MagicMock()
  ti.try_number = try_number

  return {"dag_run": dag_run, "task": task, "ti": ti, "exception": exception}


def _step(session, tid, step_name):
  return session.query(TaskStep).filter_by(tid=tid, step_name=step_name).one()


class TestDagWiring(unittest.TestCase):
  def test_step_order_matches_spec(self):
    self.assertEqual(
        STEP_NAMES,
        ["generating_config", "fetch_image", "run_inference", "parse_result"],
    )

  def test_retries_allow_five_attempts_total(self):
    """Airflow นับ retries ไม่รวมครั้งแรก จึงต้องเป็น MAX_RETRY - 1"""
    self.assertEqual(dag_module.default_args["retries"], MAX_RETRY - 1)

  def test_missing_tid_raises(self):
    with self.assertRaises(ValueError):
      dag_module._tid(_context(None, STEP_GENERATING_CONFIG))


class TestDagCallbacks(unittest.TestCase):
  def setUp(self):
    self.tid = pipeline.create_task(
        "งานทดสอบ DAG", 14.070, 100.602, 14.074, 100.607, 18
    )

  def tearDown(self):
    with session_scope() as session:
      task = session.get(Task, self.tid)
      if task:
        session.delete(task)
    pipeline.delete_task_files(self.tid)

  def test_callable_marks_running_then_completed(self):
    calls = []
    with patch.dict(
        pipeline.STEP_FUNCTIONS,
        {STEP_GENERATING_CONFIG: lambda t, p: calls.append(t)},
    ):
      runner = dag_module.make_step_callable(STEP_GENERATING_CONFIG)
      result = runner(**_context(self.tid, STEP_GENERATING_CONFIG))

    self.assertEqual(calls, [self.tid])
    self.assertEqual(result["step"], STEP_GENERATING_CONFIG)

    with session_scope() as session:
      step = _step(session, self.tid, STEP_GENERATING_CONFIG)
      self.assertEqual(step.status, STATUS_COMPLETED)
      self.assertEqual(step.retry_count, 0)
      self.assertIsNotNone(step.started_at)
      self.assertIsNotNone(step.completed_at)

      task = session.get(Task, self.tid)
      self.assertEqual(
          task.status, f"{STEP_GENERATING_CONFIG}:{STATUS_COMPLETED}"
      )

  def test_callable_propagates_error_for_airflow_to_retry(self):
    """step ต้องโยน exception ออกไป ไม่กลืนไว้ ไม่งั้น Airflow จะไม่ retry"""

    def boom(_tid, _params):
      raise RuntimeError("พัง")

    with patch.dict(pipeline.STEP_FUNCTIONS, {STEP_GENERATING_CONFIG: boom}):
      runner = dag_module.make_step_callable(STEP_GENERATING_CONFIG)
      with self.assertRaises(RuntimeError):
        runner(**_context(self.tid, STEP_GENERATING_CONFIG))

    with session_scope() as session:
      # ยังคง running อยู่ เพราะ on_failure_callback ต่างหากที่ทำให้เป็น failed
      self.assertEqual(
          _step(session, self.tid, STEP_GENERATING_CONFIG).status,
          STATUS_RUNNING,
      )

  def test_on_retry_records_attempt_and_cleans_output(self):
    context = _context(
        self.tid, STEP_RUN := "run_inference", try_number=2,
        exception=RuntimeError("โมเดลล้ม"),
    )
    with patch.object(pipeline, "clean_work_dirs") as cleaner:
      dag_module.on_retry(context)

    cleaner.assert_called_once_with(self.tid, keep_input=True)

    with session_scope() as session:
      step = _step(session, self.tid, STEP_RUN)
      self.assertEqual(step.status, STATUS_RUNNING)
      self.assertEqual(step.retry_count, 2)
      self.assertIn("โมเดลล้ม", step.error_msg)

  def test_on_retry_of_fetch_image_also_clears_input(self):
    """ภาพที่ดาวน์โหลดค้างไว้ไม่ครบต้องถูกลบ ไม่งั้นรอบถัดไปจะใช้ไฟล์เสีย"""
    context = _context(
        self.tid, STEP_FETCH_IMAGE, try_number=1, exception=OSError("เน็ตหลุด")
    )
    with patch.object(pipeline, "clean_work_dirs") as cleaner:
      dag_module.on_retry(context)

    cleaner.assert_called_once_with(self.tid, keep_input=False)

  def test_on_failure_marks_step_failed(self):
    context = _context(
        self.tid, STEP_GENERATING_CONFIG, try_number=MAX_RETRY,
        exception=RuntimeError("หมดโควต้า"),
    )
    dag_module.on_failure(context)

    with session_scope() as session:
      step = _step(session, self.tid, STEP_GENERATING_CONFIG)
      self.assertEqual(step.status, STATUS_FAILED)
      self.assertEqual(step.retry_count, MAX_RETRY)
      self.assertIn("หมดโควต้า", step.error_msg)

      task = session.get(Task, self.tid)
      self.assertEqual(task.status, f"{STEP_GENERATING_CONFIG}:{STATUS_FAILED}")

  def test_on_failure_without_tid_is_ignored(self):
    """DAG ถูก trigger มาโดยไม่ใส่ conf — ไม่ควรทำให้ callback พังซ้ำ"""
    dag_module.on_failure(_context(None, STEP_GENERATING_CONFIG))


class TestAirflowClient(unittest.TestCase):
  def tearDown(self):
    import os

    os.environ.pop("SOLAR_AIRFLOW_URL", None)

  def test_disabled_without_url(self):
    self.assertFalse(airflow_client.is_enabled())
    with self.assertRaises(airflow_client.AirflowError):
      airflow_client.trigger_dag("any-tid")

  def test_trigger_posts_tid_as_conf(self):
    import os

    os.environ["SOLAR_AIRFLOW_URL"] = "http://airflow.test:8080/"

    response = MagicMock(status_code=200)
    response.json.return_value = {"dag_run_id": "task__abc"}

    with patch("airflow_client.requests.post", return_value=response) as post:
      self.assertEqual(airflow_client.trigger_dag("abc"), "task__abc")

    url, = post.call_args.args
    self.assertEqual(
        url,
        "http://airflow.test:8080/api/v1/dags/"
        f"{airflow_client.DAG_ID}/dagRuns",
    )
    self.assertEqual(post.call_args.kwargs["json"]["conf"], {"tid": "abc"})

  def test_duplicate_run_is_not_an_error(self):
    """Airflow ตอบ 409 เมื่อ dag_run_id ซ้ำ แปลว่างานถูกสั่งไปแล้ว"""
    import os

    os.environ["SOLAR_AIRFLOW_URL"] = "http://airflow.test:8080"
    with patch(
        "airflow_client.requests.post", return_value=MagicMock(status_code=409)
    ):
      self.assertEqual(airflow_client.trigger_dag("abc"), "task__abc")

  def test_server_error_raises(self):
    import os

    os.environ["SOLAR_AIRFLOW_URL"] = "http://airflow.test:8080"
    response = MagicMock(status_code=500, text="boom")
    with patch("airflow_client.requests.post", return_value=response):
      with self.assertRaises(airflow_client.AirflowError):
        airflow_client.trigger_dag("abc")


if __name__ == "__main__":
  unittest.main()
