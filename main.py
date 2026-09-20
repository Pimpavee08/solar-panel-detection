"""สคริปต์รัน pipeline หนึ่งงานจากบรรทัดคำสั่ง (ไม่ต้องเปิดเว็บ)

    python main.py
    python main.py --title "นิคมฯ บางปู" --bbox 13.5180 100.6480 13.5220 100.6530
"""

import argparse
import sys

import pipeline
from db import init_db, session_scope
from models import STATUS_COMPLETED, Task, TaskResult, TaskStep


# คอนโซล Windows มักเป็น cp874 ซึ่งพิมพ์อักษรนอกช่วงไทยไม่ได้
if hasattr(sys.stdout, "reconfigure"):
  sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def parse_args():
  parser = argparse.ArgumentParser(
      description="ตรวจจับแผงโซลาร์เซลล์จากภาพถ่ายดาวเทียมในพื้นที่ที่กำหนด"
  )
  parser.add_argument("--title", default="งานทดสอบ SIIT รังสิต")
  parser.add_argument(
      "--bbox",
      nargs=4,
      type=float,
      metavar=("MIN_LAT", "MIN_LNG", "MAX_LAT", "MAX_LNG"),
      default=[14.0700, 100.6020, 14.0740, 100.6070],
  )
  parser.add_argument("--zoom", type=int, default=18)
  return parser.parse_args()


def print_report(tid: str) -> None:
  with session_scope() as session:
    task = session.get(Task, tid)
    steps = (
        session.query(TaskStep)
        .filter_by(tid=tid)
        .order_by(TaskStep.step_order)
        .all()
    )
    result = session.query(TaskResult).filter_by(tid=tid).one_or_none()

    print("\n" + "=" * 56)
    print(f" รายงานผล: {task.title}")
    print("=" * 56)
    print(f" - Task ID:  {tid}")
    print(f" - สถานะ:    {task.status}\n")

    for step in steps:
      mark = "OK  " if step.status == STATUS_COMPLETED else "FAIL"
      retry = f"  (retry {step.retry_count}/5)" if step.retry_count else ""
      print(f"   {mark} {step.step_name:<20} {step.status}{retry}")
      if step.error_msg:
        print(f"     -> {step.error_msg}")

    if result is None:
      print("\n ยังไม่มีผลลัพธ์ — Task ยังไม่ผ่านขั้น parse_result")
      print("=" * 56)
      return

    print()
    print(f" - พื้นที่ผิวแผงรวม:     {result.surface_area:,.2f} ตร.ม.")
    print(f" - ประมาณการจำนวนแผง:   {result.panel_count:,} แผง")
    print(f" - ไฟฟ้าที่ผลิตได้ต่อปี:  {result.power_generation:,.2f} kWh/ปี")
    print(f" - GeoJSON:             {pipeline.geojson_path(tid)}")
    print(f" - ภาพ overlay:          {result.overlay_image_path or '(ไม่ได้สร้าง)'}")
    print("=" * 56)


if __name__ == "__main__":
  args = parse_args()
  min_lat, min_lng, max_lat, max_lng = args.bbox

  init_db()
  tid = pipeline.create_task(
      title=args.title,
      min_lat=min_lat,
      min_lng=min_lng,
      max_lat=max_lat,
      max_lng=max_lng,
      zoom=args.zoom,
  )
  print(f"สร้าง Task {tid} แล้ว กำลังรัน pipeline...")
  pipeline.run_task(tid)
  print_report(tid)
