import os
from data_retrieval import download_satellite_image
from model_runner import execute_model_inference
from post_processing import process_shapefile_to_geojson

if __name__ == "__main__":
  job_name = "job_siit_test"
  base_dir = "data/inference"
  job_dir = os.path.join(base_dir, job_name)
  input_dir = os.path.join(job_dir, "input")
  os.makedirs(input_dir, exist_ok=True)

  # พิกัดเป้าหมาย (SIIT รังสิต)
  min_lat, min_lon = 14.0700, 100.6020
  max_lat, max_lon = 14.0740, 100.6070

  # Step 1: ดึงภาพ GeoTIFF
  tif_path = os.path.join(input_dir, "satellite.tif")
  download_satellite_image(
      min_lat, min_lon, max_lat, max_lon, zoom=18, output_tif_path=tif_path
  )

  # Step 2: สร้างไฟล์ TOML และสั่งรันโมเดล
  output_dir = execute_model_inference(job_name, base_dir=base_dir)

  # Step 3: แปลง GeoJSON + คำนวณสรุปผล + บันทึก Database
  geojson_path = os.path.join(job_dir, "result.geojson")
  summary = process_shapefile_to_geojson(
      output_dir, output_geojson_path=geojson_path, job_name=job_name
  )

  # แสดงผลสรุปบนหน้าจอ
  print("\n" + "=" * 50)
  print(f" 📊 JOB SUMMARY REPORT: {summary['job_name']}")
  print("=" * 50)
  print(f" • สถานะการทำงาน:      {summary['status']}")
  print(f" • จำนวนแผง/แปลงที่ตรวจพบ: {summary['polygon_count']} แปลง")
  print(f" • พื้นที่ผิวแผงรวม:      {summary['total_surface_area_sqm']:,} ตร.ม.")
  print(f" • ประมาณการจำนวนแผง:    {summary['total_panels']:,} แผง")
  print(f" • กำลังการผลิตรวม:     {summary['total_capacity_kwp']:,} kWp")
  print(
      " • ไฟฟ้าที่ผลิตได้ต่อปี:   "
      f" {summary['total_annual_generation_kwh']:,} kWh/ปี"
  )
  print("=" * 50)
