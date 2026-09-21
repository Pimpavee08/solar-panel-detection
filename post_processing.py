from datetime import datetime
import glob
import json
import os
import geopandas as gpd


def process_shapefile_to_geojson(
    output_dir: str,
    output_geojson_path: str,
    job_name: str = "job_default",
    panel_area_m2: float = 2.541,  # สเปกอาจารย์
    wp_per_m2: float = 180.0,
) -> dict:
  """อ่าน Shapefile -> คำนวณรายแผง -> เขียน GeoJSON + summary.json -> คืนค่าสรุป

  ไม่เขียนฐานข้อมูลเอง ผู้เรียก (pipeline.py) เป็นคนบันทึกลง Task_Result
  """
  job_dir = os.path.dirname(output_geojson_path)
  summary_json_path = os.path.join(job_dir, "summary.json")

  # 1. ค้นหาไฟล์ Shapefile ตัว corrected ก่อน
  shp_files = glob.glob(os.path.join(output_dir, "*corrected*.shp"))
  if not shp_files:
    shp_files = glob.glob(os.path.join(output_dir, "*.shp"))

  # กรณีไม่พบไฟล์ Shapefile
  if not shp_files:
    summary = {
        "job_name": job_name,
        "status": "failed",
        "message": "No shapefile found",
        "processed_at": datetime.now().isoformat(),
    }
    return summary

  target_shp = shp_files[0]
  print(f"Reading shapefile: {target_shp}")
  gdf = gpd.read_file(target_shp)

  # กรณีภาพนั้นตรวจไม่พบแผงโซลาร์เซลล์เลย (0 แผง)
  if gdf.empty:
    gdf.to_file(output_geojson_path, driver="GeoJSON")
    summary = {
        "job_name": job_name,
        "status": "completed",
        "polygon_count": 0,
        "total_surface_area_sqm": 0.0,
        "total_panels": 0,
        "total_capacity_kwp": 0.0,
        "total_annual_generation_kwh": 0.0,
        "geojson_path": output_geojson_path,
        "processed_at": datetime.now().isoformat(),
    }
    with open(summary_json_path, "w", encoding="utf-8") as f:
      json.dump(summary, f, indent=4)
    return summary

  # 2. แปลงเป็น UTM 47N เพื่อคำนวณพื้นที่จริง (หน่วยเมตร)
  gdf_utm = gdf.to_crs(epsg=32647)
  gdf_utm["geometry"] = gdf_utm["geometry"].simplify(
      tolerance=0.4, preserve_topology=True
  )

  # 3. คำนวณ Attributes รายแผง
  gdf_utm["area_sqm"] = gdf_utm.geometry.area.round(2)
  gdf_utm["estimated_panels"] = (
      gdf_utm["area_sqm"] / panel_area_m2
  ).round().astype(int)
  gdf_utm["capacity_kwp"] = (gdf_utm["area_sqm"] * wp_per_m2 / 1000.0).round(2)
  gdf_utm["annual_generation_kwh"] = (
      gdf_utm["capacity_kwp"] * 4.2 * 365 * 0.75
  ).round(2)

  # 4. แปลงกลับเป็น EPSG:4326 และเซฟเป็น GeoJSON
  gdf_wgs84 = gdf_utm.to_crs(epsg=4326)
  gdf_wgs84.to_file(output_geojson_path, driver="GeoJSON")
  print(f"GeoJSON successfully created at: {output_geojson_path}")

  # 5. สรุปตัวเลขรวมทั้ง Job (Summary Statistics)
  summary = {
      "job_name": job_name,
      "status": "completed",
      "polygon_count": int(len(gdf_utm)),
      "total_surface_area_sqm": float(gdf_utm["area_sqm"].sum().round(2)),
      "total_panels": int(gdf_utm["estimated_panels"].sum()),
      "total_capacity_kwp": float(gdf_utm["capacity_kwp"].sum().round(2)),
      "total_annual_generation_kwh": float(
          gdf_utm["annual_generation_kwh"].sum().round(2)
      ),
      "geojson_path": output_geojson_path,
      "shapefile_path": target_shp,
      "processed_at": datetime.now().isoformat(),
  }

  # 6. บันทึกไฟล์ summary.json
  with open(summary_json_path, "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=4, ensure_ascii=False)
  print(f"Summary JSON created at: {summary_json_path}")

  return summary
