import argparse
import glob
import os
import re
import geopandas as gpd
import rasterio
from shapely.geometry import box

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--config", type=str, required=True)
  args = parser.parse_args()

  # 1. อ่าน job_name และ inference_path จากไฟล์ .toml
  with open(args.config, "r", encoding="utf-8") as f:
    text = f.read()

  job_name_match = re.search(r"job_name\s*=\s*['\"]([^'\"]+)['\"]", text)
  path_match = re.search(r"inference_path\s*=\s*['\"]([^'\"]+)['\"]", text)

  job_name = job_name_match.group(1) if job_name_match else "job_test"
  base_path = path_match.group(1) if path_match else "data/inference/"

  job_dir = os.path.join(base_path, job_name)
  input_dir = os.path.join(job_dir, "input")
  output_dir = os.path.join(job_dir, "output")
  os.makedirs(output_dir, exist_ok=True)

  # 2. หาภาพ GeoTIFF ในโฟลเดอร์ input
  tif_files = glob.glob(os.path.join(input_dir, "*.tif"))
  if not tif_files:
    raise FileNotFoundError(f"No satellite image found in {input_dir}")

  # 3. อ่านพิกัดจริงจากภาพดาวเทียม
  with rasterio.open(tif_files[0]) as src:
    b = src.bounds
    cx = (b.left + b.right) / 2
    cy = (b.bottom + b.top) / 2
    dx = (b.right - b.left) * 0.05
    dy = (b.top - b.bottom) * 0.05

    poly1 = box(cx - dx * 2, cy - dy, cx - dx, cy + dy)
    poly2 = box(cx + dx, cy - dy, cx + dx * 2, cy + dy)

    panel_ids = [101, 102]
    panel_labels = ["solar_panel", "solar_panel"]

    gdf = gpd.GeoDataFrame(
        {"id": panel_ids, "label": panel_labels},
        geometry=[poly1, poly2],
        crs=src.crs,
    )

  gdf_wgs84 = gdf.to_crs(epsg=4326)

  # บันทึกไฟล์ผลลัพธ์เป็น -corrected.shp ตามรูปแบบของอาจารย์
  shp_output = os.path.join(output_dir, f"{job_name}-corrected.shp")
  gdf_wgs84.to_file(shp_output)
  print(f"[Mock Model] Created corrected shapefile: {shp_output}")