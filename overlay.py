"""สร้างภาพ overlay — วาดขอบเขตแผงที่ตรวจพบทับลงบนภาพถ่ายดาวเทียมต้นฉบับ

ตรงกับ Requirement Spec ข้อ 7 (Result Visualization) และเติมค่า
Task_Result.overlay_image_path ให้ครบตาม ER

ผลลัพธ์ที่ได้ 3 ไฟล์:
  overlay.png   ภาพ RGB ที่วาดทับแล้ว
  overlay.pgw   world file ให้ GIS เปิดแล้วอยู่ตำแหน่งถูกต้อง
  overlay.json  ขอบเขตในพิกัด WGS84 สำหรับให้หน้าเว็บวางเป็น image layer
"""

import json
import os

import geopandas as gpd
import numpy as np
import rasterio
from PIL import Image, ImageDraw
from rasterio.warp import transform_bounds
from shapely.geometry import MultiPolygon, Polygon

# สีเดียวกับชั้น polygon บนหน้าเว็บ เพื่อให้ภาพที่ดาวน์โหลดไปหน้าตาเหมือนที่เห็น
FILL_RGBA = (34, 211, 238, 90)
OUTLINE_RGBA = (34, 211, 238, 255)
OUTLINE_WIDTH = 3


def _iter_polygons(geom):
  """คลี่ geometry ออกมาเป็น Polygon ทีละชิ้น (รองรับ MultiPolygon)"""
  if isinstance(geom, Polygon):
    yield geom
  elif isinstance(geom, MultiPolygon):
    yield from geom.geoms


def _write_world_file(path: str, transform) -> None:
  """world file ของ PNG ใช้นามสกุล .pgw และอ้างถึงจุดกึ่งกลางพิกเซลซ้ายบน"""
  with open(path, "w", encoding="utf-8") as f:
    f.write("\n".join([
        repr(transform.a),
        repr(transform.d),
        repr(transform.b),
        repr(transform.e),
        repr(transform.c + transform.a / 2),
        repr(transform.f + transform.e / 2),
    ]) + "\n")


def create_overlay(
    tif_path: str,
    geojson_path: str,
    out_png_path: str,
) -> dict:
  """วาด polygon จาก GeoJSON ทับภาพ GeoTIFF แล้วบันทึกเป็น PNG

  คืน dict ที่มี path ของไฟล์ ขอบเขตแบบ WGS84 และจำนวน polygon ที่วาด
  """
  if not os.path.exists(tif_path):
    raise FileNotFoundError(f"ไม่พบภาพถ่ายดาวเทียมที่ {tif_path}")

  with rasterio.open(tif_path) as src:
    rgb = src.read([1, 2, 3])
    transform = src.transform
    crs = src.crs
    # ขอบเขตของภาพจริงจะกว้างกว่า bbox ที่ผู้ใช้เลือกเล็กน้อย เพราะ tile ปัดขอบ
    west, south, east, north = transform_bounds(crs, "EPSG:4326", *src.bounds)

  base = Image.fromarray(np.transpose(rgb, (1, 2, 0)).astype("uint8"), "RGB")
  base = base.convert("RGBA")
  layer = Image.new("RGBA", base.size, (0, 0, 0, 0))
  draw = ImageDraw.Draw(layer)

  drawn = 0
  if os.path.exists(geojson_path):
    gdf = gpd.read_file(geojson_path)
    if not gdf.empty:
      gdf = gdf.to_crs(crs)
      to_pixel = ~transform  # affine ผกผัน: พิกัดแผนที่ -> (คอลัมน์, แถว)

      for geom in gdf.geometry:
        if geom is None or geom.is_empty:
          continue
        for poly in _iter_polygons(geom):
          exterior = [to_pixel * (x, y) for x, y in poly.exterior.coords]
          if len(exterior) < 3:
            continue
          draw.polygon(exterior, fill=FILL_RGBA)
          draw.line(exterior + [exterior[0]],
                    fill=OUTLINE_RGBA, width=OUTLINE_WIDTH)

          # เจาะรูในแผงที่มีช่องว่างตรงกลาง (เขียนค่า alpha=0 ทับลงไปตรง ๆ)
          for ring in poly.interiors:
            hole = [to_pixel * (x, y) for x, y in ring.coords]
            if len(hole) >= 3:
              draw.polygon(hole, fill=(0, 0, 0, 0))
          drawn += 1

  os.makedirs(os.path.dirname(out_png_path) or ".", exist_ok=True)
  Image.alpha_composite(base, layer).convert("RGB").save(out_png_path, "PNG")
  _write_world_file(os.path.splitext(out_png_path)[0] + ".pgw", transform)

  meta = {
      "overlay_path": out_png_path,
      "bounds": [[south, west], [north, east]],  # รูปแบบของ Leaflet
      "polygon_count": drawn,
  }
  with open(
      os.path.splitext(out_png_path)[0] + ".json", "w", encoding="utf-8"
  ) as f:
    json.dump(meta, f, indent=2)

  print(f"Overlay image created at: {out_png_path} ({drawn} polygons)")
  return meta
