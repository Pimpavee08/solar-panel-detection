import io
import math
import os
import time
import mercantile
import numpy as np
from PIL import Image
import rasterio
from rasterio.transform import from_bounds
import requests

GOOGLE_SAT_URL = "https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

TILE_RETRIES = int(os.environ.get("SOLAR_TILE_RETRIES", "3"))
TILE_TIMEOUT = float(os.environ.get("SOLAR_TILE_TIMEOUT", "10"))
TILE_BACKOFF = float(os.environ.get("SOLAR_TILE_BACKOFF", "0.5"))


class TileDownloadError(RuntimeError):
  """ดาวน์โหลด tile ไม่สำเร็จจนหมดโควต้าที่ลองใหม่"""


def _fetch_tile(session, tile) -> bytes:
  """ดึง tile หนึ่งแผ่น ลองใหม่ได้ TILE_RETRIES ครั้งก่อนยอมแพ้

  ต้องโยน exception เมื่อล้มเหลวจริง ห้ามคืนค่าว่างเด็ดขาด ไม่งั้นบริเวณนั้น
  บนภาพจะเหลือเป็นสีดำแล้วโมเดลจะตรวจไม่เจอแผงโดยที่ไม่มีใครรู้ว่าภาพขาด
  """
  url = GOOGLE_SAT_URL.format(x=tile.x, y=tile.y, z=tile.z)
  problem = "ไม่ทราบสาเหตุ"

  for attempt in range(TILE_RETRIES):
    try:
      resp = session.get(url, headers=HEADERS, timeout=TILE_TIMEOUT)
      if resp.status_code == 200:
        return resp.content
      problem = f"HTTP {resp.status_code}"
    except requests.RequestException as exc:
      problem = f"{type(exc).__name__}: {exc}"

    if attempt < TILE_RETRIES - 1:
      time.sleep(TILE_BACKOFF * (2 ** attempt))

  raise TileDownloadError(
      f"ดึง tile z{tile.z}/{tile.x}/{tile.y} ไม่สำเร็จหลังลอง {TILE_RETRIES} ครั้ง"
      f" ({problem})"
  )


def download_satellite_image(
    min_lat: float,
    min_lon: float,
    max_lat: float,
    max_lon: float,
    zoom: int = 18,
    output_tif_path: str = "satellite_input.tif",
) -> str:
  """ดาวน์โหลดภาพถ่ายดาวเทียมจาก Bounding Box และบันทึกเป็น GeoTIFF

  CRS: EPSG:3857 (Web Mercator)
  """
  # 1. คำนวณรายการ Tiles ที่ครอบคลุม Bounding Box
  tiles = list(mercantile.tiles(min_lon, min_lat, max_lon, max_lat, zoom))
  if not tiles:
    raise ValueError("Invalid bounding box or zoom level.")

  min_x = min(t.x for t in tiles)
  max_x = max(t.x for t in tiles)
  min_y = min(t.y for t in tiles)
  max_y = max(t.y for t in tiles)

  tile_count_x = max_x - min_x + 1
  tile_count_y = max_y - min_y + 1

  # ตรวจสอบขนาดพื้นที่
  if tile_count_x * tile_count_y > 100:
    raise ValueError(
        f"Requested area is too large ({tile_count_x * tile_count_y} tiles)."
    )

  # 2. สร้าง Canvas ผืนใหญ่สำหรับต่อภาพ
  width = tile_count_x * 256
  height = tile_count_y * 256
  canvas = Image.new("RGB", (width, height))

  # 3. ดาวน์โหลดแต่ละ Tile และนำมาต่อลง Canvas
  # ใช้ Session เดียวเพื่อใช้ connection ซ้ำ เร็วกว่าเปิดใหม่ทุก tile
  with requests.Session() as session:
    for tile in tiles:
      content = _fetch_tile(session, tile)  # ล้มเหลวจริงจะโยน TileDownloadError
      tile_img = Image.open(io.BytesIO(content)).convert("RGB")
      pos_x = (tile.x - min_x) * 256
      pos_y = (tile.y - min_y) * 256
      canvas.paste(tile_img, (pos_x, pos_y))

  # 4. คำนวณพิกัด Georeferencing (EPSG:3857)
  ul_bounds = mercantile.xy_bounds(min_x, min_y, zoom)
  lr_bounds = mercantile.xy_bounds(max_x, max_y, zoom)

  transform = from_bounds(
      ul_bounds.left,
      lr_bounds.bottom,
      lr_bounds.right,
      ul_bounds.top,
      width,
      height,
  )

  # 5. บันทึกไฟล์เป็น GeoTIFF
  img_array = np.array(canvas)
  with rasterio.open(
      output_tif_path,
      "w",
      driver="GTiff",
      height=height,
      width=width,
      count=3,
      dtype=img_array.dtype,
      crs="EPSG:3857",
      transform=transform,
  ) as dst:
    for i in range(3):
      dst.write(img_array[:, :, i], i + 1)

  print(f"GeoTIFF saved successfully: {output_tif_path}")
  return output_tif_path