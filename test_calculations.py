"""เทสต์โค้ดที่ผลิตตัวเลขที่ระบบรายงานออกไป

ทุกค่าที่ผู้ใช้เห็น (พื้นที่ผิวแผง จำนวนแผง kWp kWh/ปี) ผ่าน
post_processing.process_shapefile_to_geojson ทั้งหมด ไฟล์นี้จึงตรึงสูตรไว้ด้วย
รูปทรงที่คำนวณด้วยมือได้ เพื่อให้รู้ตัวทันทีถ้าค่าคงที่หรือสูตรถูกแก้โดยไม่ตั้งใจ
"""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import geopandas as gpd
from shapely.geometry import Polygon

import data_retrieval
from post_processing import process_shapefile_to_geojson

# ค่าคงที่ที่สูตรใช้ (ดู post_processing.py)
PANEL_AREA_M2 = 2.541     # พื้นที่แผงหนึ่งแผ่นตามสเปกอาจารย์
WP_PER_M2 = 180.0         # กำลังติดตั้งต่อพื้นที่หนึ่งตารางเมตร
PEAK_SUN_HOURS = 4.2      # ชั่วโมงแดดเต็มต่อวัน
PERFORMANCE_RATIO = 0.75  # สัดส่วนที่ผลิตได้จริงหลังหักการสูญเสีย

UTM47N = 32647  # โซนที่ครอบคลุมประเทศไทย ใช้หน่วยเมตร จึงคำนวณพื้นที่ได้ตรง


def square(size_m: float, origin=(600000.0, 1550000.0)) -> Polygon:
  """สี่เหลี่ยมจัตุรัสในพิกัด UTM ด้านละ size_m เมตร"""
  x, y = origin
  return Polygon([
      (x, y), (x + size_m, y), (x + size_m, y + size_m), (x, y + size_m)
  ])


class CalculationTestCase(unittest.TestCase):
  def setUp(self):
    self.dir = tempfile.mkdtemp(prefix="solar_calc_")
    self.output_dir = os.path.join(self.dir, "output")
    os.makedirs(self.output_dir)
    self.geojson = os.path.join(self.dir, "result.geojson")

  def tearDown(self):
    shutil.rmtree(self.dir, ignore_errors=True)

  def write_shapefile(self, polygons):
    gdf = gpd.GeoDataFrame(
        {"id": list(range(len(polygons)))},
        geometry=polygons,
        crs=f"EPSG:{UTM47N}",
    )
    gdf.to_file(os.path.join(self.output_dir, "job-corrected.shp"))

  def run_processing(self):
    return process_shapefile_to_geojson(
        self.output_dir, output_geojson_path=self.geojson, job_name="job-test"
    )


class TestPanelMath(CalculationTestCase):
  def test_single_square_matches_hand_calculation(self):
    """สี่เหลี่ยม 10x10 เมตร = 100 ตร.ม. ตรวจทุกค่าที่คำนวณต่อจากนั้น"""
    self.write_shapefile([square(10.0)])
    summary = self.run_processing()

    self.assertEqual(summary["status"], "completed")
    self.assertEqual(summary["polygon_count"], 1)
    self.assertAlmostEqual(summary["total_surface_area_sqm"], 100.0, places=1)

    # 100 / 2.541 = 39.35 -> ปัดเป็น 39
    self.assertEqual(summary["total_panels"], 39)
    # 100 * 180 / 1000 = 18 kWp
    self.assertAlmostEqual(summary["total_capacity_kwp"], 18.0, places=2)
    # 18 * 4.2 * 365 * 0.75 = 20,695.5 kWh/ปี
    expected_kwh = 18.0 * PEAK_SUN_HOURS * 365 * PERFORMANCE_RATIO
    self.assertAlmostEqual(
        summary["total_annual_generation_kwh"], expected_kwh, places=1
    )
    self.assertAlmostEqual(expected_kwh, 20695.5, places=1)

  def test_totals_add_up_across_polygons(self):
    """ยอดรวมต้องเท่ากับผลบวกของแต่ละแปลง ไม่ใช่คำนวณจากพื้นที่รวมทีเดียว"""
    polygons = [
        square(10.0, (600000.0, 1550000.0)),
        square(20.0, (600100.0, 1550000.0)),
    ]
    self.write_shapefile(polygons)
    summary = self.run_processing()

    self.assertEqual(summary["polygon_count"], 2)
    self.assertAlmostEqual(
        summary["total_surface_area_sqm"], 100.0 + 400.0, places=1
    )
    # ปัดเศษรายแปลงแล้วค่อยบวก: round(39.35) + round(157.42) = 39 + 157
    self.assertEqual(summary["total_panels"], 39 + 157)
    self.assertAlmostEqual(summary["total_capacity_kwp"], 18.0 + 72.0, places=2)

  def test_capacity_scales_linearly_with_area(self):
    """พื้นที่สองเท่าต้องได้กำลังติดตั้งสองเท่าพอดี"""
    self.write_shapefile([square(10.0)])
    small = self.run_processing()["total_capacity_kwp"]

    shutil.rmtree(self.output_dir)
    os.makedirs(self.output_dir)
    self.write_shapefile([square(10.0 * (2 ** 0.5))])  # พื้นที่เป็นสองเท่า
    large = self.run_processing()["total_capacity_kwp"]

    self.assertAlmostEqual(large / small, 2.0, places=2)


class TestOutputFiles(CalculationTestCase):
  def test_geojson_is_written_in_wgs84(self):
    """หน้าเว็บวาดด้วย Leaflet จึงต้องได้ EPSG:4326 ไม่ใช่ UTM ที่ใช้คำนวณ"""
    self.write_shapefile([square(10.0)])
    self.run_processing()

    self.assertTrue(os.path.exists(self.geojson))
    result = gpd.read_file(self.geojson)
    self.assertEqual(result.crs.to_epsg(), 4326)

    # พิกัดต้องอยู่ในช่วงของประเทศไทยหลังแปลงกลับ
    minx, miny, maxx, maxy = result.total_bounds
    self.assertTrue(97 < minx < 106, f"ลองจิจูดผิดช่วง: {minx}")
    self.assertTrue(5 < miny < 21, f"ละติจูดผิดช่วง: {miny}")

  def test_per_polygon_attributes_are_in_the_geojson(self):
    """หน้าเว็บอ่านค่ารายแปลงจากไฟล์นี้โดยตรง (ไม่ได้เก็บใน Task_Result)"""
    self.write_shapefile([square(10.0)])
    self.run_processing()

    feature = gpd.read_file(self.geojson).iloc[0]
    for column in (
        "area_sqm", "estimated_panels", "capacity_kwp", "annual_generation_kwh"
    ):
      self.assertIn(column, feature.index)
    self.assertAlmostEqual(feature["area_sqm"], 100.0, places=1)

  def test_summary_json_matches_returned_summary(self):
    self.write_shapefile([square(10.0)])
    summary = self.run_processing()

    with open(os.path.join(self.dir, "summary.json"), encoding="utf-8") as f:
      on_disk = json.load(f)
    self.assertEqual(on_disk["total_panels"], summary["total_panels"])
    self.assertEqual(on_disk["polygon_count"], summary["polygon_count"])


class TestEdgeCases(CalculationTestCase):
  def test_no_shapefile_reports_failure(self):
    summary = self.run_processing()
    self.assertEqual(summary["status"], "failed")
    self.assertIn("shapefile", summary["message"].lower())

  def test_empty_shapefile_is_a_valid_zero_result(self):
    """ตรวจไม่พบแผงเลยไม่ใช่ความผิดพลาด พื้นที่นั้นอาจไม่มีแผงจริง ๆ"""
    gdf = gpd.GeoDataFrame({"id": []}, geometry=[], crs=f"EPSG:{UTM47N}")
    gdf.to_file(os.path.join(self.output_dir, "job-corrected.shp"))

    summary = self.run_processing()
    self.assertEqual(summary["status"], "completed")
    self.assertEqual(summary["polygon_count"], 0)
    self.assertEqual(summary["total_panels"], 0)
    self.assertEqual(summary["total_capacity_kwp"], 0.0)
    self.assertTrue(os.path.exists(self.geojson))

  def test_corrected_shapefile_wins_over_plain_one(self):
    """โมเดลออกไฟล์ที่แก้ค่าแล้วเป็น *-corrected.shp ต้องใช้ตัวนั้น"""
    plain = gpd.GeoDataFrame(
        {"id": [0]}, geometry=[square(50.0)], crs=f"EPSG:{UTM47N}"
    )
    plain.to_file(os.path.join(self.output_dir, "job-raw.shp"))
    self.write_shapefile([square(10.0)])  # ตัว corrected

    summary = self.run_processing()
    self.assertAlmostEqual(summary["total_surface_area_sqm"], 100.0, places=1)


class TestTileDownloadFailures(unittest.TestCase):
  """tile ที่โหลดไม่ได้ต้องทำให้งานล้มเหลว ไม่ใช่ปล่อยรูดำไว้บนภาพ"""

  def setUp(self):
    self.dir = tempfile.mkdtemp(prefix="solar_tile_")
    self.tif = os.path.join(self.dir, "satellite.tif")
    # กัน backoff ไม่ให้เทสต์ช้า
    self._backoff = data_retrieval.TILE_BACKOFF
    data_retrieval.TILE_BACKOFF = 0.0

  def tearDown(self):
    data_retrieval.TILE_BACKOFF = self._backoff
    shutil.rmtree(self.dir, ignore_errors=True)

  @staticmethod
  def _png_bytes(color=(120, 120, 120)):
    import io

    from PIL import Image

    buffer = io.BytesIO()
    Image.new("RGB", (256, 256), color).save(buffer, "PNG")
    return buffer.getvalue()

  def _response(self, status_code=200, content=None):
    return MagicMock(status_code=status_code, content=content)

  def _download(self):
    return data_retrieval.download_satellite_image(
        14.0700, 100.6020, 14.0705, 100.6025, zoom=18, output_tif_path=self.tif
    )

  def test_all_tiles_ok_writes_georeferenced_tif(self):
    ok = self._response(200, self._png_bytes())
    with patch("requests.Session.get", return_value=ok):
      path = self._download()

    self.assertTrue(os.path.exists(path))
    import rasterio

    with rasterio.open(path) as src:
      self.assertEqual(src.count, 3)
      self.assertEqual(src.crs.to_epsg(), 3857)
      self.assertGreater(src.width, 0)

  def test_persistent_http_error_raises_instead_of_leaving_a_hole(self):
    with patch("requests.Session.get", return_value=self._response(503)):
      with self.assertRaises(data_retrieval.TileDownloadError) as caught:
        self._download()

    self.assertIn("503", str(caught.exception))
    # ห้ามเขียนไฟล์ภาพที่ไม่ครบทิ้งไว้ให้ step ถัดไปหยิบไปใช้
    self.assertFalse(os.path.exists(self.tif))

  def test_network_error_raises(self):
    import requests as req

    with patch("requests.Session.get", side_effect=req.ConnectionError("เน็ตหลุด")):
      with self.assertRaises(data_retrieval.TileDownloadError):
        self._download()

  def test_transient_failure_recovers_on_retry(self):
    """พลาดสองครั้งแรกแล้วสำเร็จ ต้องได้ภาพครบ ไม่ใช่ล้มทั้งงาน"""
    ok = self._response(200, self._png_bytes())
    attempts = {"n": 0}

    def flaky(*_args, **_kwargs):
      attempts["n"] += 1
      if attempts["n"] <= 2:
        return self._response(500)
      return ok

    with patch("requests.Session.get", side_effect=flaky):
      path = self._download()

    self.assertTrue(os.path.exists(path))
    self.assertGreaterEqual(attempts["n"], 3)

  def test_area_too_large_is_rejected_before_downloading(self):
    with patch("requests.Session.get") as get:
      with self.assertRaises(ValueError) as caught:
        data_retrieval.download_satellite_image(
            14.00, 100.50, 14.10, 100.60, zoom=18, output_tif_path=self.tif
        )
    self.assertIn("too large", str(caught.exception))
    get.assert_not_called()


if __name__ == "__main__":
  unittest.main()
