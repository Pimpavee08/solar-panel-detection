from datetime import datetime
import os
import shutil
import sqlite3
from typing import List, Optional
from data_retrieval import download_satellite_image
from fastapi import BackgroundTasks, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from model_runner import execute_model_inference
from post_processing import process_shapefile_to_geojson, save_summary_to_db
from pydantic import BaseModel, Field

app = FastAPI(
    title="Solar Panel Detection Pipeline API",
    description="REST API สำหรับควบคุมดาวน์โหลดภาพดาวเทียม รัน AI โมเดล และคำนวณกำลังการผลิตแผงโซลาร์เซลล์",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = "data/solar_jobs.db"


class JobCreate(BaseModel):
  job_name: str = Field(..., pattern="^[a-zA-Z0-9_-]+$")
  min_lat: float
  min_lon: float
  max_lat: float
  max_lon: float
  zoom: int = Field(18, ge=1, le=21)


class JobResponse(BaseModel):
  job_name: str
  status: str
  polygon_count: int
  total_surface_area_sqm: float
  total_panels: int
  total_capacity_kwp: float
  total_annual_generation_kwh: float
  geojson_path: Optional[str] = None
  updated_at: Optional[str] = None


def get_job_from_db(job_name: str) -> Optional[dict]:
  if not os.path.exists(DB_PATH):
    return None
  conn = sqlite3.connect(DB_PATH)
  conn.row_factory = sqlite3.Row
  cursor = conn.cursor()
  cursor.execute("SELECT * FROM job_results WHERE job_name = ?", (job_name,))
  row = cursor.fetchone()
  conn.close()
  return dict(row) if row else None


def get_all_jobs_from_db() -> List[dict]:
  if not os.path.exists(DB_PATH):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_results (
            job_name TEXT PRIMARY KEY,
            status TEXT,
            polygon_count INTEGER,
            total_surface_area_sqm REAL,
            total_panels INTEGER,
            total_capacity_kwp REAL,
            total_annual_generation_kwh REAL,
            geojson_path TEXT,
            updated_at TEXT
        )
        """)
    conn.commit()
    conn.close()
    return []

  conn = sqlite3.connect(DB_PATH)
  conn.row_factory = sqlite3.Row
  cursor = conn.cursor()
  cursor.execute("SELECT * FROM job_results ORDER BY updated_at DESC")
  rows = cursor.fetchall()
  conn.close()
  return [dict(row) for row in rows]


def run_pipeline_background(
    job_name: str,
    min_lat: float,
    min_lon: float,
    max_lat: float,
    max_lon: float,
    zoom: int,
):
  base_dir = "data/inference"
  job_dir = os.path.join(base_dir, job_name)
  input_dir = os.path.join(job_dir, "input")
  os.makedirs(input_dir, exist_ok=True)

  running_summary = {
      "status": "running",
      "polygon_count": 0,
      "total_surface_area_sqm": 0.0,
      "total_panels": 0,
      "total_capacity_kwp": 0.0,
      "total_annual_generation_kwh": 0.0,
      "geojson_path": "",
      "processed_at": datetime.now().isoformat(),
  }
  save_summary_to_db(job_name, running_summary, DB_PATH)

  try:
    tif_path = os.path.join(input_dir, "satellite.tif")
    download_satellite_image(
        min_lat, min_lon, max_lat, max_lon, zoom=zoom, output_tif_path=tif_path
    )
    output_dir = execute_model_inference(job_name, base_dir=base_dir)
    geojson_path = os.path.join(job_dir, "result.geojson")
    process_shapefile_to_geojson(
        output_dir,
        output_geojson_path=geojson_path,
        job_name=job_name,
        db_path=DB_PATH,
    )
  except Exception as e:
    failed_summary = {
        "status": f"failed: {str(e)}",
        "polygon_count": 0,
        "total_surface_area_sqm": 0.0,
        "total_panels": 0,
        "total_capacity_kwp": 0.0,
        "total_annual_generation_kwh": 0.0,
        "geojson_path": "",
        "processed_at": datetime.now().isoformat(),
    }
    save_summary_to_db(job_name, failed_summary, DB_PATH)


# -------------------------------------------------------------
# หน้าเว็บ Dashboard ภาษาไทยแบบใช้งานง่าย (HTML Web UI)
# -------------------------------------------------------------
HTML_DASHBOARD = """
<!DOCTYPE html>
<html lang="th">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ระบบตรวจจับแผงโซลาร์เซลล์อัตโนมัติ (Solar Dashboard)</title>
    <!-- Leaflet Map CSS -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
        body { background: #f0f2f5; color: #333; padding: 20px; }
        .header { background: #1e293b; color: white; padding: 20px; border-radius: 12px; margin-bottom: 20px; display: flex; justify-content: space-between; align-items: center; }
        .header h1 { font-size: 24px; display: flex; align-items: center; gap: 10px; }
        .grid { display: grid; grid-template-columns: 380px 1fr; gap: 20px; }
        .card { background: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); margin-bottom: 20px; }
        .card h2 { font-size: 18px; margin-bottom: 15px; border-bottom: 2px solid #f1f5f9; padding-bottom: 8px; color: #0f172a; }
        .form-group { margin-bottom: 12px; }
        .form-group label { display: block; font-size: 13px; font-weight: bold; margin-bottom: 5px; color: #475569; }
        .form-group input { width: 100%; padding: 10px; border: 1px solid #cbd5e1; border-radius: 8px; font-size: 14px; }
        .btn-preset { background: #e2e8f0; border: none; padding: 6px 12px; border-radius: 6px; font-size: 12px; cursor: pointer; margin-right: 5px; margin-bottom: 10px; transition: 0.2s; }
        .btn-preset:hover { background: #cbd5e1; }
        .btn-submit { width: 100%; background: #16a34a; color: white; border: none; padding: 12px; border-radius: 8px; font-size: 16px; font-weight: bold; cursor: pointer; transition: 0.2s; margin-top: 5px; }
        .btn-submit:hover { background: #15803d; }
        #map { height: 380px; width: 100%; border-radius: 8px; margin-bottom: 15px; border: 1px solid #cbd5e1; }
        .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 20px; }
        .stat-box { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 12px; text-align: center; }
        .stat-box .title { font-size: 12px; color: #64748b; margin-bottom: 5px; }
        .stat-box .value { font-size: 20px; font-weight: bold; color: #0284c7; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #e2e8f0; font-size: 13px; }
        th { background: #f8fafc; color: #475569; }
        .status-badge { padding: 4px 8px; border-radius: 12px; font-size: 11px; font-weight: bold; display: inline-block; }
        .status-completed { background: #dcfce7; color: #15803d; }
        .status-running { background: #fef9c3; color: #a16207; }
        .status-failed { background: #fee2e2; color: #b91c1c; }
        .action-btn { padding: 5px 10px; border: none; border-radius: 6px; cursor: pointer; font-size: 12px; font-weight: bold; margin-right: 4px; }
        .btn-view { background: #0284c7; color: white; }
        .btn-download { background: #0d9488; color: white; text-decoration: none; display: inline-block; }
        .btn-del { background: #ef4444; color: white; }
    </style>
</head>
<body>

    <div class="header">
        <h1>☀️ ระบบตรวจจับแผงโซลาร์เซลล์จากภาพถ่ายดาวเทียม</h1>
        <a href="/docs" style="color: #93c5fd; text-decoration: none; font-size: 14px;">🛠️ สำหรับนักพัฒนา (Swagger UI)</a>
    </div>

    <div class="grid">
        <!-- ฝั่งซ้าย: ฟอร์มสร้างงานใหม่ -->
        <div>
            <div class="card">
                <h2>➕ สร้างงานตรวจจับใหม่ (New Job)</h2>
                
                <div style="margin-bottom: 10px;">
                    <span style="font-size: 12px; color: #64748b;">📍 ปุ่มลัดเลือกพิกัดตัวอย่าง:</span><br>
                    <button class="btn-preset" onclick="setCoords('SIIT ม.ธรรมศาสตร์ รังสิต', 14.0700, 100.6020, 14.0740, 100.6070)">🏫 SIIT รังสิต</button>
                    <button class="btn-preset" onclick="setCoords('นิคมอุตสาหกรรมบางปู', 13.5180, 100.6480, 13.5220, 100.6530)">🏭 นิคมฯ บางปู</button>
                </div>

                <div class="form-group">
                    <label>ชื่อโปรเจกต์ (Job Name):</label>
                    <input type="text" id="job_name" value="job_siit_demo" placeholder="เช่น job_test_01">
                </div>
                <div class="form-group">
                    <label>ละติจูดต่ำสุด (Min Lat):</label>
                    <input type="number" step="0.0001" id="min_lat" value="14.0700">
                </div>
                <div class="form-group">
                    <label>ลองจิจูดต่ำสุด (Min Lon):</label>
                    <input type="number" step="0.0001" id="min_lon" value="100.6020">
                </div>
                <div class="form-group">
                    <label>ละติจูดสูงสุด (Max Lat):</label>
                    <input type="number" step="0.0001" id="max_lat" value="14.0740">
                </div>
                <div class="form-group">
                    <label>ลองจิจูดสูงสุด (Max Lon):</label>
                    <input type="number" step="0.0001" id="max_lon" value="100.6070">
                </div>
                <button class="btn-submit" onclick="submitJob()">🚀 เริ่มตรวจจับแผงโซลาร์เซลล์</button>
            </div>
        </div>

        <!-- ฝั่งขวา: แผนที่ + รายการผลลัพธ์ -->
        <div>
            <!-- แผนที่แสดงผลลัพธ์ -->
            <div class="card">
                <h2>🗺️ แผนที่แสดงผลการตรวจจับ (Map Viewer)</h2>
                <div id="map"></div>
                
                <div class="stats-grid">
                    <div class="stat-box">
                        <div class="title">พื้นที่ผิวแผงรวม</div>
                        <div class="value" id="stat_area">- ตร.ม.</div>
                    </div>
                    <div class="stat-box">
                        <div class="title">จำนวนแผงโดยประมาณ</div>
                        <div class="value" id="stat_panels">- แผง</div>
                    </div>
                    <div class="stat-box">
                        <div class="title">กำลังการผลิตรวม</div>
                        <div class="value" id="stat_capacity">- kWp</div>
                    </div>
                    <div class="stat-box">
                        <div class="title">พลังงานไฟฟ้าต่อปี</div>
                        <div class="value" id="stat_energy">- kWh</div>
                    </div>
                </div>
            </div>

            <!-- ตารางรายการประวัติงาน -->
            <div class="card">
                <h2>📋 ประวัติรายการงานทั้งหมด (Job List)</h2>
                <table>
                    <thead>
                        <tr>
                            <th>ชื่อ Job</th>
                            <th>สถานะ</th>
                            <th>จำนวนแผง</th>
                            <th>จัดการ / ดูผลลัพธ์</th>
                        </tr>
                    </thead>
                    <tbody id="jobs_table_body">
                        <tr><td colspan="4" style="text-align:center;">กำลังโหลดข้อมูล...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- Leaflet Map JS -->
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script>
        // สร้างแผนที่ Leaflet (Base map เป็น Google Satellite)
        const map = L.map('map').setView([14.0720, 100.6045], 16);
        L.tileLayer('https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}', {
            maxZoom: 20,
            attribution: '© Google Satellite'
        }).addTo(map);

        let geojsonLayer = null;

        function setCoords(name, minLat, minLon, maxLat, maxLon) {
            document.getElementById('job_name').value = 'job_' + Date.now().toString().slice(-4);
            document.getElementById('min_lat').value = minLat;
            document.getElementById('min_lon').value = minLon;
            document.getElementById('max_lat').value = maxLat;
            document.getElementById('max_lon').value = maxLon;
        }

        async function submitJob() {
            const payload = {
                job_name: document.getElementById('job_name').value.trim(),
                min_lat: parseFloat(document.getElementById('min_lat').value),
                min_lon: parseFloat(document.getElementById('min_lon').value),
                max_lat: parseFloat(document.getElementById('max_lat').value),
                max_lon: parseFloat(document.getElementById('max_lon').value),
                zoom: 18
            };

            if (!payload.job_name) { alert('กรุณาระบุชื่อ Job'); return; }

            const res = await fetch('/jobs', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload)
            });

            if (res.ok) {
                alert('✅ ระบบเริ่มรันงาน "' + payload.job_name + '" ในเบื้องหลังแล้ว!');
                loadJobs();
            } else {
                const err = await res.json();
                alert('❌ เกิดข้อผิดพลาด: ' + (err.detail || 'ไม่สามารถสั่งรันได้'));
            }
        }

        async function loadJobs() {
            const res = await fetch('/jobs');
            const jobs = await res.json();
            const tbody = document.getElementById('jobs_table_body');
            tbody.innerHTML = '';

            if (jobs.length === 0) {
                tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;">ยังไม่มีรายการงานในระบบ</td></tr>';
                return;
            }

            jobs.forEach(job => {
                let statusClass = 'status-running';
                let statusText = 'กำลังประมวลผล...';
                if (job.status === 'completed') { statusClass = 'status-completed'; statusText = 'เสร็จสิ้น'; }
                else if (job.status.startsWith('failed')) { statusClass = 'status-failed'; statusText = 'ล้มเหลว'; }

                const row = document.createElement('tr');
                row.innerHTML = `
                    <td><strong>${job.job_name}</strong></td>
                    <td><span class="status-badge ${statusClass}">${statusText}</span></td>
                    <td>${job.total_panels ? job.total_panels.toLocaleString() + ' แผง' : '-'}</td>
                    <td>
                        <button class="action-btn btn-view" onclick="viewJobOnMap('${job.job_name}')">🗺️ ดูบนแผนที่</button>
                        ${job.status === 'completed' ? `<a href="/jobs/${job.job_name}/geojson" class="action-btn btn-download">📥 โหลด GeoJSON</a>` : ''}
                        <button class="action-btn btn-del" onclick="deleteJob('${job.job_name}')">🗑️</button>
                    </td>
                `;
                tbody.appendChild(row);
            });
        }

        async function viewJobOnMap(jobName) {
            const res = await fetch('/jobs/' + jobName);
            if (!res.ok) return;
            const job = await res.json();

            // อัปเดตการ์ดตัวเลขสถิติ
            document.getElementById('stat_area').innerText = (job.total_surface_area_sqm || 0).toLocaleString() + ' ตร.ม.';
            document.getElementById('stat_panels').innerText = (job.total_panels || 0).toLocaleString() + ' แผง';
            document.getElementById('stat_capacity').innerText = (job.total_capacity_kwp || 0).toLocaleString() + ' kWp';
            document.getElementById('stat_energy').innerText = (job.total_annual_generation_kwh || 0).toLocaleString() + ' kWh';

            // ดึงไฟล์ GeoJSON มาวาดบนแผนที่
            const geoRes = await fetch('/jobs/' + jobName + '/geojson');
            if (geoRes.ok) {
                const geojson = await geoRes.json();
                if (geojsonLayer) map.removeLayer(geojsonLayer);

                geojsonLayer = L.geoJSON(geojson, {
                    style: { color: '#00e5ff', weight: 3, fillOpacity: 0.5, fillColor: '#00b0ff' },
                    onEachFeature: (feature, layer) => {
                        const p = feature.properties || {};
                        layer.bindPopup(`<b>แผงโซลาร์เซลล์</b><br>พื้นที่: ${p.area_sqm || 0} ตร.ม.<br>จำนวน: ${p.estimated_panels || 0} แผง`);
                    }
                }).addTo(map);

                // ซูมแผนที่ไปที่ตำแหน่งแผงโซลาร์เซลล์
                if (geojsonLayer.getLayers().length > 0) {
                    map.fitBounds(geojsonLayer.getBounds());
                }
            }
        }

        async function deleteJob(jobName) {
            if (confirm('คุณต้องการลบงาน "' + jobName + '" ใช่หรือไม่?')) {
                await fetch('/jobs/' + jobName, { method: 'DELETE' });
                loadJobs();
            }
        }

        // โหลดรายการงานและรีเฟรชทุก 4 วินาที
        loadJobs();
        setInterval(loadJobs, 4000);
    </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse, tags=["General"])
def read_root():
  """หน้าเว็บ Dashboard ภาษาไทยแบบใช้งานง่ายสำหรับผู้ใช้ทั่วไป"""
  return HTML_DASHBOARD


# -------------------------------------------------------------
# Endpoints อื่นๆ ยังคงอยู่ครบเหมือนเดิม 100%
# -------------------------------------------------------------
@app.post("/jobs", status_code=status.HTTP_202_ACCEPTED, tags=["Jobs"])
def create_job(payload: JobCreate, background_tasks: BackgroundTasks):
  if payload.min_lat >= payload.max_lat:
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="พิกัดละติจูดสูงสุด (max_lat) ต้องมากกว่าละติจูดต่ำสุด (min_lat)",
    )
  if payload.min_lon >= payload.max_lon:
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="พิกัดลองจิจูดสูงสุด (max_lon) ต้องมากกว่าลองจิจูดต่ำสุด (min_lon)",
    )

  existing_job = get_job_from_db(payload.job_name)
  if existing_job and existing_job.get("status") == "running":
    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=(
            f"Job '{payload.job_name}' กำลังทำงานอยู่ในระบบ"
            " กรุณารอจนกว่าจะทำงานเสร็จสิ้น"
        ),
    )

  background_tasks.add_task(
      run_pipeline_background,
      payload.job_name,
      payload.min_lat,
      payload.min_lon,
      payload.max_lat,
      payload.max_lon,
      payload.zoom,
  )

  return {
      "message": (
          f"เริ่มรัน Job '{payload.job_name}' ในระบบเบื้องหลังแล้วสำเร็จ"
          f" คุณสามารถติดตามสถานะผ่าน GET /jobs/{payload.job_name}"
      ),
      "job_name": payload.job_name,
      "status": "running",
  }


@app.get("/jobs", response_model=List[JobResponse], tags=["Jobs"])
def list_jobs():
  return get_all_jobs_from_db()


@app.get("/jobs/{job_name}", response_model=JobResponse, tags=["Jobs"])
def get_job(job_name: str):
  job = get_job_from_db(job_name)
  if not job:
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"ไม่พบ Job ชื่อ '{job_name}' ในฐานข้อมูล",
    )
  return job


@app.delete("/jobs/{job_name}", tags=["Jobs"])
def delete_job(job_name: str):
  job = get_job_from_db(job_name)
  conn = sqlite3.connect(DB_PATH)
  cursor = conn.cursor()
  cursor.execute(
      "SELECT job_name FROM job_results WHERE job_name = ?", (job_name,)
  )
  row = cursor.fetchone()
  if not row and not job:
    conn.close()
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"ไม่พบ Job ชื่อ '{job_name}' ในระบบ",
    )

  cursor.execute("DELETE FROM job_results WHERE job_name = ?", (job_name,))
  conn.commit()
  conn.close()

  job_dir = os.path.join("data/inference", job_name)
  disk_deleted = False
  if os.path.exists(job_dir):
    try:
      shutil.rmtree(job_dir)
      disk_deleted = True
    except Exception as e:
      print(f"Error deleting directory {job_dir}: {e}")

  return {
      "message": f"ลบข้อมูล Job '{job_name}' ออกจากระบบสำเร็จ",
      "database_deleted": True,
      "disk_files_deleted": disk_deleted,
  }


@app.get("/jobs/{job_name}/geojson", tags=["Downloads"])
def download_geojson(job_name: str):
  job = get_job_from_db(job_name)
  if not job:
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"ไม่พบข้อมูล Job '{job_name}' ในระบบ",
    )

  geojson_path = os.path.join("data/inference", job_name, "result.geojson")
  if not os.path.exists(geojson_path):
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=(
            f"ไม่พบไฟล์ GeoJSON สำหรับ Job '{job_name}'"
            " (อาจจะกำลังทำงานอยู่หรือล้มเหลว)"
        ),
    )

  return FileResponse(
      path=geojson_path,
      media_type="application/geo+json",
      filename=f"{job_name}-result.geojson",
  )


@app.get("/jobs/{job_name}/satellite", tags=["Downloads"])
def download_satellite(job_name: str):
  tif_path = os.path.join("data/inference", job_name, "input", "satellite.tif")
  if not os.path.exists(tif_path):
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"ไม่พบไฟล์ภาพถ่ายดาวเทียมสำหรับ Job '{job_name}'",
    )

  return FileResponse(
      path=tif_path, media_type="image/tiff", filename=f"{job_name}-satellite.tif"
  )