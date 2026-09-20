# การรัน Pipeline ด้วย Apache Airflow

เอกสาร Requirement ระบุ *Workflow Management* เป็นหนึ่งใน Main Challenges และเลือก
Apache Airflow เป็นตัวจัดการ ไฟล์นี้อธิบายวิธีตั้งค่าและสิ่งที่เกิดขึ้นเบื้องหลัง

## ภาพรวม

```
เบราว์เซอร์ ──POST /tasks──► FastAPI ──┬──► บันทึก Task + Task_Step 4 แถว (pending)
                                        │
                                        └──► POST /api/v1/dags/.../dagRuns
                                             conf = {"tid": "<uuid>"}
                                                        │
                                                        ▼
                        Airflow DAG: solar_panel_detection_pipeline
                        generating_config → fetch_image → run_inference → parse_result
                                                        │
                                   callback เขียนสถานะกลับ ──► Task_Step / Task_Result
                                                        │
เบราว์เซอร์ ──GET /tasks/{tid}/progress──► FastAPI ──────┘  (อ่านจากตาราง ไม่ได้ถาม Airflow)
```

จุดสำคัญ: **หน้าเว็บไม่เคยคุยกับ Airflow เพื่อขอสถานะ** มันอ่านจากตาราง `Task_Step`
อย่างเดียว Airflow เป็นฝ่ายเขียนสถานะลงมาเอง ทำให้หน้าเว็บทำงานต่อได้แม้ Airflow ล่ม

## การ retry

Airflow เป็นคนจัดการ retry ไม่ใช่โค้ดเรา — `retries = MAX_RETRY - 1 = 4`
จึงได้ 5 ครั้งรวมครั้งแรก ตรงตามเอกสาร แล้ว callback จะสะท้อนกลับมาที่ฐานข้อมูล

| เหตุการณ์ใน Airflow | ผลที่เขียนลง Task_Step |
|---|---|
| เริ่มทำงาน | `status = running`, `started_at` |
| ล้มเหลวแต่ยังลองต่อได้ | `retry_count += 1`, `error_msg`, ล้างโฟลเดอร์ |
| ล้มเหลวจนครบ 5 ครั้ง | `status = failed`, `error_msg` |
| สำเร็จ | `status = completed`, `completed_at` |

การล้างโฟลเดอร์ก่อน retry เก็บ `input/` ไว้ตามเอกสาร **ยกเว้น** ตอนที่ step ที่ล้มเหลว
คือ `fetch_image` เอง เพราะไฟล์ภาพที่ดาวน์โหลดค้างไว้ไม่ครบต้องถูกทิ้ง

## วิธีเปิดใช้งาน

### 1. เปิด Airflow + PostgreSQL

ต้องมี Docker Desktop เปิดอยู่ (บน Windows ใช้ผ่าน WSL2)

```bash
docker compose up -d --build
```

ครั้งแรกจะนานหน่อยเพราะต้อง build image ที่มี GDAL/rasterio/geopandas
เสร็จแล้วเปิด <http://localhost:8080> ล็อกอินด้วย `airflow` / `airflow`

### 2. ชี้ฝั่งเว็บมาที่ฐานข้อมูลเดียวกัน

ทั้งเว็บและ Airflow **ต้องใช้ฐานข้อมูลตัวเดียวกัน** ไม่งั้นเว็บจะมองไม่เห็นสถานะที่
Airflow เขียนลงไป

```bash
export SOLAR_DATABASE_URL="postgresql+psycopg2://airflow:airflow@localhost:5432/solar"
export SOLAR_AIRFLOW_URL="http://localhost:8080"
python -m uvicorn app:app --reload --port 8008
```

PowerShell:

```powershell
$env:SOLAR_DATABASE_URL = "postgresql+psycopg2://airflow:airflow@localhost:5432/solar"
$env:SOLAR_AIRFLOW_URL  = "http://localhost:8080"
python -m uvicorn app:app --reload --port 8008
```

ต้องติดตั้ง `psycopg2-binary` ในเครื่องด้วย (`pip install psycopg2-binary`)

### 3. ตรวจว่าต่อติดแล้ว

```bash
curl http://localhost:8008/health
```

ควรได้ `"executor": "airflow"` และ `"reachable": true`

## โหมดไม่มี Airflow

ถ้า **ไม่ได้ตั้ง** `SOLAR_AIRFLOW_URL` ระบบจะรัน pipeline ในโปรเซสของ FastAPI เอง
ผ่าน `BackgroundTasks` และใช้ SQLite ตามเดิม — สะดวกตอนพัฒนาและตอนรันเทสต์
สถานะที่เขียนลง `Task_Step` เหมือนกันทุกประการ ต่างแค่ใครเป็นคนรัน

ดูได้จาก `GET /health` ว่าตอนนี้ใช้โหมดไหน

## ตัวแปรสภาพแวดล้อม

| ตัวแปร | ค่าเริ่มต้น | ความหมาย |
|---|---|---|
| `SOLAR_DATABASE_URL` | `sqlite:///data/solar_jobs.db` | ฐานข้อมูลของโปรเจกต์ |
| `SOLAR_DATA_DIR` | `data/inference` | ที่เก็บไฟล์ของแต่ละ Task |
| `SOLAR_AIRFLOW_URL` | *(ว่าง)* | ว่าง = รันเอง, มีค่า = ส่งให้ Airflow |
| `SOLAR_AIRFLOW_USER` | `airflow` | ผู้ใช้สำหรับ REST API |
| `SOLAR_AIRFLOW_PASSWORD` | `airflow` | รหัสผ่านสำหรับ REST API |
| `SOLAR_AIRFLOW_DAG_ID` | `solar_panel_detection_pipeline` | ชื่อ DAG |
| `SOLAR_AIRFLOW_API` | `v1` | `v1` สำหรับ Airflow 2.x, `v2` สำหรับ 3.x |

## ข้อควรระวัง

- **ค่าที่ตั้งไว้ใน `docker-compose.yml` เป็นรหัสผ่านสำหรับพัฒนาเท่านั้น**
  (`airflow`/`airflow`) ถ้านำขึ้นเซิร์ฟเวอร์จริงต้องเปลี่ยน และไม่ควรเปิดพอร์ต
  5432 ออกสู่ภายนอก
- โฟลเดอร์โปรเจกต์ถูก mount เข้า container ที่ `/opt/project` แก้โค้ดแล้ว Airflow
  เห็นทันที แต่ถ้าแก้ `requirements.txt` ต้อง `docker compose build` ใหม่
- `run.sh` ของโมเดลจริงยังไม่ได้อยู่ในโปรเจกต์นี้ ตอนนี้ `run_inference` จึงถอยไปใช้
  `mock_model.py` แทนโดยอัตโนมัติ
