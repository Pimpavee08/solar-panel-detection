# Solar Panel Detection Pipeline

ระบบตรวจจับแผงโซลาร์เซลล์จากภาพถ่ายดาวเทียม ผู้ใช้ลากกรอบพื้นที่บนแผนที่ ระบบจะ
ดึงภาพจาก Google ส่งเข้าโมเดล U-Net (EfficientNetB7) แล้วสรุปพื้นที่ผิวแผง จำนวนแผง
และกำลังการผลิตกลับมาแสดงบนแผนที่

โครงงานหนึ่งภาคการศึกษา ต่อยอดโมเดลของ ผศ.ดร.อภิชน วิทยางกูร และคุณ John Sullivan
เพื่อใช้ประเมินปริมาณโซลาร์รูฟท็อปในระดับประเทศ

## ขั้นตอนการทำงาน

```
generating_config → fetch_image → run_inference → parse_result
      .toml          GeoTIFF       shapefile      GeoJSON + overlay + ตัวเลขสรุป
```

แต่ละขั้นบันทึกสถานะลงตาราง `Task_Step` ถ้าล้มเหลวจะลองใหม่สูงสุด 5 ครั้งพร้อม
เก็บสาเหตุไว้ และเขียน `Task_Result` เมื่อขั้นสุดท้ายสำเร็จเท่านั้น

## เปิดด้วยคลิกเดียว

ดับเบิลคลิก **`start.cmd`** — สคริปต์จะเปิด Docker Desktop, Airflow, PostgreSQL
และเว็บให้ครบ แล้วเปิดเบราว์เซอร์ไปที่ <http://localhost:8009> เอง

```bash
.\start.ps1
```

```bash
.\start.ps1 -Simple
```

แบบ `-Simple` ไม่ใช้ Docker รันเว็บกับ SQLite ที่พอร์ต 8008 · กด Ctrl+C เพื่อปิดเว็บ
และสั่ง `docker compose down` ถ้าจะปิด Airflow ด้วย

> ดับเบิลคลิก `start.ps1` ตรง ๆ ไม่ได้ Windows จะเปิดไฟล์ใน Notepad แทนการรัน
> จึงมี `start.cmd` ไว้ครอบอีกชั้น

## รันเองทีละคำสั่ง

ไม่ต้องติดตั้ง Docker หรือ Airflow — เว็บจะรัน pipeline ในตัวเอง ใช้ SQLite

```bash
pip install -r requirements.txt
python -m uvicorn app:app --reload --port 8008
```

เปิด <http://localhost:8008> แล้วกด "สมัครสมาชิก"

รันจากบรรทัดคำสั่งอย่างเดียวก็ได้:

```bash
python main.py --title "นิคมฯ บางปู" --bbox 13.5180 100.6480 13.5220 100.6530
```

## แบบเต็มระบบ (Airflow + PostgreSQL)

```bash
docker compose up -d --build
```

แล้วชี้เว็บมาที่ฐานข้อมูลเดียวกัน — ดูขั้นตอนครบใน [AIRFLOW.md](AIRFLOW.md)

ตรวจว่ากำลังใช้โหมดไหนอยู่ได้ที่ `GET /health`

## โครงสร้างไฟล์

| ไฟล์ | หน้าที่ |
|---|---|
| `app.py` | REST API ทั้งหมด |
| `static/dashboard.html` | หน้าเว็บ (แผนที่ ผลลัพธ์ log ผู้ใช้) |
| `pipeline.py` | ตัวเดิน 4 step และกฎการ retry |
| `models.py` | ตารางตาม ER (`User` / `Task` / `Task_Step` / `Task_Result`) |
| `db.py` | การต่อฐานข้อมูลและการเพิ่มคอลัมน์ที่มาทีหลัง |
| `auth.py` | รหัสผ่าน session และการจำกัดอัตราล็อกอิน |
| `data_retrieval.py` | ต่อ tile จาก Google เป็น GeoTIFF |
| `model_runner.py` | สร้าง `.toml` และเรียกโมเดล |
| `post_processing.py` | shapefile → GeoJSON + คำนวณรายแผง |
| `overlay.py` | วาดผลการตรวจจับทับภาพต้นฉบับ |
| `airflow_client.py` | สั่ง DAG และดึง log ผ่าน REST API |
| `solar_pipeline_dag.py` | DAG (ตัวชี้อยู่ที่ `dags/solar_dag.py`) |
| `manage_users.py` | จัดการบัญชีผู้ใช้จากบรรทัดคำสั่ง |
| `start.ps1` / `start.cmd` | เปิดระบบทั้งชุดด้วยคำสั่งเดียว |
| `mock_model.py` | โมเดลจำลองสำหรับทดสอบเมื่อไม่มี `run.sh` |

## เอกสารเพิ่มเติม

- [DEVELOPMENT.md](DEVELOPMENT.md) — พัฒนาอะไรเพิ่มจากโค้ดตั้งต้นบ้าง และทำไม
- [AIRFLOW.md](AIRFLOW.md) — ตั้ง Airflow, การ retry, ตัวแปรสภาพแวดล้อม
- [AUTH.md](AUTH.md) — ระบบบัญชีผู้ใช้และการป้องกันที่ใส่ไว้

## การทดสอบ

```bash
python -m pytest -q
```

เทสต์ใช้ฐานข้อมูลชั่วคราว ไม่แตะข้อมูลจริง และไม่ต้องติดตั้ง Airflow
(mock แพ็กเกจไว้) แต่ **ต้องต่ออินเทอร์เน็ตได้** เพราะบาง step ดึงภาพจริง

## ข้อจำกัดที่ยังมีอยู่

- **ยังไม่ได้ต่อโมเดลจริง** — ไม่มี `run.sh` ในโปรเจกต์ `run_inference` จึงถอยไปใช้
  `mock_model.py` ที่สร้างรูปหลายเหลี่ยมปลอมสองชิ้น ตัวเลขที่ได้จึงเป็นตัวเลขทดสอบ
- **ภาพดาวเทียมดึงจาก endpoint ที่ Google ไม่ได้เปิดเป็นสาธารณะ**
  (`mt1.google.com`) ใช้ทำต้นแบบได้ แต่ถ้าจะใช้งานจริงต้องเปลี่ยนไปใช้ API
  ที่มี key ตามเงื่อนไขของ Google
- **การคำนวณกำลังผลิตใช้ค่าคงที่** — 180 Wp/ตร.ม., แดดเต็ม 4.2 ชม./วัน และ
  performance ratio 0.75 สมมติหลังคาแบนทั้งหมด ยังไม่ได้คิดมุมเอียง ทิศหัน
  หรือเงาบัง สูตรถูกตรึงไว้ด้วยเทสต์ใน `test_calculations.py` แล้ว ถ้าจะแก้ค่า
  ต้องแก้ค่าที่คาดหวังในเทสต์ด้วย
- **ยังไม่มี role** ทุกคนเห็นเฉพาะงานของตัวเอง ไม่มีผู้ดูแลที่เห็นได้ทั้งหมด
- **รหัสผ่านใน `docker-compose.yml` เป็นค่าสำหรับพัฒนาเท่านั้น** ต้องเปลี่ยนก่อน
  นำขึ้นเซิร์ฟเวอร์จริง และต้องตั้ง `SOLAR_SECRET_KEY` ด้วย
