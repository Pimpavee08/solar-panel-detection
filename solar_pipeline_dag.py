from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# นำเข้าฟังก์ชันจากไฟล์ที่คุณเขียนไว้แล้ว
from data_retrieval import download_satellite_image
from post_processing import process_shapefile_to_geojson

# กำหนดค่าเริ่มต้นสำหรับ DAG
default_args = {
    "owner": "solar_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,  # ถ้ารันล้มเหลว ให้ลองใหม่ 1 ครั้ง
    "retry_delay": timedelta(minutes=2),
}

TOML_TEMPLATE = """[inference]
    inference_path = '{inference_path}'
    job_name = '{job_name}'

    input_folder = 'input'
    output_folder = 'output'
    tile_folder = 'input_tile'
    tile_size   = 512
    
    mlflow_registry = "solar-googleearth"
    mlflow_alias    = "current"
    
    batch_size  = 50
    mask_folder = 'mask_tile'
    mask_save_raw = true
    mask_raw_folder = 'mask_tile_raw'
    decide_threshold = 0.5

    redo = true
    n_thread    = -1

    simplify_tolerance = 0.4
    minimum_area = 1
    average_panel_size = 2.541

    [[inference.panel]]
       size = "xs"
       lower_bound = 0
       upper_bound = 22
       correct = -7.71
    [[inference.panel]]
       size = "s"
       lower_bound = 22
       upper_bound = 30
       correct = -5.48
    [[inference.panel]]
       size = "m"
       lower_bound = 30
       upper_bound = 45
       correct = -4.80
    [[inference.panel]]
       size = "l"
        lower_bound = 45
        upper_bound = inf
        correct = 0.79
"""


# ฟังก์ชันสำหรับ Task ที่ 1: ดาวน์โหลดภาพถ่ายดาวเทียม
def task_download_imagery(**context):
  # ดึงพารามิเตอร์ที่หน้าเว็บส่งเข้ามาผ่าน dag_run.conf
  conf = context["dag_run"].conf or {}
  job_name = conf.get("job_name", "job_default")
  min_lat = float(conf.get("min_lat", 14.0700))
  min_lon = float(conf.get("min_lon", 100.6020))
  max_lat = float(conf.get("max_lat", 14.0740))
  max_lon = float(conf.get("max_lon", 100.6070))
  zoom = int(conf.get("zoom", 18))

  base_dir = "data/inference"
  input_dir = os.path.join(base_dir, job_name, "input")
  os.makedirs(input_dir, exist_ok=True)

  tif_path = os.path.join(input_dir, "satellite.tif")
  download_satellite_image(
      min_lat, min_lon, max_lat, max_lon, zoom=zoom, output_tif_path=tif_path
  )
  print(f"[Airflow] Downloaded imagery to {tif_path}")


# ฟังก์ชันสำหรับ Task ที่ 2: สร้างไฟล์ .toml แบบไดนามิก
def task_generate_config(**context):
  conf = context["dag_run"].conf or {}
  job_name = conf.get("job_name", "job_default")
  base_dir = "data/inference"

  job_dir = os.path.join(base_dir, job_name)
  output_dir = os.path.join(job_dir, "output")
  os.makedirs(output_dir, exist_ok=True)

  config_content = TOML_TEMPLATE.format(
      inference_path=base_dir.replace("\\", "/") + "/", job_name=job_name
  )
  config_path = os.path.join(job_dir, f"{job_name}.toml")

  with open(config_path, "w", encoding="utf-8") as f:
    f.write(config_content)
  print(f"[Airflow] Generated config at {config_path}")


# ฟังก์ชันสำหรับ Task ที่ 4: Post-processing แปลงผลลัพธ์เป็น GeoJSON
def task_post_process(**context):
  conf = context["dag_run"].conf or {}
  job_name = conf.get("job_name", "job_default")
  base_dir = "data/inference"

  job_dir = os.path.join(base_dir, job_name)
  output_dir = os.path.join(job_dir, "output")
  geojson_path = os.path.join(job_dir, "result.geojson")

  process_shapefile_to_geojson(
      output_dir, output_geojson_path=geojson_path, job_name=job_name
  )
  print(f"[Airflow] Generated final GeoJSON at {geojson_path}")


# ประกาศ DAG หลัก
with DAG(
    dag_id="solar_panel_detection_pipeline",
    default_args=default_args,
    description="Automated pipeline for satellite retrieval, AI inference, and GeoJSON export",
    schedule_interval=None,  # ไม่ตั้งเวลารันอัตโนมัติ แต่รอรับ Trigger จากหน้าเว็บ
    catchup=False,
    tags=["solar", "geospatial", "ai"],
) as dag:

  # -------------------------------------------------------------
  # Task 1: ดาวน์โหลดภาพถ่ายดาวเทียม (PythonOperator)
  # -------------------------------------------------------------
  download_task = PythonOperator(
      task_id="download_satellite_imagery",
      python_callable=task_download_imagery,
      provide_context=True,
  )

  # -------------------------------------------------------------
  # Task 2: สร้างไฟล์ TOML Config (PythonOperator)
  # -------------------------------------------------------------
  generate_config_task = PythonOperator(
      task_id="generate_toml_config",
      python_callable=task_generate_config,
      provide_context=True,
  )

  # -------------------------------------------------------------
  # Task 3: รันคำสั่ง run.sh ผ่าน BashOperator (หัวข้อที่คุณถาม)
  # -------------------------------------------------------------
  # ใช้ Jinja Template ของ Airflow ดึงชื่อ job_name จากคำสั่ง trigger มาใส่ในพาธ
  run_model_task = BashOperator(
      task_id="execute_run_sh_model",
      bash_command="""
        JOB_NAME="{{ dag_run.conf.get('job_name', 'job_default') }}"
        CONFIG_PATH="data/inference/${JOB_NAME}/${JOB_NAME}.toml"
        
        echo "Executing model with config: ${CONFIG_PATH}"
        
        # ตรวจสอบว่ามี run.sh บนเซิร์ฟเวอร์จริงหรือไม่
        if [ -f "./run.sh" ]; then
            ./run.sh "5 -f ${CONFIG_PATH}"
        else
            echo "run.sh not found, falling back to mock_model.py"
            python mock_model.py --config "${CONFIG_PATH}"
        fi
        """,
  )

  # -------------------------------------------------------------
  # Task 4: แปลง Shapefile เป็น GeoJSON และคำนวณสถิติ (PythonOperator)
  # -------------------------------------------------------------
  post_process_task = PythonOperator(
      task_id="post_processing_geojson",
      python_callable=task_post_process,
      provide_context=True,
  )

  # -------------------------------------------------------------
  # กำหนดลำดับการทำงาน (Pipeline Flow Dependency)
  # -------------------------------------------------------------
  download_task >> generate_config_task >> run_model_task >> post_process_task
