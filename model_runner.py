import os
import shutil
import subprocess

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


def generate_config(job_name: str, base_dir: str = "data/inference") -> str:
  """Step 1 (generating_config): สร้างไฟล์ .toml ของงานนี้ แล้วคืน path ของไฟล์

  job_name ที่ส่งเข้ามาคือ tid ของ Task จึงทำให้ทุก path ใน config
  ชี้ไปที่โฟลเดอร์เฉพาะของงานนั้น (data/inference/{tid}/...)
  """
  job_dir = os.path.join(base_dir, job_name)
  os.makedirs(os.path.join(job_dir, "output"), exist_ok=True)

  config_content = TOML_TEMPLATE.format(
      inference_path=base_dir.replace("\\", "/") + "/", job_name=job_name
  )
  config_path = os.path.join(job_dir, f"{job_name}.toml")

  with open(config_path, "w", encoding="utf-8") as f:
    f.write(config_content)
  print(f"Generated TOML config at: {config_path}")
  return config_path


def run_inference(job_name: str, base_dir: str = "data/inference") -> str:
  """Step 3 (run_inference): สั่งรันโมเดลด้วยไฟล์ config ที่สร้างไว้แล้ว

  บนเซิร์ฟเวอร์จริงจะเรียก run.sh ตามสเปกของอาจารย์
  ส่วนบนเครื่อง local จะ fallback ไปใช้ mock_model.py แทน
  """
  job_dir = os.path.join(base_dir, job_name)
  output_dir = os.path.join(job_dir, "output")
  config_path = os.path.join(job_dir, f"{job_name}.toml")

  if not os.path.exists(config_path):
    raise FileNotFoundError(f"ไม่พบไฟล์ config ที่ {config_path}")

  os.makedirs(output_dir, exist_ok=True)

  if os.path.exists("./run.sh"):
    cmd = ["./run.sh", f"5 -f {config_path}"]
  else:
    cmd = ["python", "mock_model.py", "--config", config_path]

  print(f"Executing: {' '.join(cmd)}")
  process = subprocess.run(cmd, capture_output=True, text=True, shell=True)

  if process.returncode != 0:
    raise RuntimeError(f"Model execution failed: {process.stderr.strip()}")

  print("Model execution completed successfully.")
  return output_dir


def execute_model_inference(job_name: str, base_dir: str = "data/inference"):
  """สร้าง config แล้วรันโมเดลต่อในครั้งเดียว (ใช้โดย main.py และ DAG เดิม)"""
  generate_config(job_name, base_dir=base_dir)
  return run_inference(job_name, base_dir=base_dir)
