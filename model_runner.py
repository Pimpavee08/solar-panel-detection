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


def execute_model_inference(job_name: str, base_dir: str = "data/inference"):
  """สร้างไฟล์ config .toml ตามสเปกของอาจารย์ และสั่งรันโมเดล Inference"""
  job_dir = os.path.join(base_dir, job_name)
  output_dir = os.path.join(job_dir, "output")
  os.makedirs(output_dir, exist_ok=True)

  # 1. บันทึกไฟล์ .toml สำหรับ Job นี้
  config_content = TOML_TEMPLATE.format(
      inference_path=base_dir.replace("\\", "/") + "/", job_name=job_name
  )
  config_filename = f"{job_name}.toml"
  config_path = os.path.join(job_dir, config_filename)

  with open(config_path, "w", encoding="utf-8") as f:
    f.write(config_content)
  print(f"Generated TOML config at: {config_path}")

  # 2. ตรวจสอบว่าอยู่บนเซิร์ฟเวอร์จริง (มี run.sh) หรือกำลังทดสอบในเครื่อง (Windows)
  if os.path.exists("./run.sh"):
    # คำสั่งจริงบนเซิร์ฟเวอร์ตามสไลด์ของอาจารย์
    cmd = ["./run.sh", f"5 -f {config_path}"]
  else:
    # รันผ่าน mock_model.py สำหรับทดสอบบนเครื่อง Local
    cmd = ["python", "mock_model.py", "--config", config_path]

  print(f"Executing: {' '.join(cmd)}")
  process = subprocess.run(cmd, capture_output=True, text=True, shell=True)

  if process.returncode != 0:
    raise RuntimeError(f"Model execution failed: {process.stderr}")

  print("Model execution completed successfully.")
  return output_dir