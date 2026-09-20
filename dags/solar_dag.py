"""จุดที่ Airflow มองหา DAG

DAGS_FOLDER ชี้มาที่โฟลเดอร์นี้โฟลเดอร์เดียว ไม่ใช่ root ของโปรเจกต์
เพราะถ้าชี้ที่ root Airflow จะไล่ import ทุกไฟล์ .py เพื่อค้นหา DAG
ซึ่งรวมถึง app.py และไฟล์เทสต์ที่ mock แพ็กเกจ airflow ไว้
"""

from solar_pipeline_dag import dag  # noqa: F401
