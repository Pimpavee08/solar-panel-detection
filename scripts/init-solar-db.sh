#!/bin/bash
# สร้าง database ของโปรเจกต์แยกจาก metadata ของ Airflow
# (สคริปต์นี้รันครั้งเดียวตอน Postgres container ถูกสร้างใหม่)
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-SQL
    CREATE DATABASE solar;
    GRANT ALL PRIVILEGES ON DATABASE solar TO $POSTGRES_USER;
SQL
