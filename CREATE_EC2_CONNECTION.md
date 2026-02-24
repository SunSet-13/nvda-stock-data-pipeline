# Tạo EC2 PostgreSQL Connection

## Thông tin Connection:
- **Host**: 47.129.136.168
- **Database**: postgres
- **User**: postgres
- **Password**: postgres
- **Port**: 5432

## Cách 1: Tạo qua Airflow UI (Khuyến nghị)

1. Truy cập Airflow UI: http://localhost:8080
2. Vào **Admin** → **Connections**
3. Click nút **+** (Add a new record)
4. Điền thông tin:
   ```
   Connection Id: postgres_ec2
   Connection Type: Postgres
   Host: 47.129.136.168
   Schema: postgres
   Login: postgres
   Password: postgres
   Port: 5432
   ```
5. Click **Save**

## Cách 2: Tạo qua CLI (trong container)

```bash
docker exec -it <airflow-webserver-container> bash

airflow connections add 'postgres_ec2' \
    --conn-type 'postgres' \
    --conn-host '47.129.136.168' \
    --conn-schema 'postgres' \
    --conn-login 'postgres' \
    --conn-password 'postgres' \
    --conn-port '5432'
```

## Cách 3: Chạy script Python

```bash
docker exec -it <airflow-webserver-container> python /opt/airflow/dags/setup_ec2_connection.py
```

## Test Connection

Sau khi tạo xong, test connection bằng cách:

1. Vào Airflow UI → Admin → Connections
2. Tìm connection `postgres_ec2`
3. Click vào Test button
4. Hoặc chạy DAG `postgres_to_ec2_transfer` để kiểm tra
