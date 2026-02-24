from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd


@dag(
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['postgres', 'ec2', 'sync'],
    description='Sync stock market data from local PostgreSQL to EC2 PostgreSQL daily'
)
def postgres_to_ec2_transfer():
    
    @task
    def extract_from_local_postgres(**context):
        """
        Lấy TẤT CẢ dữ liệu từ PostgreSQL Docker
        """
        # Kết nối tới PostgreSQL local (Docker)
        postgres_hook = PostgresHook(postgres_conn_id='postgres')
        
        # Query để lấy TẤT CẢ dữ liệu
        sql = """
            SELECT * FROM stock_market 
            ORDER BY date DESC
        """
        
        # Lấy dữ liệu
        connection = postgres_hook.get_conn()
        df = pd.read_sql(sql, connection)
        connection.close()
        
        # Chuyển DataFrame thành list of dicts để pass qua XCom
        records = df.to_dict('records')
        
        print(f"Extracted {len(records)} records from local PostgreSQL")
        return records
    
    @task
    def load_to_ec2_postgres(records: list):
        """
        Đẩy dữ liệu lên PostgreSQL EC2
        """
        if not records:
            print("No records to load")
            return
        
        # Kết nối tới PostgreSQL EC2
        postgres_hook = PostgresHook(postgres_conn_id='postgres_ec2')
        
        # Tạo bảng nếu chưa có (lần đầu tiên hoặc dùng init DAG)
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS stock_market (
                timestamp BIGINT,
                close DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                open DOUBLE PRECISION,
                volume BIGINT,
                date TEXT,
                PRIMARY KEY (date)
            )
        """
        
        # Insert data với conflict handling
        insert_sql = """
            INSERT INTO stock_market (timestamp, close, high, low, open, volume, date)
            VALUES (%(timestamp)s, %(close)s, %(high)s, %(low)s, %(open)s, %(volume)s, %(date)s)
            ON CONFLICT (date) 
            DO UPDATE SET 
                timestamp = EXCLUDED.timestamp,
                close = EXCLUDED.close,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                open = EXCLUDED.open,
                volume = EXCLUDED.volume
        """
        
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        
        try:
            # Tạo bảng nếu chưa có
            cursor.execute(create_table_sql)
            connection.commit()
            print("Table ready")
            
            # Insert records
            cursor.executemany(insert_sql, records)
            connection.commit()
            print(f"Successfully loaded {len(records)} records to EC2 PostgreSQL")
            
        except Exception as e:
            connection.rollback()
            print(f"Error loading data: {e}")
            raise
        finally:
            cursor.close()
            connection.close()
    
    @task
    def verify_data_transfer():
        """
        Kiểm tra dữ liệu đã được transfer thành công
        """
        # Đếm tổng số records trong local DB
        local_hook = PostgresHook(postgres_conn_id='postgres')
        local_count = local_hook.get_first(
            "SELECT COUNT(*) FROM stock_market"
        )[0]
        
        # Đếm tổng số records trong EC2 DB
        ec2_hook = PostgresHook(postgres_conn_id='postgres_ec2')
        ec2_count = ec2_hook.get_first(
            "SELECT COUNT(*) FROM stock_market"
        )[0]
        
        print(f"Local PostgreSQL: {local_count} records")
        print(f"EC2 PostgreSQL: {ec2_count} records")
        
        if ec2_count >= local_count:
            print("✓ Data transfer verified successfully!")
        else:
            print("⚠ Warning: EC2 has fewer records than local")
        
        return {
            'local_count': local_count,
            'ec2_count': ec2_count,
            'status': 'success' if ec2_count >= local_count else 'incomplete'
        }
    
    # Định nghĩa flow
    records = extract_from_local_postgres()
    load_task = load_to_ec2_postgres(records)
    verify_data_transfer() << load_task


postgres_to_ec2_transfer()
