from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['postgres', 'ec2', 'init', 'setup'],
    description='[RUN ONCE] Initialize stock_market table on EC2 PostgreSQL'
)
def postgres_ec2_init_table():
    
    @task
    def drop_and_create_table():
        """
        DROP bảng cũ và tạo bảng mới với cấu trúc đúng
        CHỈ CHẠY MỘT LẦN ĐẦU TIÊN
        """
        # Kết nối tới PostgreSQL EC2
        postgres_hook = PostgresHook(postgres_conn_id='postgres_ec2')
        
        # Drop bảng cũ
        drop_table_sql = "DROP TABLE IF EXISTS stock_market CASCADE"
        
        # Tạo bảng mới với cấu trúc đúng
        create_table_sql = """
            CREATE TABLE stock_market (
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
        
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        
        try:
            print("Dropping old table (if exists)...")
            cursor.execute(drop_table_sql)
            connection.commit()
            print("✓ Old table dropped")
            
            print("Creating new table...")
            cursor.execute(create_table_sql)
            connection.commit()
            print("✓ New table created with correct structure")
            
            # Kiểm tra cấu trúc bảng
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'stock_market'
                ORDER BY ordinal_position
            """)
            
            columns = cursor.fetchall()
            print("\nTable structure:")
            for col in columns:
                print(f"  - {col[0]}: {col[1]}")
            
            return "Table initialized successfully"
            
        except Exception as e:
            connection.rollback()
            print(f"Error: {e}")
            raise
        finally:
            cursor.close()
            connection.close()
    
    drop_and_create_table()


postgres_ec2_init_table()
