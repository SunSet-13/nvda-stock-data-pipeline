"""
Script để tạo connection EC2 PostgreSQL trong Airflow
Chạy script này một lần để tạo connection
"""
from airflow import settings
from airflow.models import Connection
from sqlalchemy.orm import Session

def create_ec2_postgres_connection():
    """
    Tạo connection cho EC2 PostgreSQL
    """
    session = Session(bind=settings.engine)
    
    # Xóa connection cũ nếu có
    existing_conn = session.query(Connection).filter(
        Connection.conn_id == 'postgres_ec2'
    ).first()
    
    if existing_conn:
        session.delete(existing_conn)
        session.commit()
        print("Đã xóa connection cũ")
    
    # Tạo connection mới
    new_conn = Connection(
        conn_id='postgres_ec2',
        conn_type='postgres',
        host='47.129.136.168',
        schema='postgres',  # database name
        login='postgres',
        password='postgres',
        port=5432,
        extra=None
    )
    
    session.add(new_conn)
    session.commit()
    session.close()
    
    print("✓ Đã tạo connection 'postgres_ec2' thành công!")
    print(f"  Host: 47.129.136.168")
    print(f"  Database: postgres")
    print(f"  User: postgres")
    print(f"  Port: 5432")

if __name__ == "__main__":
    create_ec2_postgres_connection()
