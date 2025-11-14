# dags/logic/db_utils.py
# CHUNG: Chứa các hàm tiện ích về DB

import os
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

@contextmanager
def get_db_engine():
    """Hàm helper để tạo và đóng engine kết nối DB an toàn."""
    db_user = os.getenv("POSTGRES_USER", "careerbi_user")
    db_pass = os.getenv("POSTGRES_PASSWORD", "careerbi_password")
    db_name = os.getenv("POSTGRES_DB", "careerbi")
    db_host = "postgres"
    
    conn_string = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:5432/{db_name}"
    engine = None
    try:
        engine = create_engine(conn_string)
        yield engine
    finally:
        if engine:
            engine.dispose()