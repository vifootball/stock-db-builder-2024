import os
from config import *
import psycopg2
from postgresql_queries import *

# create database

def get_db_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    return conn


def __get_db_connection():  # 초기 연결해서 CREATE DATABASE 하기 위한 용도
    conn = psycopg2.connect(**DB_CONFIG_INITIAL)
    return conn


def get_db_cursor():
    conn = psycopg2.connect(
        host="localhost",           # 호스트 주소
        port="5432",                # 포트 번호, 기본값은 5432
        database="aaaa",            # 연결할 데이터베이스 이름
        user="aaaa",                # 데이터베이스 사용자 이름
        password="aaaa"             # 해당 사용자의 비밀번호
    )
    print("DB Connected.")
    cur = conn.cursor()
    print("DB Cursor Returned.")
    return cur


def create_database(database_name):
    connection = __get_db_connection()
    print(f"Database Connected for create_database({database_name})")

    # autocommit 설정
    connection.autocommit = True

    try:
        with connection.cursor() as cursor:
            # 데이터베이스 존재 여부 확인
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = %s;", (database_name,))
            exists = cursor.fetchone()

            # 데이터베이스가 없으면 생성
            if not exists:
                cursor.execute(f"CREATE DATABASE {database_name};")
                print(f"Database '{database_name}' created successfully.")
            else:
                print(f"Database '{database_name}' already exists.")

    except psycopg2.Error as e:
        print(f"An error occurred: {e}")

    finally:
        # 연결 닫기
        connection.close()


def copy_csv_files_to_db(csv_dir_path, table_name, create_table_query,):
    connection = get_db_connection()
    # table_name = "DW_L1_HISTORY"
    # csv_dir_path = os.path.join("downloads", "history", "chunks")

    try:
        with connection: # connection 블록 내에서 모든 쿼리가 실행되므로, 모든 명령어가 성공하면 connection.commit()이 자동으로 호출
            with connection.cursor() as cursor:

                # 기존 테이블 삭제
                print("Drop Table if exists: Started")
                cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                print("Drop Table if exists: Ended")
                print()

                # 테이블 생성 - CSV 파일에 맞게 컬럼 타입 설정
                print(f'Create Table "{table_name}": Started')
                cursor.execute(create_table_query)
                print(f'Create Table "{table_name}": Ended')
                print()
                
                # 폴더 내의 모든 CSV 파일을 가져오기
                filenames = sorted([f for f in os.listdir(csv_dir_path) if f.endswith('.csv')])
                num_total_files = len(filenames)  # 총 파일 개수 계산

                for index, filename in enumerate(filenames, start=1):
                    file_path = os.path.join(csv_dir_path, filename)
                    print(f"[{index}/{num_total_files}] Upload data from {file_path} to the table '{table_name}': Started")

                    # COPY 명령어를 사용하여 CSV 파일을 통째로 테이블에 업로드
                    with open(file_path, 'r') as f:
                        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','", f)

                    print(f"[{index}/{num_total_files}] Upload data from {file_path} to the table '{table_name}': Ended")

                    # 업로드된 행 수를 데이터베이스에서 직접 확인
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                    row_count = cursor.fetchone()[0]
                    print(f"Current total rows in '{table_name}': {row_count}")
                    print()

    except Exception as e:
        # 예외 발생 시 롤백
        print(f"An error occurred: {e}")
        connection.rollback()

    finally:
        # 연결 닫기
        connection.close()


if __name__ == "__main__":
    create_database(database_name="stock_db_2024")
    print()

    # COPY FILES TO DB: DW_L1_HISTORY
    copy_csv_files_to_db(
        csv_dir_path = os.path.join("downloads", "history", "chunks"),
        table_name = "DW_L1_HISTORY",
        create_table_query = CREATE_TABLE_DW_L1_HISTORY
    )
    