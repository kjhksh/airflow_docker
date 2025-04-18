import logging
import json
import os
import tarfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner': 'jonghyun kim',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def fn_oralceExport():
      logging.info("# step 1: query data from oralceSql db and save into json file")

      # Oracle 데이터베이스 연결 설정

      hook = OracleHook(oracle_conn_id="oracle_223")
      conn = hook.get_conn()
      cursor = conn.cursor()

      # 데이터 쿼리 실행
      cursor.execute("SELECT * FROM SF114_ACTIONLOG.ACTIONLOG")
      rows = cursor.fetchall()

      # 컬럼 이름 가져오기
      columns = [desc[0] for desc in cursor.description]
      logging.info("# columns :: %s", columns)
      
      # JSON 파일로 저장
      data = [dict(zip(columns, row)) for row in rows]      
      
      json_file_path = "data/oracle/actionlog.json"
      #/ os.makedirs(os.path.dirname(json_file_path), exist_ok=True)  # 디렉토리 생성
      with open(f"{json_file_path}", 'w') as json_file:
            json.dump(data, json_file, default=str)  # default=str를 사용하여 datetime 객체를 문자열로 변환
    
      logging.info("Data saved to  %s", f"{json_file_path}")

def fn_mssqlExport():
      logging.info("# step 1: query data from msSql db and save into json file")
      
      # Oracle 데이터베이스 연결 설정
      hook = MsSqlHook(mssql_conn_id="mssql_220")
      conn = hook.get_conn()
      cursor = conn.cursor()
      
      # 데이터 쿼리 실행
      cursor.execute("select * from sf120_action_mssql.dbo.ACTIONLOG")
      rows = cursor.fetchall()

      # 컬럼 이름 가져오기
      columns = [desc[0] for desc in cursor.description]
      logging.info("# columns :: %s", columns)
      
      # JSON 파일로 저장
      data = [dict(zip(columns, row)) for row in rows]      
      
      json_file_path = "data/mssql/actionlog.json"
      #/ os.makedirs(os.path.dirname(json_file_path), exist_ok=True)  # 디렉토리 생성
      with open(f"{json_file_path}", 'w') as json_file:
            json.dump(data, json_file, default=str)  # default=str를 사용하여 datetime 객체를 문자열로 변환
    
      logging.info("Data saved to  %s", f"{json_file_path}")
      
def fn_postgresExport():
      logging.info("# step 1: query data from postgresql db and save into json file")
      
      hook = PostgresHook(postgres_conn_id="postgres_localhost")
      conn = hook.get_conn()
      cursor = conn.cursor()
      
      # 데이터 쿼리 실행
      cursor.execute("select * from orders")
      rows = cursor.fetchall()

      # 컬럼 이름 가져오기
      columns = [desc[0] for desc in cursor.description]
      logging.info("# columns :: %s", columns)
      
      # JSON 파일로 저장
      data = [dict(zip(columns, row)) for row in rows]      
      
      json_file_path = "data/postgres/orders.json"
      #/ os.makedirs(os.path.dirname(json_file_path), exist_ok=True)  # 디렉토리 생성
      with open(f"{json_file_path}", 'w') as json_file:
            json.dump(data, json_file, default=str)  # default=str를 사용하여 datetime 객체를 문자열로 변환
    
      logging.info("Data saved to  %s", f"{json_file_path}")

def compress_to_tar():
      logging.info("# step 2: compress json files into tar files")
      directory_path = "./data/"
      directories = ["oracle", "mssql", "postgres"]

      for directory in directories:
            compress_files_in_directory(directory_path+directory)

def compress_files_in_directory(directory):
      # 디렉토리가 존재하는지 확인
      if not os.path.exists(directory):
            print(f"Directory {directory} does not exist.")
            return
      
      # 디렉토리 내의 모든 파일을 가져오기
      files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
      
      for file_name in files:
            json_file_path = os.path.join(directory, file_name)
            tar_file_path = os.path.join(directory, f"{file_name}.tar.gz")
      
            # 각 파일을 개별적으로 tar 파일로 압축
            with tarfile.open(tar_file_path, "w:gz") as tar:
                  tar.add(json_file_path, arcname=os.path.basename(json_file_path))
      
                  print(f"Compressed {json_file_path} to {tar_file_path}")

def sqltarfile_to_s3():
      logging.info("# step 3: upload tar file into S3")
      
      directory_path = "./data/"
      directories = ["oracle", "mssql", "postgres"]
      
      s3_hook = S3Hook(aws_conn_id="minio_conn")
      
      for directory in directories:
            print(directory_path+directory)
            
            files = [f for f in os.listdir(directory_path + directory) if f.endswith(".tar.gz")]
            print(files)
            for file_name in files:
                  file_path = os.path.join(directory_path + directory, file_name)
                  s3_hook.load_file(
                        filename=file_path,
                        key=f"{directory}/{file_name}",
                        bucket_name="aws",
                        replace=True
                  )
                  logging.info(f"Uploaded {file_path} to S3") 
        
with DAG(
    dag_id='dag_with_rdbms_tar_hooks_v1',
    default_args=default_args,
    description='Oracle,mssql,postgres -> joson 파일 생성 -> tar로 압축  -> 아마존 s3에 업로드 (minio 버킷)	',
    #start_date=datetime(2021, 7, 29, 2),
    start_date=datetime(2025, 3, 13),
    schedule_interval='@daily'
)as dag:
  
  task1 = BashOperator(
        task_id='task1',
        bash_command="echo postgres world, this is the postgresExport_task!"
  )
  
  oralceExport_task = PythonOperator(
        task_id='oralceExport_task',
        python_callable=fn_oralceExport
  )
  
  mssqlExport_task = PythonOperator(
        task_id='mssqlExport_task',
        python_callable=fn_mssqlExport
  )
  
  postgreslExport_task = PythonOperator(
        task_id='postgreslExport_task',
        python_callable=fn_postgresExport
  )
  
  compress_task = PythonOperator(
        task_id='compress_task',
        python_callable=compress_to_tar
  )
  
  sqltarfile_to_s3_task = PythonOperator(
        task_id='sqltarfile_to_s3',
        python_callable=sqltarfile_to_s3
  )
  
  task1 >> [oralceExport_task, mssqlExport_task, postgreslExport_task] >> compress_task >> sqltarfile_to_s3_task




  



   