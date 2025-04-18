import gc
import logging
import yaml
import re
import ast
import pandas as pd
import boto3
import pendulum
from datetime import datetime, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# config 파일 불러오기
conf_url = Path(__file__).parent / "resource/dag_with_rdbms_tar_ETL_config.yaml"
with open(conf_url, 'r') as f:
	conf = yaml.load(f, Loader=yaml.FullLoader)

logging.info("load config file  : %s ", conf_url)
logging.info(f"project name : {conf['name']}")
logging.info(f"project version : {conf['version']}")
logging.info(f"project description : {conf['description']}")
logging.info(f"project tags : {conf['tags']}")
logging.info(f"project start : {datetime.today().strftime('%Y-%m-%d %H:%M:%S')}")

# sql 파일 불러오기
sql_url = Path(__file__).parent / conf['jobs']['settings']['sources']['sql_path']
jobSetting = conf['jobs']['settings']
jobList = conf['jobs']['job_list']

# TODO: Airflow UI에서 실제 설정된 Connection ID로 변경하세요.
ORACLE_CONN_ID          = jobSetting['sources']['conn_id']
S3_CONN_ID              = jobSetting['targets']['conn_id']  # S3/MinIO Connection ID (Airflow UI에 생성 필요)
END_POINT_URL            = jobSetting['targets']['endpoint_url']   
S3_BUCKET_NAME          = jobSetting['targets']['bucket_name'] 
AWS_ACCESS_KEY_ID       = jobSetting['targets']['aws_access_key_id']   
AWS_SECRET_ACCESS_KEY   = jobSetting['targets']['aws_secret_access_key']   
NUM_ROWS_PER_FILE       = jobSetting['targets']['num_rows_per_file']
FILE_FORMAT             = jobSetting['targets']['file_format']
JSON_ORIENT             = jobSetting['targets']['orient']
JSON_LINES              = True
FORCE_ASCII             = False
COERCE_TO_TIMESTAMP     = False
RECORD_TIME_ADDED       = False
    
def fn_clean_space():
    logging.info("# step 2: Clean up resources and global variables")
    logging.info(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    
    try:
        global conf_url, conf, sql_url
        del conf_url, conf, sql_url

        # 초기화
        for obj in list(globals().keys()):
            # checking for built-in variables/functions
            if not obj.startswith('_'):
                # deleting the said obj, since a user-defined function
                del globals()[obj]
    except:
        gc.collect()


def change_exp_chars(text, ref_dict=None):
    exp_list = re.compile('\{([^}]+)').findall(text)
    for exp in exp_list:
        text = text.replace('{'+exp+'}', str(ref_dict[exp])).strip()
    return text

def get_query_from_xml(file_path, query_id):
    tree = ET.parse(file_path)
    root = tree.getroot()
    for query in root.findall('query'):
        if query.get('id') == query_id:
            return query.text.strip()
    return None

# def execute_query(hook, job, endpointUrl, bucketName, awsAccessKeyId, awsSecretAccessKey, num_rows_per_file, fileFormat, orient, startDate, endDate):
def execute_query(hook, job, startDate, endDate, ds=None, **context):
    """
    Oralce 데이터를 조회하여 S3에 청크 단위로 업로드합니다.
    Airflow Context에서 'ds' (논리적 날짜)를 받아 파일명에 사용합니다.
    """
    query = get_query_from_xml(sql_url, job['id'])
    queryparams = job['queryparams']
    query = query.replace("<conditions>", queryparams)
    query = query.replace("<startDate>", startDate)
    query = query.replace("<endDate>", endDate)
    logging.info("query ::"+query)
    
    oralce_hook = hook
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        
    logging.info("Oralce 및 S3 Hook 생성 완료.")
    
    # S3 저장 옵션 설정 (Hook에서 endpoint_url 등 가져오기)
    s3_storage_options = {}
    
    try:
        s3_conn = s3_hook.get_connection(S3_CONN_ID)
        extra_options = s3_conn.extra_dejson
            
        if extra_options.get('endpoint_url'):
            s3_storage_options['endpoint_url'] = extra_options['endpoint_url']
        # Pandas는 key/secret 또는 profile을 사용하므로, hook의 credential 직접 사용
        credentials = s3_hook.get_credentials()
        s3_storage_options['key'] = credentials.access_key
        s3_storage_options['secret'] = credentials.secret_key
        logging.info(f"S3 Storage Options 설정 완료 (Endpoint: {s3_storage_options.get('endpoint_url')})")
    except Exception as e:
        logging.warning(f"S3 Connection ('{S3_CONN_ID}')에서 추가 옵션(endpoint_url) 로드 중 오류: {e}. 기본 AWS 설정 사용 시도.")
        # S3, MinIO 등 endpoint_url이 필수인 경우, 여기서 오류를 발생시키거나 기본값을 설정해야 할 수 있습니다.
        # raise ValueError(f"S3 Connection '{S3_CONN_ID}'에 endpoint_url 설정이 필요합니다.") from e
    
    
    file_path = job['targets']['file_path']
    log_path = job['targets']['log_path']
    account_name = job['targets']['account_name']
    table_name = job['targets']['table_name']
    chunk_index = 0
        
    try:
        file_path = change_exp_chars(file_path, ref_dict={'bucket_name':S3_BUCKET_NAME,
                                        'account_name':account_name,
                                        'table_name':table_name})
       
        # logging.info(f"file_path : ({file_path})")
        # 로그 정보를 저장할 딕셔너리 초기화
        log_info_dict = []
      
        for chunk_df in hook.get_pandas_df_by_chunks(sql=query, chunksize=NUM_ROWS_PER_FILE):
            if len(chunk_df) > 0:
                logical_date_str = ds if ds else pendulum.now("UTC").to_date_string()
                s3_key = f"_{logical_date_str}_part_{chunk_index}.{FILE_FORMAT}"
                s3_uri = f"{file_path}{s3_key}"
                # logging.info(f"s3_uri : {s3_uri}")
                try:
                    chunk_df.to_json(
                        s3_uri,
                        orient=JSON_ORIENT,
                        date_format='iso',
                        lines=JSON_LINES,
                        force_ascii=FORCE_ASCII,
                        compression='gzip',
                        storage_options=s3_storage_options
                    )
                    logging.info(f"청크 {chunk_index} 데이터 S3 저장 완료: {s3_uri}")
                
                except Exception as s3_error:
                    logging.error(f"S3 저장 중 오류 발생 ({s3_uri}): {s3_error}")
                    logging.error(f"사용된 S3 Storage Options: {s3_storage_options}")
                    raise # 오류를 다시 발생시켜 Airflow 태스크 실패 처리
                
                chunk_index += 1    
                # logging.info(f"chunk_index : {chunk_index}")
                # 데이터 카운트 및 null 카운트 로그 추가
                log_info = {
                    "file_path": s3_uri,
                    "row_count": len(chunk_df),
                    "null_count": chunk_df.isnull().sum().to_dict()
                }
                log_info_dict.append(log_info)
                
        # 로그 정보를 하나의 JSON 파일로 저장
        log_path = change_exp_chars(log_path, ref_dict={'bucket_name':S3_BUCKET_NAME,
                                                            'account_name':account_name,
                                                            'table_name':table_name})
        log_path = f"{log_path}/{table_name}_log.json"
        # log 데이터프레임 생성
        log_df = pd.DataFrame(log_info_dict)
        
        # JSON 문자열 생성 (파일 경로 없이)
        log_json_string = log_df.to_json(
            orient=JSON_ORIENT,
            date_format='iso',
            lines=JSON_LINES,
            force_ascii=FORCE_ASCII
        )
        # 이스케이프된 슬래시 수정
        corrected_log_json_string = log_json_string.replace('\\/', '/')
        
        logging.info(f"corrected_log_json_string : {corrected_log_json_string}")
        
        # 수정된 문자열을 S3에 업로드
        s3_hook.load_string(
            string_data=corrected_log_json_string,
            key=log_path, # S3 내 파일 경로 (key)
            bucket_name=S3_BUCKET_NAME, # 버킷 이름 명시
            replace=True # 기존 파일이 있으면 덮어쓰기
        )
        
        logging.info(f"{log_path} 로그가 생성되었습니다.")
        
    except Exception as e:
            logging.error(f"Oracle 데이터 처리 또는 S3 업로드 중 예기치 않은 오류 발생: {e}")
            raise # 오류를 다시 발생시켜 Airflow 태스크 실패 처리    
    
def fn_oralceExport(startDate, endDate):
    logging.info("# step 1: query data from oralceSql db and save into json.gz file 생성 ")
    logging.info(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    
    hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
    
    def worker(job):
        execute_query(hook, job, startDate, endDate)
    
    # ThreadPoolExecutor를 사용하여 최대 4개의 쓰레드만 동시에 실행되도록 설정
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(worker, job) for job in jobList]
        
        for future in as_completed(futures):
            
            try:
                future.result()  # 각 쓰레드의 결과를 기다림
            except Exception as e:
                logging.error("Error message : %s", e)
    
default_args = {
        'owner': 'jonghyun kim',
        'retries': 1,
        'retry_delay': pendulum.duration(minutes=2),
        'email_on_failure': False, # 필요시 알림 설정
        'email_on_retry': False,
}

with DAG(
    dag_id=conf['name'], # DAG ID 변경 또는 기존 ID 유지: dag_oracle_to_s3_python_improved
    start_date = pendulum.datetime(2025, 4, 14, tz="Asia/Seoul"), # 서울 시간대 설정
    # schedule=None, # 필요시 스케줄 간격 설정 (예: '@daily')
    schedule_interval = "5 0 * * *", # 매일 0시 5분에 실행
    catchup=False,
    tags=conf['tags'],
    doc_md=f"""
        ### Oracle to S3 DAG (Python Function - Improved)

        Oracle에서 데이터를 조회하여 S3 버킷에 JSON 파일로 저장합니다.
        - 로컬 임시 파일을 생성하지 않고 메모리에서 직접 S3로 스트리밍합니다.
        - **Oracle Connection**: `{ORACLE_CONN_ID}` (Airflow UI 확인 필요)
        - **S3 Connection**: `{S3_CONN_ID}` (Airflow UI 확인 및 생성 필요)
        - **S3 Bucket**: `{S3_BUCKET_NAME}`
    """
    
) as dag:
    start_task = EmptyOperator(
        task_id='start_task'
    )
    
    startDate = datetime.today()
    endDate = startDate - timedelta(days=6*30)

    oralceExport_task = PythonOperator(
        task_id='oralceExport_task',
        python_callable=fn_oralceExport,
        op_kwargs={"startDate": startDate.strftime('%Y-%m-%d'),
                   "endDate": endDate.strftime('%Y-%m-%d')
                }
    )
    
    cleanSpace_task = PythonOperator(
        task_id='cleanSpace_task',
        python_callable=fn_clean_space
    )
    

start_task >> oralceExport_task  >>  cleanSpace_task

