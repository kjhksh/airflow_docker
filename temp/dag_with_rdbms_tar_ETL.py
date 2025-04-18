import gc
import logging
import yaml
import re
# import json
import ast
# import numpy as np
import pandas as pd
import boto3
import pendulum
from datetime import datetime, timedelta
# from tempfile import NamedTemporaryFile
from pathlib import Path
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow import DAG
from airflow.operators.empty import EmptyOperator
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.oracle.hooks.oracle import OracleHook
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook


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
    
def execute_query(hook, job, endpointUrl, bucketName, awsAccessKeyId, awsSecretAccessKey, num_rows_per_file, fileFormat, orient, startDate, endDate):
    
    query = get_query_from_xml(sql_url, job['id'])
    # queryparams = ast.literal_eval("{" + job['queryparams'] + "}")
    queryparams = job['queryparams']
    query = query.replace("<conditions>", queryparams)
    query = query.replace("<startDate>", startDate)
    query = query.replace("<endDate>", endDate)
    # logging.info("query ::"+query)
    
    #extract
    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        data_tf = False
        
        file_path = job['targets']['file_path']
        log_path = job['targets']['log_path']
        account_name = job['targets']['account_name']
        table_name = job['targets']['table_name']
        
        file_path = change_exp_chars(file_path, ref_dict={'bucket_name':bucketName,
                                                            'account_name':account_name,
                                                            'table_name':table_name,
                                                            'file_format':fileFormat})
        
        square_exp_list = re.compile('\[([^]]+)').findall(file_path)
        
        #[숫자] 형태가 있으면 start_num과 step을 만든다.
        for square_exp in square_exp_list:
            if re.search('^[0-9]', square_exp):
                num = int(square_exp.split(',')[0])
                step = int(square_exp.split(',')[1])
        
        # 디렉토리 생성
        dir_path = Path(file_path).parent
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)
            logging.info(f"{dir_path} 디렉토리가 생성되었습니다.")
        
        # 로그 정보를 저장할 딕셔너리 초기화
        log_info_dict = []
        
        # for chunk_df in pd.read_sql(query, conn, params=queryparams, chunksize=num_rows_per_file):
        for chunk_df in pd.read_sql(query, conn, chunksize=num_rows_per_file):
            if len(chunk_df) > 0:
                data_tf = True
                for square_exp in square_exp_list:
                    if re.search(r'^[0-9]', square_exp):    
                        # final_file_path = file_path.replace('['+square_exp+']', str(num)).strip()  
                        final_file_path = file_path.replace('['+square_exp+']', str(num)).strip()  
                        
                        # logging.info("final_file_path ::"+final_file_path)
                         
                        chunk_df.to_json(final_file_path, orient=orient, date_format='iso', lines=True, force_ascii=False,
                                            storage_options={'endpoint_url':endpointUrl, 'key':awsAccessKeyId,'secret':awsSecretAccessKey})
                        
                        logging.info(f"{final_file_path} 파일이 적재 돼었습니다..")
                        
                        num = num + step     
                        
                        # 데이터 카운트 및 null 카운트 로그 추가
                        log_info = {
                            "file_path": final_file_path,
                            "row_count": len(chunk_df),
                            "null_count": chunk_df.isnull().sum().to_dict()
                        }
                        log_info_dict.append(log_info)
                       
        # 로그 정보를 하나의 JSON 파일로 저장
        log_path = change_exp_chars(log_path, ref_dict={'bucket_name':bucketName,
                                                            'account_name':account_name,
                                                            'table_name':table_name,
                                                            'file_format':fileFormat})
        
        log_path = log_path+"/"+(table_name + "_log.json")
        
        # log 데이터프레임 생성
        log_df = pd.DataFrame(log_info_dict)
        # 이스케이프된 슬래시를 일반 슬래시로 변경
        json_data = log_df.to_json(orient=orient, lines=True, force_ascii=False).replace('\\/', '/')
        
        # S3에 저장
        s3 = boto3.client('s3', endpoint_url=endpointUrl, aws_access_key_id=awsAccessKeyId, aws_secret_access_key=awsSecretAccessKey)
        s3.put_object(Bucket=bucketName, Key=log_path, Body=json_data)
    
        logging.info(f"{log_path} 로그가 생성되었습니다.")
            
        if data_tf:
            logging.info(" - load complete.")
        else:
            logging.info("- skip load job. (0 rows)")
        
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error("Error message : %s", e)
        # raise
 
def fn_oralceExport(jobSetting, jobList, startDate, endDate):
        
    logging.info("# step 1: query data from oralceSql db and save into json.gz file 생성 ")
    logging.info(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    
    oracleConnId        = jobSetting['sources']['name']
    endpointUrl         = jobSetting['targets']['endpoint_url']   
    bucketName          = jobSetting['targets']['bucket_name']   
    awsAccessKeyId      = jobSetting['targets']['aws_access_key_id']   
    awsSecretAccessKey  = jobSetting['targets']['aws_secret_access_key']   
    num_rows_per_file   = jobSetting['targets']['num_rows_per_file']
    fileFormat          = jobSetting['targets']['file_format']
    orient              = jobSetting['targets']['orient']
    
    hook = OracleHook(oracle_conn_id=oracleConnId)
    
    # for job in job_list:
    #     execute_query(hook, job, num_rows_per_file, file_format)
    
    def worker(job):
        execute_query(hook, job, endpointUrl, bucketName, awsAccessKeyId, awsSecretAccessKey, num_rows_per_file, fileFormat, orient, startDate, endDate)
    
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
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id=conf['name'],
    default_args=default_args,
    description = conf['description'],
    start_date = pendulum.datetime(2025, 4, 14, tz="Asia/Seoul"), # 서울 시간대 설정
    schedule_interval = "5 0 * * *", # 매일 0시 5분에 실행
    catchup = False,
    tags = conf['tags'],
) as dag:
    
    start_task = EmptyOperator(
        task_id='start_task'
    )
    
    jobList = conf['jobs']['job_list']
    jobSetting = conf['jobs']['settings']
    
    startDate = datetime.today()
    endDate = startDate - timedelta(days=6*30)

    oralceExport_task = PythonOperator(
        task_id='oralceExport_task',
        python_callable=fn_oralceExport,
        # op_args=[jobSetting, jobList]
        op_kwargs={'jobSetting': jobSetting, 
                   'jobList': jobList,
                   "startDate": startDate.strftime('%Y-%m-%d'),
                   "endDate": endDate.strftime('%Y-%m-%d')
                }
    )
    
    
    cleanSpace_task = PythonOperator(
        task_id='cleanSpace_task',
        python_callable=fn_clean_space
    )

start_task >> oralceExport_task  >>  cleanSpace_task
