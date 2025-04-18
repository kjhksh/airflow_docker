import logging
import pandas as pd
import pendulum # datetime 대신 pendulum 사용 권장

from airflow.decorators import dag, task
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- 상수 정의 ---
# TODO: Airflow UI에서 실제 설정된 Connection ID로 변경하세요.
SALESFORCE_CONN_ID = "Salseforce_Conn"
S3_CONN_ID = "locknlock_s3" # S3/MinIO Connection ID (Airflow UI에 생성 필요)

S3_BUCKET_NAME = "sfquickstarts"
S3_KEY_PREFIX = "salesforce_data"
SALESFORCE_QUERY = "SELECT Id, Name FROM Account" # 필요시 필드 추가
JSON_ORIENT = "records"
JSON_LINES = True
FORCE_ASCII = False
coerceToTimestamp = False
recordTimeAdded = False



# --- TaskFlow API 사용 ---
@dag(
    dag_id='dag_salesforce_to_s3_python_improved', # DAG ID 변경 또는 기존 ID 유지
    start_date=pendulum.datetime(2025, 3, 27, tz="UTC"), # 시작 날짜 및 타임존 설정
    schedule=None, # 필요시 스케줄 간격 설정 (예: '@daily')
    catchup=False,
    tags=["prd", "migration", "salesforce", "s3", "python"],
    default_args={
        'owner': 'jonghyun kim',
        'retries': 1,
        'retry_delay': pendulum.duration(minutes=2),
        'email_on_failure': False, # 필요시 알림 설정
        'email_on_retry': False,
    },
    doc_md=f"""
    ### Salesforce to S3 DAG (Python Function - Improved)

    Salesforce에서 Account 데이터를 조회하여 S3 버킷에 JSON 파일로 저장합니다.
    - 로컬 임시 파일을 생성하지 않고 메모리에서 직접 S3로 스트리밍합니다.
    - Salesforce API의 `nextRecordsUrl`을 사용하여 페이지네이션합니다.
    - **Salesforce Connection**: `{SALESFORCE_CONN_ID}` (Airflow UI 확인 필요)
    - **S3 Connection**: `{S3_CONN_ID}` (Airflow UI 확인 및 생성 필요)
    - **S3 Bucket**: `{S3_BUCKET_NAME}`
    - **S3 Key Pattern**: `{S3_KEY_PREFIX}/account_data_{{{{ ds }}}}_part_*.json.gz`
    """
)
def salesforce_to_s3_python_dag():

    @task
    def export_salesforce_to_s3_task(ds=None, **context):
        """
        Salesforce 데이터를 조회하여 S3에 청크 단위로 업로드합니다.
        Airflow Context에서 'ds' (논리적 날짜)를 받아 파일명에 사용합니다.
        """
        salesforce_hook = SalesforceHook(salesforce_conn_id=SALESFORCE_CONN_ID)
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        logging.info("Salesforce 및 S3 Hook 생성 완료.")

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
            # MinIO 등 endpoint_url이 필수인 경우, 여기서 오류를 발생시키거나 기본값을 설정해야 할 수 있습니다.
            # raise ValueError(f"S3 Connection '{S3_CONN_ID}'에 endpoint_url 설정이 필요합니다.") from e

        next_records_url = None
        chunk_index = 0
        total_records_processed = 0

        try:
            while True:
                if next_records_url:
                    # 다음 페이지 조회 (hook.run은 REST API 엔드포인트를 직접 호출)
                    # SalesforceHook._get_auth_header()는 private 메서드이므로 직접 사용 지양.
                    # 대신 hook.get_conn()을 통해 session 객체를 얻어 사용하는 것이 더 안정적일 수 있음.
                    # 여기서는 간단하게 hook.run 사용 예시를 유지.
                    # 참고: hook.run은 JSON 응답을 자동으로 파싱해줌.
                    logging.info(f"Salesforce 다음 페이지 조회 시도 (URL: {next_records_url})")
                    response = salesforce_hook.run(endpoint=next_records_url)

                else:
                    # 첫 페이지 조회
                    logging.info(f"Salesforce 첫 페이지 조회 시도 (Query: {SALESFORCE_QUERY})")
                    response = salesforce_hook.make_query(SALESFORCE_QUERY)
                    # records = response.get("records")
                    records = response["records"]
                
                if not records:
                    logging.info("더 이상 처리할 Salesforce 레코드가 없습니다.")
                    break

                # chunk_df = pd.DataFrame(records)
                chunk_df = salesforce_hook.object_to_df(
                    query_results=records,
                    coerce_to_timestamp=coerceToTimestamp,
                    record_time_added=recordTimeAdded,
                )
                
                num_records_in_chunk = len(chunk_df)
                total_records_processed += num_records_in_chunk
                logging.info(f"청크 {chunk_index}: {num_records_in_chunk}개 레코드 처리 (총 {total_records_processed}개)")

                # S3 파일 경로 설정 (논리적 날짜와 청크 인덱스 사용)
                # ds가 None일 경우 대비 (예: 수동 실행 시)
                logical_date_str = ds if ds else pendulum.now("UTC").to_date_string()
                s3_key = f"{S3_KEY_PREFIX}/account_data_{logical_date_str}_part_{chunk_index:04d}.json.gz"
                s3_uri = f"s3://{S3_BUCKET_NAME}/{s3_key}"

                # 데이터 저장 (압축 포함)
                logging.info(f"청크 {chunk_index} 데이터를 S3에 저장 시작: {s3_uri}")
                try:
                    chunk_df.to_json(
                        s3_uri,
                        orient=JSON_ORIENT,
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

                # 다음 페이지 URL 확인
                if response.get("done"):
                    logging.info("Salesforce 데이터 처리 완료 (마지막 페이지).")
                    break
                next_records_url = response.get("nextRecordsUrl")
                if not next_records_url:
                     logging.warning("Salesforce 응답에 'nextRecordsUrl'이 없습니다. 루프를 종료합니다.")
                     break
                 # nextRecordsUrl은 상대 경로일 수 있으므로, hook의 instance_url과 조합 필요
                if not next_records_url.startswith('http'):
                    try:
                        instance_url = salesforce_hook.get_conn().instance_url.rstrip('/')
                        next_records_url = f"{instance_url}{next_records_url}"
                    except Exception as conn_err:
                         logging.error(f"Salesforce instance URL 가져오기 실패: {conn_err}. 상대 경로 사용 불가.")
                         raise # 진행 불가 오류

        except Exception as e:
            logging.error(f"Salesforce 데이터 처리 또는 S3 업로드 중 예기치 않은 오류 발생: {e}")
            raise # 오류를 다시 발생시켜 Airflow 태스크 실패 처리

        logging.info(f"총 {total_records_processed}개의 레코드를 {chunk_index}개의 청크 파일로 S3에 저장 완료.")
        return total_records_processed # 처리된 레코드 수 반환 (XCom으로 사용 가능)

    # 태스크 호출
    export_salesforce_to_s3_task()

# DAG 인스턴스 생성 (TaskFlow API 사용 시 필수)
salesforce_to_s3_python_dag()
