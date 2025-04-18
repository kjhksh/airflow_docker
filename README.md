/* docker build */
docker build --tag extending_airflow:latest .

/* docker-compose.yaml 다운로드 */
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml'

/* 환경 초기화 */
mkdir -p ./dags ./logs ./plugins ./config
mkdir -p ./data ./data/oracle ./data/mssql ./data/postgres
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env



/* 데이터베이스 마이그레이션, 사용자 계정 생성 */
docker compose up airflow-init

<!-- /* 폴더 환경 파일 생성 */
mkdir -p ./dags ./logs ./plugins ./config
mkdir -p ./data ./data/oracle ./data/mssql ./data/postgres
# echo -e "AIRFLOW_UID=$(id -u)" > .env -->


/* 에어플로우 시작*/
docker-compose up -d


/* 에어플로우 Cleaning up*/
docker compose down --volumes --rmi all

/* 참고URL */
https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html


/* 참고URL */

/* S3 대용 : moino server docker  */

docker run -dt \
  -p 9000:9000	\
  -p 9001:9001	\
  -e "MINIO_ROOT_USER=admin"	\
  -e "MINIO_ROOT_PASSWORD=cspi00P@ssWord"	\
  --name "minio_local"	\
  quay.io/minio/minio server /data --console-address ":9001"

---
compose

version: '3.8'

services:
  minio:
    image: quay.io/minio/minio
    container_name: minio_local
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=cspi00P@ssWord
    volumes:
      - ./data:/data
----

version: '3.8'

services:
  minio:
    image: quay.io/minio/minio
    container_name: minio_local
    command: server ${MINIO_VOLUMES} ${MINIO_OPTS}
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=cspi00P@ssWord
      - MINIO_VOLUMES=/mnt/data
      - MINIO_OPTS=--console-address :9001
    volumes:
      - /mnt/data:/mnt/data

/* 참고 debug */

airflow providers list

pip list | grep simple_salesforce


airflow dags trigger -d <dag_id>
airflow dags list
airflow tasks list locknlock_s3_2025_mig

airflow tasks run locknlock_s3_2025_mig oralceExport_task run_id=manual__2025-03-27T01:54:27.987756+00:00

DAG run
airflow dags trigger <dag_id>

DAG 상태 확인
airflow dags list-runs -d <dag_id>

특정 실행 날짜 지정
airflow dags trigger <dag_id> -e <execution_date>

테스크만 다시 실행
airflow tasks run my_dag my_task <execution_date>

재실행 옵션 사용: 에러가 발생한 테스크만 선택적으로 재실행
airflow tasks run my_dag my_task <execution_date> --ignore-all-dependencies

에러 테스크 확인
airflow tasks list my_dag

----------------
airflow dags trigger locknlock_s3_2025_mig

airflow dags list-runs -d locknlock_s3_2025_mig

airflow tasks run locknlock_s3_2025_mig_mem oralceExport_task 2025-04-14T12:20:41+05:00 --ignore-all-dependencies


airflow tasks run dag_salesforce_test salesforce_export_task 2025-04-16T04:37:36.594444+00:00 --ignore-all-dependencies

airflow tasks run dag_oracle_to_s3_python_improved oralceExport_task 2025-04-17T08:05:19.335940+00:00 --ignore-all-dependencies


