FROM apache/airflow:2.10.5

# # 임시로 root 사용자로 전환
# USER root

# # 필요한 패키지 설치
# RUN apt-get update && apt-get install -y \
#     gcc 
# USER airflow

ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
