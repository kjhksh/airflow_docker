

import pandas as pd

# 예시 데이터
data = {"column1": ["value1", "value2"],
        "column2": ["s3://path/to/file", "s3://another/path"]
}

# 데이터프레임 생성
df = pd.DataFrame(data)

# JSON 파일로 저장 (force_ascii=False 옵션 사용)
json_data = df.to_json("s3://sfquickstarts/tastybytes/raw_pos/order_detail/order_detail_log.json", orient=orient, lines=True, force_ascii=False,
                                            storage_options={'endpoint_url':endpointUrl, 'key':awsAccessKeyId,'secret':awsSecretAccessKey})

print(json_data)


log_df.to_json("s3://sfquickstarts/tastybytes/raw_pos/order_detail/order_detail_log.json", orient=orient, lines=True, force_ascii=False,
                                            storage_options={'endpoint_url':endpointUrl, 'key':awsAccessKeyId,'secret':awsSecretAccessKey})



'''
import pandas as pd
import boto3
from io import BytesIO

# 데이터프레임 생성
data = {'column1': [1, 2, 3], 'column2': [4, 5, 6]}
chunk_df = pd.DataFrame(data)

# 데이터프레임을 JSON 형식으로 변환
json_buffer = BytesIO()
chunk_df.to_json(json_buffer, orient="records", lines=True, force_ascii=False)
json_buffer.seek(0)

# MinIO 클라이언트 생성
s3 = boto3.client('s3', endpoint_url='http://192.168.10.29:9000', aws_access_key_id='NkCNrjizEMHIvFrGa3cl', aws_secret_access_key='etgLxl58LgPKSWELhDpvrrjdK4VrfoCBFXbK2dtR')

# MinIO에 파일 업로드
s3.put_object(Bucket='sfquickstarts', Key='tastybytes/raw_pos/order/order_1.json.gz', Body=json_buffer)

print("파일이 MinIO에 성공적으로 업로드되었습니다!")

'''

'''
import pandas as pd
import boto3
from io import BytesIO

# 데이터프레임 생성
data = {'column1': [1, 2, 3], 'column2': [4, 5, 6]}
chunk_df = pd.DataFrame(data)

# 데이터프레임을 JSON 형식으로 변환
json_buffer = BytesIO()
chunk_df.to_json(json_buffer, orient="records", lines=True, force_ascii=False)
json_buffer.seek(0)

# MinIO 클라이언트 생성
s3 = boto3.client('s3', endpoint_url='http://192.168.10.29:9000', aws_access_key_id='NkCNrjizEMHIvFrGa3cl', aws_secret_access_key='etgLxl58LgPKSWELhDpvrrjdK4VrfoCBFXbK2dtR')

# MinIO에 파일 업로드
s3.put_object(Bucket='sfquickstarts', Key='tastybytes/raw_pos/order/order_1.json.gz', Body=json_buffer)

print("파일이 MinIO에 성공적으로 업로드되었습니다!")

'''





'''
import pandas as pd
import s3fs

# 데이터프레임 생성
data = {'column1': [1, 2, 3], 'column2': [4, 5, 6]}
chunk_df = pd.DataFrame(data)

print(chunk_df)

# S3 파일 경로 설정
# final_file_path = 's3://sfquickstarts/tastybytes/raw_pos/order/order_1.json.gz'
final_file_path = 'tastybytes/raw_pos/order/order_1.json.gz'


# S3 파일 시스템 객체 생성
fs = s3fs.S3FileSystem(key='NkCNrjizEMHIvFrGa3cl', secret='etgLxl58LgPKSWELhDpvrrjdK4VrfoCBFXbK2dtR')



# 데이터프레임을 JSON 형식으로 S3에 저장
chunk_df.to_json(final_file_path, orient="records", lines=True, force_ascii=False,
                    storage_options={'key': 'NkCNrjizEMHIvFrGa3cl', 'secret': 'etgLxl58LgPKSWELhDpvrrjdK4VrfoCBFXbK2dtR'})


print("파일이 S3에 성공적으로 업로드되었습니다!")

'''


'''
import oracledb
from airflow.providers.oracle.hooks.oracle import OracleHook
import ast

queryparams = "'id':'admin'"
queryparams2 = {}

# 문자열을 딕셔너리로 변환
queryparams_dict = ast.literal_eval("{" + queryparams + "}")

# queryparams2에 추가
queryparams2.update(queryparams_dict)

print(queryparams2)


print({{ ds }})
'''

'''
try:
    connection = oracledb.connect(
        user="system",
        password="cspi00",
        dsn="192.168.10.220/ORCL",
        port="1521"
    )
    print("OracleDB 패키지가 성공적으로 설치되었습니다!")
except oracledb.Error as e:
    print(f"오류 발생: {e}") 
'''    
    