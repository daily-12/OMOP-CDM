import cx_Oracle
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd 
import datetime

def ORCL_connect():
    # 라이브러리 연결
    cx_Oracle.init_oracle_client(lib_dir=r"C:/Oracle/instantclient_19_6")

    con_ip = '' # 서버주소/service_name
    con_id = ''
    con_pw = ''

    # 연결에 필요한 기본 정보(유저, 비밀번호, 데이터베이스 서버 주소)
    connection = cx_Oracle.connect(con_id, con_pw, con_ip)

    return connection

def POSTGRE_connect():
    con_ip = ''
    con_db = ''
    con_user = ''
    con_pw = ''
    con_port = 
    con_option = '-c search_path='

    connection = psycopg2.connect(host=con_ip, dbname = con_db, user = con_user, password = con_pw, port = con_port, options = con_option)

    return connection

def provider():
    oracle_connection = ORCL_connect()
    oracle_cursor = oracle_connection.cursor()
    postgresql_connection = POSTGRE_connect()
    postgresql_cursor = postgresql_connection.cursor()

    # care_site_id 매핑을 위해 PostgreSQL에 있는 care_site테이블 조회
    postgresql_cursor.execute("""
                                select 
                                    care_site_id
                                    , care_site_source_value
                                from 
                                    cdmpv531.care_site ;
                                """)
    care_site = postgresql_cursor.fetchall()
    care_site_df = pd.DataFrame(care_site, columns= ["care_site_id", "care_site_source_value"])

    # DW Oracle에서 조회
    oracle_cursor.execute("""
                        select
                            EMPNO
                            , USERID
                            , USERNAME
                            , JIKJONGNM
                            , DEPTCODE
                        from 
                            CUHDW.DD_CSUSERMT
                        """)
    oracle_result = oracle_cursor.fetchall()
    # Oracle 조회 결과 데이터프레임에 저장
    dw_df = pd.DataFrame(oracle_result, columns = ["EMPNO", "USERID", "USERNAME", "JIKJONGNM", "DEPTCODE"] ) #["provider_id", "provider_name", "npi", "dea", "specialty_concept_id", "care_site_id", "year_of_birth", "gender_concept_id", "provider_source_value", "specialty_source_value", "specialty_source_concept_id", "gender_source_value", "gender_source_concept_id"])
    
    # Oracle, PostgreSQL 조회한 테이블 inner 조인
    provider_df = pd.merge(left = dw_df, right = care_site_df, how = 'inner', left_on='DEPTCODE', right_on='care_site_source_value')
    result_data = [] 
    provider_id = "nextval(\'cdmpv531.\"seq_provider_id\"\')"
    for idx, row in provider_df.iterrows():

        #provider_id = idx + 1
        provider_name = row["USERNAME"]
        npi = None
        dea = None
        specialty_concept_id = 0 #df["care_site_source_value"][idx]
        care_site_id = row['care_site_id']
        year_of_birth = None
        gender_concept_id = 0 
        provider_source_value = row["USERID"]
        specialty_source_value = row["JIKJONGNM"]
        specialty_source_concept_id = 0
        gender_source_value = None
        gender_source_concept_id = 0
        provider_source_value_group = row["EMPNO"]

        tmp = ( provider_id, provider_name, npi, dea, int(specialty_concept_id), int(care_site_id), year_of_birth, int(gender_concept_id), provider_source_value, specialty_source_value, int(specialty_source_concept_id), gender_source_value, int(gender_source_concept_id), provider_source_value_group)
        result_data.append(tmp)

    oracle_cursor.close()
    oracle_connection.close()

    sql = """
        insert into cdmpv531.provider
        (provider_id, provider_name, npi, dea, specialty_concept_id, care_site_id, year_of_birth, gender_concept_id, provider_source_value, specialty_source_value, specialty_source_concept_id, gender_source_value, gender_source_concept_id, provider_source_value_group)
        values %s;
    """

    execute_values(postgresql_cursor, sql, result_data)
    postgresql_connection.commit()

    postgresql_cursor.close()
    postgresql_connection.close()