import cx_Oracle
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd 

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


def care_site():
    oracle_connection = ORCL_connect()
    oracle_cursor = oracle_connection.cursor()
    postgresql_connection = POSTGRE_connect()
    postgresql_cursor = postgresql_connection.cursor()

    # 오라클에서 실행
    oracle_cursor.execute("""
                        select
                            ROW_NUMBER() OVER(ORDER BY DEPTCODE DESC)   AS care_site_id
                            , DEPTLNM                                   AS care_site_name
                            , 0                                         AS place_of_service_concept_id
                            , NULL                                      AS location_id
                            , DEPTCODE                                  AS care_site_source_value
                            , subdept                                   AS place_of_service_source_value 
                        from 
                            CUHDW.DD_CCDEPTCT
                        """)
    oracle_result = oracle_cursor.fetchall()

    df = pd.DataFrame(oracle_result, columns = ["care_site_id", "care_site_name", "place_of_service_concept_id", "localtion_id", "care_site_source_value", "place_of_service_source_value"])
    result_data = [] 
    for idx, row in df.iterrows():
        #result_data.append(df.loc[[idx]])
        #print(result_data)
        care_site_id = row["care_site_id"]
        care_site_name = row["care_site_name"]
        place_of_service_concept_id = 0 #df["place_of_service_concept_id"][0]
        location_id = 0 #df["location_id"][0]
        care_site_source_value = row["care_site_source_value"]
        place_of_service_source_value = row["place_of_service_source_value"]

        tmp = (int(care_site_id), care_site_name, int(place_of_service_concept_id), int(location_id), care_site_source_value, place_of_service_source_value)
        result_data.append(tmp)


    oracle_cursor.close()
    oracle_connection.close()

    sql = """
        insert into cdmpv531.care_site
        (care_site_id, care_site_name, place_of_service_concept_id, location_id, care_site_source_value, place_of_service_source_value)
        values %s;
    """

    execute_values(postgresql_cursor, sql, result_data)
    postgresql_connection.commit()

    postgresql_cursor.close()
    postgresql_connection.close()
