# 전북대학교병원 사망정보 데이터는 일단 변환하지 않음 -- 김민걸 교수님 지시.

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

def validate_date(date_text):
    try:
        datetime.datetime.strptime(date_text,"%Y%m%d")
        return True
    except ValueError:
        print("Incorrect data format({0}), should be YYYY-MM-DD".format(date_text))
        return False

def death():
    oracle_connection = ORCL_connect()
    oracle_cursor = oracle_connection.cursor()
    postgresql_connection = POSTGRE_connect()
    postgresql_cursor = postgresql_connection.cursor()

    postgresql_cursor.execute("""
                                select 
                                    person_id
                                    , person_source_value
                                from 
                                    cdmpv531.person ;
                                """)
    person = postgresql_cursor.fetchall()
    person_df = pd.DataFrame(person, columns= ["person_id", "person_source_value"])

    table_name = ""
    oracle_sql = """
                        select
                            PATNO
                            , DIEDATE
                            , DIECAUSE
                        from 
                            %s
                        where 
                            DIEDATE IS NOT NULL
                        """%(table_name)

    oracle_cursor.execute(oracle_sql)
    oracle_result = oracle_cursor.fetchall()
    dw_df = pd.DataFrame(oracle_result, columns = ["PATNO", "DIEDATE", "DIECAUSE"] )
    cnt = 1

    result_data = []
    death_df = pd.merge(left = dw_df, right = person_df, how = 'inner', left_on='PATNO', right_on='person_source_value')

    for idx, row in death_df.iterrows():

        person_id = row['person_id']
        if validate_date(row['DIEDATE']) is True :
            death_date = row['DIEDATE']
            death_datetime = death_date
        else : 
            death_date = None
            death_datetime = death_date
        death_type_concept_id = 0
        cause_concept_id = 0
        cause_source_value = row['DIECAUSE']
        cause_source_concept_id = 0
        diedate = row['DIEDATE']

        tmp = (person_id, death_date, death_datetime, death_type_concept_id, cause_concept_id, cause_source_value, cause_source_concept_id, diedate)
        result_data.append(tmp)
        cnt += 1

    oracle_cursor.close()
    oracle_connection.close()

    sql = """
            insert into cdmpv531.death
            (person_id, death_date, death_datetime, death_type_concept_id, cause_concept_id, cause_source_value, cause_source_concept_id, diedate)
            values %s;
            """
    execute_values(postgresql_cursor, sql, result_data)
    postgresql_connection.commit()

    postgresql_cursor.close()
    postgresql_connection.close()