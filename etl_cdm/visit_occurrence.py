import cx_Oracle
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd 
import os, inspect, time, datetime
import datetime
import numpy as np
import logging


filename = (inspect.getfile(inspect.currentframe())).split('\\')[-1].split('.')[0] 


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s- %(levelname)s [%(filename)s:%(lineno)d] - %(message)s ')

log_dir = 'log'
FileHandler = logging.FileHandler(filename = os.path.join(log_dir, '{}_{:%Y%m%d%H%M%S}.log'.format(filename,datetime.datetime.now())), encoding='utf-8')
FileHandler.setFormatter(formatter)
logger.addHandler(FileHandler)


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

def validate_datetime(date_text):
    try:
        datetime.datetime.strptime(date_text,"%Y%m%d%H%M")
        return True
    except ValueError:
        print("Incorrect data format({0}), should be YYYY-MM-DD HH24:MM".format(date_text))
        return False

def none_to_null(values):
    if values == None or values == 'N/A':
        values = None 
        return values
    else : 
        return values

def visit_occurrence():
    start = time.time()
    # person, provider, care_site
    oracle_connection = ORCL_connect()
    oracle_cursor = oracle_connection.cursor()
    postgresql_connection = POSTGRE_connect()
    postgresql_cursor = postgresql_connection.cursor()

    # person 데이터 가져오기
    postgresql_cursor.execute("""
                                select 
                                    person_id
                                    , person_source_value
                                from 
                                    cdmpv531.person ;
                                """)
    care_site = postgresql_cursor.fetchall()
    person_df = pd.DataFrame(care_site, columns= ["person_id", "person_source_value"])

    # provider 데이터 가져오기
    postgresql_cursor.execute("""
                                select 
                                    provider_id
                                    , provider_source_value
                                from 
                                    cdmpv531.provider ;
                                """)
    care_site = postgresql_cursor.fetchall()
    provider_df = pd.DataFrame(care_site, columns= ["provider_id", "provider_source_value"])

    # care_site 데이터 가져오기
    postgresql_cursor.execute("""
                                select 
                                    care_site_id
                                    , care_site_source_value
                                from 
                                    cdmpv531.care_site ;
                                """)
    care_site = postgresql_cursor.fetchall()
    care_site_df = pd.DataFrame(care_site, columns= ["care_site_id", "care_site_source_value"])

    # 전체 데이터 중 fetch_size개씩 읽어서 commit 하고자 함..
    fetch_size = 10000
    # DW Oracle에서 조회
    table_name = ""
    oracle_sql = """
                    select
                        PATNO
                        , PATFG
                        , IOSTAT
                        , MEDDEPT
                        , MEDDR
                        , MEDTIME
                        , FSTMEDTYP
                        , MEDTYP
                        , PATTYP
                        , TYPECD
                        , MEDSTM
                        , MEDETM
                        , MEDYN
                        , REJTTIME
                        , ADMTIME
                        , ADMPATH
                        , DSCHTIME
                        , DSCTYPE
                        , DSCRSN
                    from 
                        %s
                    where
                        substr(medtime,1,8) between '20220929' and '20220930' 
                    """%(table_name)

    oracle_cursor.execute(oracle_sql)
    idx = 1
    while True :
        oracle_result = oracle_cursor.fetchmany(fetch_size)

        if not oracle_result:
            oracle_cursor.close()
            oracle_connection.close()
            postgresql_cursor.close()
            postgresql_connection.close()
            
            break
        
        dw_df = pd.DataFrame(oracle_result, columns = ["PATNO", "PATFG", "IOSTAT", "MEDDEPT", "MEDDR", "MEDTIME", "FSTMEDTYP", "MEDTYP", "PATTYP", "TYPECD", "MEDSTM", "MEDETM", "MEDYN", "REJTTIME", "ADMTIME", "ADMPATH", "DSCHTIME", "DSCTYPE", "DSCRSN"] )
        #dw_dd = dd.from_pandas(dw_df, npartitions = 2)
        result_data = []
        #dw_df['SEX'] = dw_df['SEX'].apply(change_val)
        visit_df = pd.merge(left = dw_df, right=person_df, how='inner', left_on='PATNO', right_on='person_source_value')
        visit_df = pd.merge(left = visit_df, right=care_site_df, how='left', left_on='MEDDEPT', right_on='care_site_source_value')
        visit_df = pd.merge(left = visit_df, right=provider_df, how='left', left_on='MEDDR', right_on='provider_source_value')

        # 시간 데이터 Null값으로 NaT가 있는 경우가 있는데, 그걸 None으로 바꿔주기 위함임
        visit_df.replace({np.nan:None}, inplace = True)

        for i, row in visit_df.iterrows():
            
            visit_occurrence_id = idx
            person_id = row['person_id']
            if row['PATFG'] == 'I':
                visit_concept_id = 9201
            elif row['PATFG'] == 'O':
                visit_concept_id = 9202
            elif row['PATFG'] == 'E':
                visit_concept_id = 9203

            visit_start_date = row['MEDTIME'][0:8]
            visit_start_datetime = row['MEDTIME']
            if validate_datetime(row['MEDTIME']) is True and row['PATFG'] == 'O':
                visit_start_date = datetime.datetime.strptime(row['MEDTIME'][0:8], "%Y%m%d")
                visit_start_datetime = datetime.datetime.strptime(row['MEDTIME'], "%Y%m%d%H%M")
            elif validate_datetime(row['MEDTIME']) is True and row['ADMTIME'] == None :
                visit_start_date = datetime.datetime.strptime(row['MEDTIME'][0:8], "%Y%m%d")
                visit_start_datetime = datetime.datetime.strptime(row['MEDTIME'], "%Y%m%d%H%M")
            elif row['PATFG'] in ('I', 'E'):
                visit_start_date = row['ADMTIME'].date()
                visit_start_datetime = row['ADMTIME']
            else :
                visit_start_date = datetime.datetime.strptime(row['MEDTIME'][0:8], "%Y%m%d")
                visit_start_datetime = datetime.datetime.strptime(row['MEDTIME'][0:8], "%Y%m%d") 

            if row['DSCHTIME'] == None:
                visit_end_date = visit_start_date
                visit_end_datetime = visit_start_datetime
            else :
                visit_end_date = row['DSCHTIME'].date()
                visit_end_datetime = row['DSCHTIME'] 
            
            visit_type_concept_id = 44818518
            provider_id = row['provider_id']
            care_site_id = row['care_site_id']
            visit_source_value = row['PATFG']
            visit_source_concept_id = 0
            admitting_source_concept_id = 0
            admitting_source_value = row['ADMPATH']
            discharge_to_concept_id = 0
            discharge_to_source_value = row['DSCTYPE']
            preceding_visit_occurrence_id = 0
            medtime = row['MEDTIME']

            admtime = none_to_null(row['ADMTIME'])
            iostat = row['IOSTAT']
            fstmedtyp = none_to_null(row['FSTMEDTYP'])
            medtyp = none_to_null(row['MEDTYP'])
            medstm = none_to_null(row['MEDSTM'])
            medetm = none_to_null(row['MEDETM'])
            medyn = row['MEDYN']
            rejttime = none_to_null(row['REJTTIME'])
            admpath = none_to_null(row['ADMPATH'])
            dschtime = none_to_null(row['DSCHTIME'])
            dsctype = none_to_null(row['DSCTYPE'])
            dscrsn = none_to_null(row['DSCRSN'])


            tmp = (visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_start_datetime, visit_end_date, visit_end_datetime, visit_type_concept_id, provider_id, care_site_id, visit_source_value, visit_source_concept_id, admitting_source_concept_id, admitting_source_value, discharge_to_concept_id, discharge_to_source_value, preceding_visit_occurrence_id, medtime, admtime, iostat, fstmedtyp, medtyp, medstm, medetm, medyn, rejttime, admpath, dschtime, dsctype, dscrsn)
            result_data.append(tmp)
            
            idx += 1

        sql = """
            insert into cdmpv531.visit_occurrence
            (visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_start_datetime, visit_end_date, visit_end_datetime, visit_type_concept_id, provider_id, care_site_id, visit_source_value, visit_source_concept_id, admitting_source_concept_id, admitting_source_value, discharge_to_concept_id, discharge_to_source_value, preceding_visit_occurrence_id, medtime, admtime, iostat, fstmedtyp, medtyp, medstm, medetm, medyn, rejttime, admpath, dschtime, dsctype, dscrsn)
            values %s;
            """
        execute_values(postgresql_cursor, sql, result_data)
        postgresql_connection.commit()
        end = time.time() - start
        logger.info('{} 회, 실행시간: {}'.format(idx, str(datetime.timedelta(seconds = end))) )
            
    end = time.time() - start
    logger.info('실행 완료, 총 소요시간 : %s' %(str(datetime.timedelta(seconds = end)) ))

visit_occurrence()