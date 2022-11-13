import cx_Oracle
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd 
import time
import datetime
import numpy as np

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


import pandas as pd 
import numpy as np
import os, inspect, time, datetime

import cx_Oracle, psycopg2
from psycopg2.extras import execute_values

import logging
from logging import handlers


start = time.time()

filename = (inspect.getfile(inspect.currentframe())).split('\\')[-1].split('.')[0] 


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s- %(levelname)s [%(filename)s:%(lineno)d] - %(message)s ')

log_dir = 'v_1/log'
FileHandler = logging.FileHandler(filename = os.path.join(log_dir, '{}_{:%Y%m%d%H%M%S}.log'.format(filename,datetime.datetime.now())), encoding='utf-8')
FileHandler.setFormatter(formatter)
logger.addHandler(FileHandler)


def validate_date(date_text):
    try:
        datetime.datetime.strptime(date_text,"%Y%m%d")
        return True
    except ValueError:
        print("Incorrect data format({0}), should be YYYY-MM-DD".format(date_text))
        logger.info("Incorrect data format({0}), should be YYYY-MM-DD".format(date_text))
        return False

def validate_datetime(date_text):
    try:
        datetime.datetime.strptime(date_text,"%Y%m%d%H%M")
        return True
    except ValueError:
        print("Incorrect data format({0}), should be YYYY-MM-DD HH24:MM".format(date_text))
        logger.info("Incorrect datatime format({0}), should be YYYY-MM-DD HH24:MM".format(date_text))
        return False


def none_to_null(values):
    if values == None or values == 'N/A':
        values = None 
        return values
    else : 
        return values


def pre_condition_occurrence():
    start = time.time()
    
    try :
        oracle_connection = ORCL_connect()
        oracle_cursor = oracle_connection.cursor()
        postgresql_connection = POSTGRE_connect()
        postgresql_cursor = postgresql_connection.cursor()

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

        # 전체 데이터 중 fetch_size개씩 읽어서 하고자 함..
        fetch_size = 20000
        # DW Oracle에서 조회
        table_name = ""
        oracle_sql = """
                        select
                            patno
                            , medtime
                            , meddept
                            , patfg
                            , diagfg
                            , chadr
                            , gendr
                            , diagcode
                            , onccode
                            , checkyn
                            , extcdyn
                            , cliniccode
                            , mainyn
                            , impressyn
                            , comdisyn
                            , admdiayn
                            , fnshdate
                            , opdate
                            , opseqno
                            , cpyn
                            , ruleoutyn
                        from 
                            %s
                        where
                            meddate between '19980101' and '20220930' 
                        """%(table_name)
        # person 
        # provider
        # visit_occur
        # local_kcd
        oracle_cursor.execute(oracle_sql)
        cnt = 1
        while True :
            oracle_result = oracle_cursor.fetchmany(fetch_size)

            if not oracle_result:
                oracle_cursor.close()
                oracle_connection.close()
                postgresql_cursor.close()
                postgresql_connection.close()

                break
            
            dw_df = pd.DataFrame(oracle_result, columns = ["PATNO", "MEDTIME", "MEDDEPT", "PATFG", "DIAGFG", "CHADR", "GENDR", "DIAGCODE", "ONCCODE", "CHECKYN", "EXTCDYN", "CLINICCODE", "MAINYN", "IMPRESSYN", "COMDISYN", "ADMDIAYN", "FNSHDATE", "OPDATE", "OPSEQNO", "CPYN", "RULEOUTYN"] )
            result_data = []

            dw_df.fillna('None')
            dw_df.replace({np.nan:None}, inplace = True)
            dw_df["CHADR"] = dw_df["CHADR"].apply(none_to_null)
            dw_df["GENDR"] = dw_df["GENDR"].apply(none_to_null)
            dw_df["ONCCODE"] = dw_df["ONCCODE"].apply(none_to_null)
            dw_df["CLINICCODE"] = dw_df["CLINICCODE"].apply(none_to_null)

            dw_lst = dw_df.values.tolist()

            sql = """
                insert into cdmpv531.pre_condition_occurrence
                (patno, medtime, meddept, patfg, diagfg, chadr, gendr, diagcode, onccode, checkyn, extcdyn, cliniccode, mainyn, impressyn, comdisyn, admdiayn, fnshdate, opdate, opseqno, cpyn, ruleoutyn)
                values %s;
                """
            execute_values(postgresql_cursor, sql, dw_lst)
            postgresql_connection.commit()

            
            cnt += len(dw_df)
            
            end = time.time() - start
            logger.info('{} 회, 실행시간: {}'.format(cnt, str(datetime.timedelta(seconds = end))) )

        end = time.time() - start
        logger.info('실행 완료, 총 소요시간 : %s' %(str(datetime.timedelta(seconds = end))))

    except Exception as err :
        logger.error(err)

pre_condition_occurrence()
