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



start = time.time()

filename = (inspect.getfile(inspect.currentframe())).split('\\')[-1].split('.')[0] 


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s- %(levelname)s [%(filename)s:%(lineno)d] - %(message)s ')

log_dir = 'log'
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
    if values == None or values == 'N/A' or pd.isnull(values):
        values = None 
        return values
    else : 
        return values

def none_to_lob(values):
    if values == None :
        return None 
    else : 
        # Oracle lob 데이터 타입을 가진경우 .read()를 붙여야 string 타입으로 인식함 
        return values.read()

def pre_measurement():
    start = time.time()
    
    try :
        oracle_connection = ORCL_connect()
        oracle_cursor = oracle_connection.cursor()
        postgresql_connection = POSTGRE_connect()
        postgresql_cursor = postgresql_connection.cursor()

        # 전체 데이터 중 fetch_size개씩 읽어서 하고자 함..
        fetch_size = 20000
        # DW Oracle에서 조회
        table_name = ""
        start_date = ""
        end_date = ""
        oracle_sql = """
                        select
                            PATNO
                            , MEDDATE
                            , MEDDEPT
                            , MEDDR
                            , MEDTIME
                            , RSVTYP
                            , FSTMEDTYP
                            , MEDYN
                            , ADMDATE
                            , GUDMEDFG
                            , REJTTIME
                            , FOLLOWUPYN
                            , PJTCODE
                            , NRREGIYN
                            , NRRETIME
                            , FSTMEDTYP2
                            , REQHOS
                            , REJTCD
                            , MEDCENTER
                            , ORDTABLE
                            , ORDERKEY
                            , THEDAYMEDYN
                            , CANCERYN
                            , REGIP
                            , REGTIME
                            , EDITIP
                            , EDITTIME
                            , EXMAFYN
                            , MEDSTTM
                            , MEDENTM
                            , PRTDATE
                            , MTCODE
                            , ETLDATE          
                        from 
                            %s
                        where
                            MEDDATE between '%s' and '%s' 
                        """%(table_name, start_date, end_date)

        #오라클에서 쿼리 수행
        oracle_cursor.execute(oracle_sql)
        idx = 1
        while True :
            # fetch_size씩 데이터 불러오기
            oracle_result = oracle_cursor.fetchmany(fetch_size)

            if not oracle_result:
                # 데이터 전체를 다 읽은 경우 DB연결 끊기
                oracle_cursor.close()
                oracle_connection.close()
                postgresql_cursor.close()
                postgresql_connection.close()

                break
            
            # DW에서 읽은 데이터를 데이터프레임에 저장
            dw_df = pd.DataFrame(oracle_result, columns = ["PATNO", "MEDDATE", "MEDDEPT", "MEDDR", "MEDTIME", "RSVTYP", "FSTMEDTYP", "MEDYN", "ADMDATE", "GUDMEDFG", "REJTTIME", "FOLLOWUPYN", "PJTCODE", "NRREGIYN", "NRRETIME", "FSTMEDTYP2", "REQHOS", "REJTCD", "MEDCENTER", "ORDTABLE", "ORDERKEY", "THEDAYMEDYN", "CANCERYN", "REGIP", "REGTIME", "EDITIP", "EDITTIME", "EXMAFYN", "MEDSTTM", "MEDENTM", "PRTDATE", "MTCODE", "ETLDATE"] )
            dw_df.fillna('None')
            dw_df.replace({np.nan:None}, inplace = True)


            # 데이터프레임에서 ((values, val), (values, val), ...) 이런 형태로 변경하기 위함
            dw_df = dw_df.values.tolist()

            sql = """
                insert into cdmpv531.pre_apopdlst
                (patno, meddate, meddept, meddr, medtime, rsvtyp, fstmedtyp, medyn, admdate, gudmedfg, rejttime, followupyn, pjtcode, nrregiyn, nrretime, fstmedtyp2, reqhos, rejtcd, medcenter, ordtable, orderkey, thedaymedyn, canceryn, regip, regtime, editip, edittime, exmafyn, medsttm, medentm, prtdate, mtcode, etldate)
                values %s;
                """
            execute_values(postgresql_cursor, sql, dw_df)
            postgresql_connection.commit()
            end = time.time() - start
            logger.info('{} 회, 실행시간: {}'.format(idx, str(datetime.timedelta(seconds = end))) )

            idx += len(dw_df)
            

        end = time.time() - start
        logger.info('실행 완료, 총 소요시간 : %s' %(str(datetime.timedelta(seconds = end)) ))

    except Exception as err :
        logger.error(err)
        

pre_measurement()
