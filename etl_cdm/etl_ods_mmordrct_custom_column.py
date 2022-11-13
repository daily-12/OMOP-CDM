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
formatter = logging.Formatter('%(asctime)s - %(name)s- %(levelname)s - %(message)s')

log_dir = './v_1/log'
FileHandler = handlers.TimedRotatingFileHandler(filename = os.path.join(log_dir, '{}_{:%Y%m%d%H%M%S}.log'.format(filename,datetime.datetime.now())), interval = 1, encoding = 'utf-8')
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


def etl_ods_mmordrct_custom_column():
    start = time.time()
    
    try :
        oracle_connection = ORCL_connect()
        oracle_cursor = oracle_connection.cursor()
        postgresql_connection = POSTGRE_connect()
        postgresql_cursor = postgresql_connection.cursor()

        # Oracle : 상병코드 마스터 불러오기
        # 전체 데이터 중 fetch_size개씩 읽어서 commit...
        fetch_size = 30000

        # DW Oracle에서 조회
        table_name = ''
        oracle_sql = """
                        SELECT 
                            ORDCODE
                            , FROMDATE
                            , TODATE
                            , ORDNAME
                            , ORDCLS
                            , ORDCLSTYP
                            , SLIPCODE
                            , PATGUIDE
                            , ORDYN
                            , DRUGNAME
                            , PACKUNIT
                            , CONTQTY
                            , CONTUNIT
                            , PRODKRNM
                            , PRODENNM
                            , IGRDNAME
                            , DRUGCLS
                            , DRUGTYP
                            , DRUGFORM
                            , EFCCODE
                            , METHODCD
                            , SPDGYN
                            , ANTIYN
                            , DISTRNAME
                            , PHRMNAME
                            , RSLTTYP
                            , SPCCODE1
                            , SPCQTY1
                            , TUBECODE1
                            , EXAMTYP
                            , RSLTUNIT
                            , REGTIME
                            , EDITTIME
                            , MAINIGRCD
                            , KIMSCATE
                            , ATCCODE
                            , ETLDATE
                        FROM 
                        	%s
                        WHERE 
                            EDITTIME BETWEEN   trunc(SYSDATE-4, 'DD') AND  trunc(SYSDATE, 'DD')
                    """ %(table_name)
        oracle_cursor.execute(oracle_sql)
        
        # update할 row만 있는 _temp 테이블 이전 데이터 삭제
        postgresql_cursor.execute("truncate table cdmpv531.ods_mmordrct_temp;")

        while True:
 
            oracle_result = oracle_cursor.fetchmany(fetch_size)

            if not oracle_result:
                oracle_cursor.close()
                oracle_connection.close()
                break

            dw_df = pd.DataFrame(oracle_result, columns = ["ordcode", "fromdate", "todate", "ordname", "ordcls", "ordclstyp", "slipcode", "patguide", "ordyn", "drugname", "packunit", "contqty", "contunit", "prodkrnm", "prodennm", "igrdname", "drugcls", "drugtyp", "drugform", "efccode", "methodcd", "spdgyn", "antiyn", "distrname", "phrmname", "rslttyp", "spccode1", "spcqty1", "tubecode1", "examtyp", "rsltunit", "regtime", "edittime", "mainigrcd", "kimscate", "atccode", "etldate"])
            dw_df.replace({np.nan:None}, inplace = True)
            dw_df.fillna('None')

            result_data = dw_df.values.tolist()
            
            sql = """
                    insert into cdmpv531.ods_mmordrct_temp
                    (ordcode, fromdate, todate, ordname, ordcls, ordclstyp, slipcode, patguide, ordyn, drugname, packunit, contqty, contunit, prodkrnm, prodennm, igrdname, drugcls, drugtyp, drugform, efccode, methodcd, spdgyn, antiyn, distrname, phrmname, rslttyp, spccode1, spcqty1, tubecode1, examtyp, rsltunit, regtime, edittime, mainigrcd, kimscate, atccode, etldate)
                    values %s;
                """
            # Oracle에서 조회하여 update할 row만 있는 데이터 _temp 테이블에 저장
            execute_values(postgresql_cursor, sql, result_data)
            
        # PostgreSQL 코드 마스터 테이블에 update할 row 최종 insert
        postgresql_cursor.execute("CALL pc_update_ods_mmordrct();")
        postgresql_connection.commit()
        
        postgresql_cursor.close()
        postgresql_connection.close()

        end = time.time() - start
        logger.info('실행 완료, 총 소요시간 : %s' %(str(datetime.timedelta(seconds = end )) ))

    except Exception as err :
        logger.error(err)

etl_ods_mmordrct_custom_column()