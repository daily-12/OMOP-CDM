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

StreamHandler = logging.StreamHandler()
StreamHandler.setFormatter(formatter)
logger.addHandler(StreamHandler)

log_dir = 'log'
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


def local_edi():
    start = time.time()
    
    try :
        oracle_connection = ORCL_connect()
        oracle_cursor = oracle_connection.cursor()
        postgresql_connection = POSTGRE_connect()
        postgresql_cursor = postgresql_connection.cursor()

        # Postgresql : Concept테이블 불러오기
        postgresql_cursor.execute("""
                                    select 
                                        concept_id
                                        , domain_id
                                        , vocabulary_id
                                        , replace(concept_code, '.', '') AS concept_code
                                    from
                                        omop.concept
                                    where
                                        vocabulary_id = 'EDI' ;
                                    """)
        care_site = postgresql_cursor.fetchall()
        concept_df = pd.DataFrame(care_site, columns= ["concept_id", "domain_id", "vocabulary_id", "concept_code"])

        # Oracle : 상병코드 마스터 불러오기
        # 전체 데이터 중 fetch_size개씩 읽어서 commit...
        fetch_size = 50000
        # DW Oracle에서 조회
        oracle_sql = """
                        SELECT 
                            dm.ordcode 
                        	, CASE 
                        		WHEN da.FROMDATE IS NULL THEN '19000101'
                        		ELSE  da.fromdate
                        	  END AS FROMDATE
                        	, CASE 
                        		WHEN da.TODATE IS NULL THEN '29991231'
                        		ELSE  da.TODATE 
                        	  END AS FROMDATE
                        	, dm.ORDNAME  
                        	, da.INSEDICODE
                        	, dm.ORDCLSTYP
                        	, dm.SLIPCODE
                        FROM 
                        	dm 
                        LEFT JOIN 
                        	da 
                        ON dm.ORDCODE = da.SUGACODE
                    """
        oracle_cursor.execute(oracle_sql)

        while True:
            
            oracle_result = oracle_cursor.fetchmany(fetch_size)
            if not oracle_result:
                oracle_cursor.close()
                oracle_connection.close()
                postgresql_cursor.close()
                postgresql_connection.close()
                break

            dw_df = pd.DataFrame(oracle_result, columns = ["ORDCODE", "FROMDATE", "TODATE", "ORDNAME", "INSEDICODE", "ORDCLSTYP", "SLIPCODE"])
            local_edi_df = pd.merge(left = dw_df, right = concept_df, how= 'left', left_on = 'INSEDICODE', right_on = 'concept_code')
            local_edi_df.replace({np.nan:None}, inplace = True)

            result_data = []
            
            for idx, row in local_edi_df.iterrows():
                if row["concept_id"] == None:
                    concept_id = None
                else :
                    concept_id = int(row["concept_id"])
                if row["vocabulary_id"] == None:
                    vocabulary_id = None
                else :
                    vocabulary_id = row["vocabulary_id"]
                if row["domain_id"] == None :
                    domain_id = None
                else :
                    domain_id = row["domain_id"]
                ordcode = row["ORDCODE"]
                fromdate = row["FROMDATE"]
                todate = row["TODATE"]
                ordname = row["ORDNAME"]
                insedicode = row["INSEDICODE"]
                ordclstyp = row["ORDCLSTYP"]
                slipcode = row["SLIPCODE"]
                tmp = (concept_id, vocabulary_id, domain_id, ordcode, fromdate, todate, ordname, insedicode, ordclstyp, slipcode)
                result_data.append(tmp)

            sql = """
                    insert into cdmpv531.local_edi
                    (concept_id, vocabulary_id, domain_id, ordcode, fromdate, todate, ordname, insedicode, ordclstyp, slipcode)
                    values %s;
                """
            execute_values(postgresql_cursor, sql, result_data)
            postgresql_connection.commit()

            end = time.time() - start
        logger.info('실행 완료, 총 소요시간 : %s' %(str(datetime.timedelta(seconds = end)) ))

    except Exception as err :
        logger.error(err)
