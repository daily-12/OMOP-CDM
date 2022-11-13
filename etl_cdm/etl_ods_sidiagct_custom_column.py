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

log_dir = 'v_1/log'
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

def none_to_null(values):
    if values == None or values == 'N/A' or pd.isnull(values):
        values = None 
        return values
    else : 
        return values

def etl_ods_sidiagct_custom_column():
    start = time.time()
    
    try :
        oracle_connection = ORCL_connect()
        oracle_cursor = oracle_connection.cursor()
        postgresql_connection = POSTGRE_connect()
        postgresql_cursor = postgresql_connection.cursor()

        # Oracle : 상병코드 마스터 불러오기
        # 전체 데이터 중 fetch_size개씩 읽어서 commit...
        fetch_size = 10000
        # DW Oracle에서 조회
        table_name = ""
        dw_table_name = ""
        oracle_sql = """
                            select
                                a.DIAGCODE
                                , a.FROMDATE 
                                , a.TODATE
                                , a.ENGNAME
                                , a.KORNAME
                                , a.DIAGREQYN
                                , a.DIAGSEX
                                , a.INFDIAGYN
                                , a.INFDIAGFG
                                , a.INFDIAGCD
                                , a.MEDUSEYN
                                , a.STDIAGCD
                                , a.KCDVERSION
                                , SGRPENM
                                , SGRPKNM
                                , MGRPENM
                                , MGRPKNM
                                , LGRPENM
                                , LGRPKNM
                                , KOSTOMCD
                                , a.REGTIME
                                , a.EDITTIME
                                , a.ETLDATE
                            from 
                                %s a
                                left join 
                                %s b 
                                on a.diagcode = b.diagcode
                                and a.fromdate = b.fromdate
                            """%(table_name, dw_table_name)

        oracle_cursor.execute(oracle_sql)
        oracle_result = oracle_cursor.fetchall()
        dw_df = pd.DataFrame(oracle_result, columns = ["DIAGCODE", "FROMDATE", "TODATE", "ENGNAME", "KORNAME", "DIAGREQYN", "DIAGSEX", "INFDIAGYN", "INFDIAGFG", "INFDIAGCD", "MEDUSEYN", "STDIAGCD", "KCDVERSION", "SGRPENM", "SGRPKNM", "MGRPENM", "MGRPKNM", "LGRPENM", "LGRPKNM", "KOSTOMCD", "REGTIME", "EDITTIME", "ETLDATE"])

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
                                        vocabulary_id = 'KCD7' ;
                                    """)
        care_site = postgresql_cursor.fetchall()
        concept_df = pd.DataFrame(care_site, columns= ["concept_id", "domain_id", "vocabulary_id", "concept_code"])
        
        local_kcd_df = pd.merge(left = dw_df, right = concept_df, how= 'left', left_on = 'DIAGCODE', right_on = 'concept_code')

        # concept_code컬럼은 테이블에 값을 넣지 않기 때문에 제거
        local_kcd_df.drop(['concept_code'], axis = 1, inplace = True)

        #local_kcd_df["concept_id"] = local_kcd_df["concept_id"].astype('int64', copy=False)
        local_kcd_df.replace({np.nan:None}, inplace = True)
        # timestamp에 대한 잘못된 입력 NaT 해결하고자
        #local_kcd_df.replace({pd.NaT:None}, inplace = True)
        local_kcd_df.fillna(np.nan).replace([np.nan], [None])
        local_kcd_df.fillna('None')
        
        print(local_kcd_df.columns)
        print(local_kcd_df.head())

        # Null값이 NaN으로 표기되어 None으로 변경하기 위함
        local_kcd_df["DIAGCODE"] = local_kcd_df["DIAGCODE"].apply(none_to_null)
        local_kcd_df["FROMDATE"] = local_kcd_df["FROMDATE"].apply(none_to_null)
        local_kcd_df["TODATE"] = local_kcd_df["TODATE"].apply(none_to_null)
        local_kcd_df["ENGNAME"] = local_kcd_df["ENGNAME"].apply(none_to_null)
        local_kcd_df["KORNAME"] = local_kcd_df["KORNAME"].apply(none_to_null)
        local_kcd_df["DIAGREQYN"] = local_kcd_df["DIAGREQYN"].apply(none_to_null)
        local_kcd_df["DIAGSEX"] = local_kcd_df["DIAGSEX"].apply(none_to_null)
        local_kcd_df["INFDIAGYN"] = local_kcd_df["INFDIAGYN"].apply(none_to_null)
        local_kcd_df["INFDIAGFG"] = local_kcd_df["INFDIAGFG"].apply(none_to_null)
        local_kcd_df["INFDIAGCD"] = local_kcd_df["INFDIAGCD"].apply(none_to_null)
        local_kcd_df["MEDUSEYN"] = local_kcd_df["MEDUSEYN"].apply(none_to_null)
        local_kcd_df["STDIAGCD"] = local_kcd_df["STDIAGCD"].apply(none_to_null)
        local_kcd_df["KCDVERSION"] = local_kcd_df["KCDVERSION"].apply(none_to_null)
        local_kcd_df["SGRPENM"] = local_kcd_df["SGRPENM"].apply(none_to_null)
        local_kcd_df["SGRPKNM"] = local_kcd_df["SGRPKNM"].apply(none_to_null)
        local_kcd_df["MGRPENM"] = local_kcd_df["MGRPENM"].apply(none_to_null)
        local_kcd_df["MGRPKNM"] = local_kcd_df["MGRPKNM"].apply(none_to_null)
        local_kcd_df["LGRPENM"] = local_kcd_df["LGRPENM"].apply(none_to_null)
        local_kcd_df["LGRPKNM"] = local_kcd_df["LGRPKNM"].apply(none_to_null)
        local_kcd_df["KOSTOMCD"] = local_kcd_df["KOSTOMCD"].apply(none_to_null)
        local_kcd_df["REGTIME"] = local_kcd_df["REGTIME"].apply(none_to_null)
        local_kcd_df["EDITTIME"] = local_kcd_df["EDITTIME"].apply(none_to_null)
        local_kcd_df["ETLDATE"] = local_kcd_df["ETLDATE"].apply(none_to_null)
        local_kcd_df["concept_id"] = local_kcd_df["concept_id"].apply(none_to_null)
        local_kcd_df["domain_id"] = local_kcd_df["domain_id"].apply(none_to_null)
        local_kcd_df["vocabulary_id"] = local_kcd_df["vocabulary_id"].apply(none_to_null)

        result_data = local_kcd_df.values.tolist()    
        print(result_data)

        sql = """
                insert into cdmpv531.local_kcd
                (diagcode, fromdate, todate, engname, korname, diagreqyn, diagsex, infdiagyn, infdiagfg, infdiagcd, meduseyn, stdiagcd, kcdversion, sgrpenm, sgrpknm, mgrpenm, mgrpknm, lgrpenm, lgrpknm, kostomcd, regtime, edittime, etldate, concept_id, vocabulary_id, domain_id)
                values %s;
            """
        execute_values(postgresql_cursor, sql, result_data)
        postgresql_connection.commit()
        
        oracle_cursor.close()
        oracle_connection.close()

        postgresql_cursor.close()
        postgresql_connection.close()

        end = time.time() - start
        logger.info('실행 완료, 총 소요시간 : %s' %(str(datetime.timedelta(seconds = end)) ))

    except Exception as err :
        logger.error(err)

etl_ods_sidiagct_custom_column()