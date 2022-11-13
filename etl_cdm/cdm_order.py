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


def cdm_order():
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
                            patno
                            , orddate
                            , ordseqno
                            , ordtabfg
                            , patfg
                            , appatfg
                            , meddate
                            , medtime
                            , medtimed
                            , meddept
                            , sex
                            , age
                            , opdate
                            , opseqno
                            , ordcls
                            , ordkind
                            , ordclstyp
                            , ordtyp
                            , ordtyp2
                            , ordcode
                            , ordname
                            , orddetail
                            , slipcode
                            , patguide
                            , orddr
                            , chadr
                            , gendr
                            , opdr1
                            , anethdr
                            , execdr
                            , addordyn
                            , carestat
                            , procstat
                            , remark
                            , ordcfmtimed
                            , dcyn
                            , dctime
                            , pjtcode
                            , pjtdr
                            , regtime
                            , medcenter
                            , orddept
                            , outqty
                            , retnqty
                            , wasteqty
                            , slfbldyn
                            , donbldyn
                            , bldreqfg
                            , examtyp
                            , readdr1
                            , spccode1
                            , spcno1
                            , exectime
                            , exectimed
                            , readtimed
                            , repttimed
                            , colltimed
                            , drugcls
                            , drugtyp
                            , dgordtyp
                            , inoutfg
                            , contqty
                            , contunit
                            , cnt
                            , day
                            , methodcd
                            , dgrtnfg
                            , dgrtncd
                            , phrtnqty
                            , aprtnqty
                            , etldate
                        from 
                            %s
                        where
                            orddate between '%s' and '%s' 
                        """%(table_name, start_date, end_date)
        # person 
        # provider
        # visit_occur
        # local_kcd
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
            
            dw_df = pd.DataFrame(oracle_result, columns = ["PATNO", "ORDDATE", "ORDSEQNO", "ORDTABFG", "PATFG", "APPATFG", "MEDDATE", "MEDTIME", "MEDTIMED", "MEDDEPT", "SEX", "AGE", "OPDATE", "OPSEQNO", "ORDCLS", "ORDKIND", "ORDCLSTYP", "ORDTYP", "ORDTYP2", "ORDCODE", "ORDNAME", "ORDDETAIL", "SLIPCODE", "PATGUIDE", "ORDDR", "CHADR", "GENDR", "OPDR1", "ANETHDR", "EXECDR", "ADDORDYN", "CARESTAT", "PROCSTAT", "REMARK", "ORDCFMTIMED", "DCYN", "DCTIME", "PJTCODE", "PJTDR", "REGTIME", "MEDCENTER", "ORDDEPT", "OUTQTY", "RETNQTY", "WASTEQTY", "SLFBLDYN", "DONBLDYN", "BLDREQFG", "EXAMTYP", "READDR1", "SPCCODE1", "SPCNO1", "EXECTIME", "EXECTIMED", "READTIMED", "REPTTIMED", "COLLTIMED", "DRUGCLS", "DRUGTYP", "DGORDTYP", "INOUTFG", "CONTQTY", "CONTUNIT", "CNT", "DAY", "METHODCD", "DGRTNFG", "DGRTNCD", "PHRTNQTY", "APRTNQTY", "ETLDATE"] )
            dw_df.fillna('None')
            dw_df.replace({np.nan:None}, inplace = True)
            
            dw_df["ORDTYP2"] = dw_df["ORDTYP2"].apply(none_to_null)
            dw_df["PATGUIDE"] = dw_df["PATGUIDE"].apply(none_to_null)
            dw_df["ORDDR"] = dw_df["ORDDR"].apply(none_to_null)
            dw_df["CHADR"] = dw_df["CHADR"].apply(none_to_null)
            dw_df["GENDR"] = dw_df["GENDR"].apply(none_to_null)
            dw_df["OPDR1"] = dw_df["OPDR1"].apply(none_to_null)
            dw_df["ANETHDR"] = dw_df["ANETHDR"].apply(none_to_null)
            dw_df["EXECDR"] = dw_df["EXECDR"].apply(none_to_null)
            dw_df["CARESTAT"] = dw_df["CARESTAT"].apply(none_to_null)
            dw_df["PROCSTAT"] = dw_df["PROCSTAT"].apply(none_to_null)
            dw_df["DCYN"] = dw_df["DCYN"].apply(none_to_null)
            dw_df["DCTIME"] = dw_df["DCTIME"].apply(none_to_null)
            dw_df["PJTCODE"] = dw_df["PJTCODE"].apply(none_to_null)
            dw_df["PJTDR"] = dw_df["PJTDR"].apply(none_to_null)
            dw_df["EXAMTYP"] = dw_df["EXAMTYP"].apply(none_to_null)
            dw_df["READDR1"] = dw_df["READDR1"].apply(none_to_null)
            dw_df["SPCCODE1"] = dw_df["SPCCODE1"].apply(none_to_null)
            dw_df["SPCNO1"] = dw_df["SPCNO1"].apply(none_to_null)
            dw_df["DRUGCLS"] = dw_df["DRUGCLS"].apply(none_to_null)
            dw_df["DRUGTYP"] = dw_df["DRUGTYP"].apply(none_to_null)
            dw_df["DGORDTYP"] = dw_df["DGORDTYP"].apply(none_to_null)
            dw_df["INOUTFG"] = dw_df["INOUTFG"].apply(none_to_null)
            dw_df["METHODCD"] = dw_df["METHODCD"].apply(none_to_null)
            dw_df["DGRTNFG"] = dw_df["DGRTNFG"].apply(none_to_null)
            dw_df["DGRTNCD"] = dw_df["DGRTNCD"].apply(none_to_null)
            dw_df["PHRTNQTY"] = dw_df["PHRTNQTY"].apply(none_to_null)
            dw_df["APRTNQTY"] = dw_df["APRTNQTY"].apply(none_to_null)

            # 데이터프레임에서 ((values, val), (values, val), ...) 이런 형태로 변경하기 위함
            dw_df = dw_df.values.tolist()
            
            idx += len(dw_df)

            sql = """
                insert into cdmpv531.cdm_order
                (patno, orddate, ordseqno, ordtabfg, patfg, appatfg, meddate, medtime, medtimed, meddept, sex, age, opdate, opseqno, ordcls, ordkind, ordclstyp, ordtyp, ordtyp2, ordcode, ordname, orddetail, slipcode, patguide, orddr, chadr, gendr, opdr1, anethdr, execdr, addordyn, carestat, procstat, remark, ordcfmtimed, dcyn, dctime, pjtcode, pjtdr, regtime, medcenter, orddept, outqty, retnqty, wasteqty, slfbldyn, donbldyn, bldreqfg, examtyp, readdr1, spccode1, spcno1, exectime, exectimed, readtimed, repttimed, colltimed, drugcls, drugtyp, dgordtyp, inoutfg, contqty, contunit, cnt, "day", methodcd, dgrtnfg, dgrtncd, phrtnqty, aprtnqty, etldate)
                values %s;
                """
            execute_values(postgresql_cursor, sql, dw_df)
            postgresql_connection.commit()
            end = time.time() - start
            logger.info('{} 회, 실행시간: {}'.format(idx, str(datetime.timedelta(seconds = end))) )
            

        end = time.time() - start
        logger.info('실행 완료, 총 소요시간 : %s' %(str(datetime.timedelta(seconds = end)) ))

    except Exception as err :
        logger.error(err)

cdm_order()
