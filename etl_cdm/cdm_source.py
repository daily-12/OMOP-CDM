import cx_Oracle
import psycopg2
from psycopg2.extras import execute_values

def ORCL_connect():
    # 라이브러리 연결
    cx_Oracle.init_oracle_client(lib_dir=r"C:/Oracle/instantclient_19_6")

    con_ip = '192.1.101.120/DWDB' # 서버주소/service_name
    con_id = 'CUHCDM'
    con_pw = 'CUHCDM0409'

    # 연결에 필요한 기본 정보(유저, 비밀번호, 데이터베이스 서버 주소)
    connection = cx_Oracle.connect(con_id, con_pw, con_ip)

    return connection

def POSTGRE_connect():
    con_ip = '192.1.170.180'
    con_db = 'odscdw'
    con_user = 'sisim'
    con_pw = 'qlrepdlxj'
    con_port = 5433
    con_option = '-c search_path=cdmpv531'

    connection = psycopg2.connect(host=con_ip, dbname = con_db, user = con_user, password = con_pw, port = con_port, options = con_option)

    return connection

def cdm_source():
        oracle_connection = ORCL_connect()
        oracle_cursor = oracle_connection.cursor()
        postgresql_connection = POSTGRE_connect()
        postgresql_cursor = postgresql_connection.cursor()
    #try:
        cdm_source_value = [("cdmpv531", "cdmpv531", "JBCD", "JBUH", None, None, None, None, "5.3.1", "5.0")]
        sql = """
            insert into cdmpv531.cdm_source
            (cdm_source_name, cdm_source_abbreviation, cdm_holder, source_description, source_documentation_reference, cdm_etl_reference, source_release_date, cdm_release_date, cdm_version, vocabulary_version)
            values %s;
            """ 
        
        execute_values(postgresql_cursor, sql, cdm_source_value)
        postgresql_connection.commit()


        postgresql_cursor.close()
        postgresql_connection.close()