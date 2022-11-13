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

def person():
    
    oracle_connection = ORCL_connect()
    oracle_cursor = oracle_connection.cursor()
    postgresql_connection = POSTGRE_connect()
    postgresql_cursor = postgresql_connection.cursor()

    # 전체 데이터 중 fetch_size개씩 읽어서 commit...
    fetch_size = 10000
    # DW Oracle에서 조회
    table_name = ""
    oracle_sql = """
                        select
                            PATNO
                            , RESNO1
                            , RESNO2
                            , BIRTHDAY
                            , SEX
                            , ZIPCODE
                            , DIEDATE
                            , ABOTYP
                            , RHTYP
                            , HEIGHT
                            , WEIGHT
                        from 
                            %s
                        where
                            BIRTHDAY IS NOT NULL
                            AND BIRTHDAY NOT LIKE \'%%%s%%\'
                        """%(table_name, '-')

    oracle_cursor.execute(oracle_sql)
    cnt = 1
    person_id = "nextval(\'cdmpv531.\"seq_person_id\"\')"

    while True :
        oracle_result = oracle_cursor.fetchmany(fetch_size)

        if not oracle_result:
            oracle_cursor.close()
            oracle_connection.close()
            postgresql_cursor.close()
            postgresql_connection.close()
            
            break
        
        dw_df = pd.DataFrame(oracle_result, columns = ["PATNO", "RESNO1", "RESNO2", "BIRTHDAY", "SEX", "ZIPCODE", "DIEDATE", "ABOTYP", "RHTYP", "HEIGHT", "WEIGHT"] )
        #dw_dd = dd.from_pandas(dw_df, npartitions = 2)
        result_data = []
        #dw_df['SEX'] = dw_df['SEX'].apply(change_val)

        for i, row in dw_df.iterrows():
            #person_id = cnt
            if row["SEX"] == 'M':
                gender_concept_id = 8507
            elif row["SEX"] == 'F':
                gender_concept_id = 8532
            year_of_birth = row["BIRTHDAY"][0:4] # 0 ~ 3 번째 값 까지 ...
            month_of_birth = row["BIRTHDAY"][4:6]
            day_of_birth = row["BIRTHDAY"][6:9]
            if validate_date(row["BIRTHDAY"]) is True :
                birth_datetime = year_of_birth + '-' + month_of_birth + '-' + day_of_birth + ' 00:00:00'
            else :
                birth_datetime = None
            if row["RESNO2"][0:1] in ('5', '6', '7' ,'8'):
                race_concept_id = 38003564 
            elif row["RESNO2"][0:1] in ('0', '1', '2', '3', '4' ,'9'):
                race_concept_id = 38003585
            ethnicity_concept_id = 0
            location_id = 0
            provider_id = 0
            care_site_id = 0
            person_source_value = row["PATNO"]
            gender_source_value = row["SEX"]
            gender_source_concept_id = 0
            race_source_value = row["RESNO2"][0:1]
            race_source_concept_id = 0
            ethnicity_source_value = None
            ethnicity_source_concept_id = 0
            zipcode = row["ZIPCODE"]
            if row["DIEDATE"] == None :
                diedate = None
            elif validate_date(row["DIEDATE"]) is True :
                diedate = row["DIEDATE"]
            else : 
                diedate = None
            height = row["HEIGHT"]
            weight = row["WEIGHT"]
            abotyp = row["ABOTYP"]
            rhtyp = row["RHTYP"]
            birthday = row["BIRTHDAY"]
            resno1 = row["RESNO1"]

            tmp = (person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth, birth_datetime, race_concept_id, ethnicity_concept_id, location_id, provider_id, care_site_id, person_source_value, gender_source_value, gender_source_concept_id, race_source_value, race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id, zipcode, diedate, height, weight, abotyp, rhtyp, birthday, resno1)
            result_data.append(tmp)
            cnt += 1
        
        sql = """
            insert into cdmpv531.person
            (person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth, birth_datetime, race_concept_id, ethnicity_concept_id, location_id, provider_id, care_site_id, person_source_value, gender_source_value, gender_source_concept_id, race_source_value, race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id, zipcode, diedate, height, weight, abotyp, rhtyp, birthday, resno1)
            values %s;
            """
        execute_values(postgresql_cursor, sql, result_data)
        postgresql_connection.commit()