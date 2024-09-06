import redshift_connector
import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from db import db_settings
from common.logmgr import get_logger

main_logger = get_logger()


def get_redshift_conn(host, port, db_user, db_password, database=db_settings.DEF_REDSHIFT_DB):
    try:
        # conn = redshift_connector.connect(
        #     host=host,
        #     port=int(port),
        #     database=database,
        #     user=db_user,
        #     password=db_password
        # )
        engine = create_engine(
            f"postgresql+psycopg2://{db_user}:{db_password}@{host}:{port}/{database}"
        )
        sql = "select concat(USER(),' connection success') as status"
        df = pd.read_sql(sql="select 'CONNECTION SUCCESS' as status", con=engine)
        if df is not None:
            main_logger.info(f"Connection to redshift success : {host}:{port}@{db_user}/{database}")
            return engine

    except Exception as e:
        main_logger.error(f"Connection failed {host}:{port}:{db_user}:{db_password}")
        raise e


def create_table_like(engine: sqlalchemy.engine, src_sch, src_tbl, tgt_sch, tgt_tbl, drop_stg_flg=False):
    # Define cursor
    # cur = engine.connect().cursor()
    with engine.connect() as connection:
        # drop stage table if required
        # trans = connection.begin()
        main_logger.info(f'Creatingstage table  {src_sch}."{src_tbl}" table like {tgt_sch}."{tgt_tbl}" ')
        drop_stg_table_sql = f'Drop table IF EXISTS {tgt_sch}."{tgt_tbl}" '
        create_stg_tbl_sql = f'create table if not exists {tgt_sch}."{tgt_tbl}" (like {src_sch}."{src_tbl}")'
        if drop_stg_flg:
            try:
                trans = connection.begin()
                connection.execute(drop_stg_table_sql)
                main_logger.info(f"Executing {drop_stg_table_sql}")
                connection.execute(create_stg_tbl_sql)
                main_logger.info(f"Executing {create_stg_tbl_sql}")
                trans.commit()
            except Exception as e:
                main_logger.error(f"Error in creating {src_sch}.{src_tbl} like table {tgt_sch}.{tgt_tbl} ")
                raise e
        else:
            main_logger.info(f"Skipping drop table execution {tgt_sch}.{tgt_tbl}")

    val_sql = f"""
                select TABLE_SCHEMA, TABLE_NAME 
                from information_schema.TABLES t 
                where TABLE_NAME = '{tgt_tbl}' and TABLE_SCHEMA ='{tgt_sch}'"""
    df = pd.read_sql(val_sql, engine)
    # engine.commit()
    main_logger.info(f"Committed stage table created if not exist:{df.to_dict('records')}")

    return tgt_sch, tgt_tbl


def create_table_as(conn, src_sch, src_tbl, tgt_sch, tgt_tbl, sel_col_str, drop_and_create_flg=False):
    """
    Function to create target table as source
    :param conn: common connection for src and target table
    :param src_sch: source table schema
    :param src_tbl: source table
    :param tgt_sch: Target schema in which table to be created
    :param tgt_tbl: Target table to be created like src_tbl
    :param sel_col_str: column name string
    :param drop_and_create_flg: Flag to drop and create
    :return:
    """
    if sel_col_str is None:
        raise Exception("Column names not provided for creating table with only key columns for deletion activity")

    # Define cursor
    cur = conn.cursor()

    # drop stage table if required
    if drop_and_create_flg:
        drop_stg_table_sql = f"Drop table IF EXISTS {tgt_sch}.{tgt_tbl} "
        main_logger.info(f"Executing {drop_stg_table_sql}")
        try:
            cur.execute(drop_stg_table_sql)
        except Exception as e:
            main_logger.error(f"Error in dropping stage table {tgt_sch}.{tgt_tbl} ")
            raise e
    else:
        main_logger.info("Skipping drop stage table execution")

    create_stg_tbl_sql = f'create table {tgt_sch}.{tgt_tbl} as ' \
                         f'select {sel_col_str} from {src_sch}."{src_tbl}" where 1=2'
    main_logger.info(f"Executing {create_stg_tbl_sql}")
    cur.execute(create_stg_tbl_sql)

    val_sql = f"""
                    select TABLE_SCHEMA, TABLE_NAME 
                    from information_schema.TABLES t 
                    where TABLE_NAME = '{src_tbl}' and TABLE_SCHEMA ='{tgt_sch}'"""

    df = pd.read_sql(val_sql, conn)
    conn.commit()
    main_logger.info(f"Committed stage table created if not exist:{df.to_dict('records')}")


def check_reconnect(test_conn):
    main_logger.info(pd.read_sql("select 'CONN_VALID' as status", test_conn))
    test_conn.close()
    try:
        main_logger.info("Closing existing connection and creating new one")
        test_conn.close()
    except Exception as e:
        main_logger.warning(f"Ignoring closing target connection error {e}")

    test_conn = get_redshift_conn(host, port, db_user, db_password)
    main_logger.info(pd.read_sql("select 'RECONN_CONN_VALID' as status", test_conn))


def get_redshift_local_conn():
    import os
    host = '10.10.75.193'
    port = 5439
    db_user = os.environ['redshift_db_user']
    db_password = os.environ['redshift_db_pass']
    _conn = get_redshift_conn(host, port, db_user, db_password)
    return _conn


def get_redshift_tbl_col(conn, schema_nm, tbl_nm):
    _sql = f"""
    select  
    TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,IS_NULLABLE,DATA_TYPE
    ,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE
    ,CHARACTER_MAXIMUM_LENGTH/4 source_length
    from information_schema.COLUMNS c
    where c.TABLE_NAME ='{tbl_nm}'
    and c.TABLE_SCHEMA ='{schema_nm}'
    order by  ORDINAL_POSITION
    """
    df = pd.read_sql(_sql, conn)
    df.columns = [x.decode('latin-1') for x in list(df.columns)]

    tgt_ordered_col_list = df['column_name'].tolist()
    main_logger.debug(f"Target col list : {tgt_ordered_col_list}")
    main_logger.info(f"Number of columns in target :{len(tgt_ordered_col_list)}")
    return tgt_ordered_col_list


def check_if_table_exist_in_redshift_conn(sch_nm, tbl_nm, conn):
    val_sql = f"""
                select TABLE_SCHEMA, TABLE_NAME 
                from information_schema.TABLES t 
                where TABLE_NAME = '{tbl_nm}' and TABLE_SCHEMA ='{sch_nm}'
                and table_type='BASE TABLE'
                """
    df = pd.read_sql(val_sql, conn)
    if df.empty:
        tbl_exists_flg = False

    elif df.shape[0] > 1:
        main_logger.info(f"More than one table table exists {df.to_dict('records')} exists")
        raise Exception(f"Expected one table but found more than one object with name {tbl_nm} in {sch_nm} ")
    else:
        main_logger.info(f"Table {sch_nm}.{tbl_nm} exists. Validation success")
        tbl_exists_flg = True
    return tbl_exists_flg


def get_redshift_add_col_ddl(conn, sch, tbl):
    length_not_req_data_types = ['SMALLINT', 'INTEGER', 'BIGINT', 'REAL', 'BOOLEAN', 'DATE', 'TIMESTAMP', 'TIMESTAMPTZ',
                                 'GEOMETRY', 'GEOGRAPHY', 'TIME', 'TIMETZ', 'VARBYTE', 'DOUBLE PRECISION']
    length_not_req_data_types = ['NUMERIC', 'FLOAT', 'FLOAT4', 'FLOAT8', 'DECIMAL', 'CHAR', 'VARCHAR']
    _redshift_ddl_sql = f"""
            select  
                COLUMN_NAME ,ORDINAL_POSITION 
                ,IS_NULLABLE 
                ,DATA_TYPE 
                ,CHARACTER_MAXIMUM_LENGTH 
                ,NUMERIC_PRECISION 
                ,NUMERIC_SCALE 
                ,CHARACTER_MAXIMUM_LENGTH
                ,concat('alter table {sch}.{tbl} add column ',COLUMN_NAME,' ',DATA_TYPE) as DDL_WITH_DT
                ,concat('(',character_maximum_length,')') as DDL_LEN_SUFFIX
                ,concat('(',NUMERIC_PRECISION,',',NUMERIC_SCALE')') as DDL_N_P_SUFFIX
                from information_schema.COLUMNS c
                where c.TABLE_NAME ='{tbl}'
                 and c.TABLE_SCHEMA ='{sch}'
            """
    main_logger.info(f"Redshift ddl check sql :{_redshift_ddl_sql}")
    df = pd.read_sql(sql=_redshift_ddl_sql, con=conn)
    upper_cols_list = [_.decode('latin-1').upper() for _ in df.columns]
    df.columns = upper_cols_list
    main_logger.info(df.head().to_string())
    return df


def execute_sql_in_redshift(conn, sql_list):
    # Define cursor
    cur = conn.cursor()
    for each_sql in sql_list:
        main_logger.info(f"Executing sql = \n {each_sql}")
        cur.execute(each_sql)
    conn.commit()
    dq = conn.notices
    log_redshift_conn_resp(dq)
    return dq


if __name__ == '__main__':
    import os

    host = '10.10.75.193'
    port = 5439
    db_user = os.environ['redshift_db_user']
    db_password = os.environ['redshift_db_pass']
    _conn = get_redshift_local_conn()
    # check_reconnect(test_conn=conn)
    # create_table_like(conn, 'data_sciences', 'dim_member', 'data_sciences','stg_dim_member_0')
    get_redshift_tbl_col(_conn, schema_nm='data_science_edw', tbl_nm='dim_member')


def log_redshift_conn_resp(dq):
    main_logger.info("------------------Logs from redshift operations ------------------")
    if len(dq) > 0:
        df = pd.DataFrame(dq)
        df.columns = [x.decode('latin-1') for x in list(df.columns)]
        for each_msg in df["M"].apply(lambda x: x.decode('latin-1')).tolist():
            main_logger.info(f"Redshift Logs :{each_msg}")
            main_logger.info("Redshift Logs :----")
    else:
        main_logger.info("No messages in redshift notice deque")
