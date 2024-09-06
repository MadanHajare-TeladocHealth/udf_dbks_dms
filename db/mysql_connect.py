import csv
import sys

import pandas as pd
# import mysql.connector
# from sqlalchemy import create_engine
import sqlalchemy
import pymysql
import os

# from dev_setup import db_env_settings
from db import db_settings
from common.logmgr import get_logger

main_logger = get_logger()

def_user = os.environ.get('db_user')
def_pass = os.environ.get('db_pass')


def tunnel_db_decorator(func):
    main_logger.info("Inside decorator")

    def set_tunnel_db_for_developer_connection(*args, **kargs):
        main_logger.info(f"Configured host and port for meta connection {kargs['host']}:{kargs['port']}")
        if sys.platform.upper().startswith("WIN") and kargs["host"] == db_settings.MYSQL_PROD_PRIMARY_DIRECT_HOST:
            kargs["host"] = db_settings.MYSQL_PROD_PRIMARY_TUNNEL_HOST
            kargs["port"] = db_settings.MYSQL_PROD_PRIMARY_TUNNEL_PORT
            main_logger.info(f"Setting tunnel host for connection from local WINDOWS")
        else:
            main_logger.info(f"Meta connection host and port not overwritten in platform {sys.platform}")
        main_logger.info(f"Host and port for meta connection {kargs['host']}:{kargs['port']}")
        return func(*args, **kargs)

    return set_tunnel_db_for_developer_connection


@tunnel_db_decorator
def get_mysql_alchemy_engine(host, port, ip_user=None, ip_password=None, default_db=None):
    db_user = def_user if ip_user is None else ip_user
    db_pass = def_pass if ip_password is None else ip_password
    #
    if None in [db_pass, db_user] or '' in [db_pass, db_user]:
        raise Exception(f"DB user or password is None {db_user}/{db_pass}")
    main_logger.info(f"Connecting {host}:{port}:{str(db_user)}:{len(str(db_pass)) * '*'}")

    engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{host}:{port}/{default_db}")

    if check_mysql_connection(conn_obj=engine):
        main_logger.info(f"Connection string 'mysql+pymysql://{db_user}:{len(db_pass)*'*'}@{host}:{port}/{default_db}'")
    main_logger.info(f"Mysql connection using Sql alchemy engine :{engine} created")

    return engine


def check_mysql_connection(conn_obj):
    try:
        sql = "select concat(USER(),' connection success') as status"
        main_logger.info(pd.read_sql(sql, conn_obj.raw_connection()).to_dict('records'))
        return True
    except Exception as e:
        print(f"Connection failed")
        return False


def get_pymysql_conn(host, port, ip_user=None, ip_password=None, default_db=None):
    db_user = def_user if ip_user is None else ip_user
    db_pass = def_pass if ip_password is None else ip_password

    cnx = pymysql.connect(
        host=host, port=int(port),
        user=db_user,
        passwd=db_pass,
        database=default_db
    )
    main_logger.info(f"Mysql connection using pymysql :{cnx} created")


def sql_to_csv(conn, sql, csv_file=None):
    df = pd.read_sql(sql, conn)

    if csv_file is not None:
        df.to_csv(csv_file, index=False)
        main_logger.info(f"csv_file :{csv_file}")
    else:
        main_logger.info(f"No CSV file provided for sample df with shape {df.shape}:\n{df.to_dict()}")
    return df


def get_columns_from_table(conn, schema_name, table_name, filter_data_type=None):
    dt_filter = '' if filter_data_type is None else f"and DATA_TYPE in ({filter_data_type}) "

    _dtype_sql = f"""select lower(COLUMN_NAME) as COLUMN_NAME, DATA_TYPE from information_schema.COLUMNS c
        where TABLE_SCHEMA ='{schema_name}'
        and TABLE_NAME ='{table_name}'
        {dt_filter}"""
    df = pd.read_sql(_dtype_sql, conn)
    col_list = df["COLUMN_NAME"].values.tolist()

    return col_list


def sql_to_dsv(sql, src_schema, src_table, src_conn, dsv_file=None, delimiter=",", header=True, enclose_quotes='"'):
    try:
        df = pd.read_sql(sql, src_conn)
    except Exception as e:
        main_logger.error(f"Error in executing sql :{sql}")
        raise e

    int_col_list = get_columns_from_table(src_conn, src_schema, src_table,
                                          filter_data_type="'int','smallint','tinyint','bigint'")

    if dsv_file is not None:
        # df = df.fillna("\\x00\\x01\\x02")
        df = df.fillna("\\n\\n")
        df.columns = [_.lower() for _ in df.columns]
        # main_logger.info(f"Workaround for int as float dtype issue. Stripping .0 in int columns :{col_list}")
        for each_col in int_col_list:
            df[each_col] = df[each_col].apply(lambda x: str(x).replace('.0', ''))

        df.to_csv(dsv_file, header=header, quoting=csv.QUOTE_MINIMAL, sep=delimiter, index=False, chunksize=10000)
        main_logger.info(f"Spooled data to delimited file :{dsv_file}")
    else:
        raise Exception(f"No dsv file provided for writing df :{df.count()}")
    return dsv_file


def get_edw_mysql_env_host_port(mysql_env):
    if mysql_env.lower() == "uat":
        host = db_settings.MYSQL_HA_HOST
        port = db_settings.MYSQL_HA_PORT
    elif mysql_env.lower() == "prod":
        host = db_settings.MYSQL_PROD_PRIMARY_DIRECT_HOST
        port = db_settings.MYSQL_PROD_PRIMARY_DIRECT_PORT
    elif mysql_env.lower() == "dev":
        host = db_settings.MYSQL_DEV_HOST
        port = db_settings.MYSQL_DEV_PORT
    else:
        raise Exception("Invalid mysql environment to get host and port")
    main_logger.info(f"Host and port for env:{mysql_env} :{host},{port}")
    return host, port


def reconnect_meta_conn(conn_obj):
    main_logger.info(f"Trying to reconnect object :{type(conn_obj)}")
    # import ipdb; ipdb.set_trace()
    if type(conn_obj) == pymysql.Connect:
        if conn_obj.open:
            main_logger.info("The pymysql connection is open")
        else:
            main_logger.info("The pymysql connection is closed. Reconnecting")
            conn_obj.ping(reconnect=True)
    elif type(conn_obj) == sqlalchemy.engine.base.Connection:
        main_logger.info(f"Type of engine {type(conn_obj.engine)}")
        conn_obj.engine.dispose()
        conn_obj.engine.connect()
    else:
        raise Exception(f"Unhandled db connection type :{conn_obj}")


# if __name__ == '__main__':
#     conn = get_local_conn()
#     schema_name = "DW"
#     table_name = 'denorm_survey_response'
#     sql = f"""select * from {schema_name}.{table_name} where denorm_survey_response_key in (3127902,3649200,7769722,7428692,7034571,6218520,6235428)    """
#     main_logger.info(sql)
# dat_file = Path(conf.DATA_LOCAL_PATH).joinpath("dim_members_ext_data.txt").as_posix()

# df = sql_to_csv(conn=conn, sql=sql, csv_file=dat_file, delimiter='|', header=True)

# df_t = transpose_df(df)
# col_list = get_columns_from_table(conn=conn, schema_name=schema_name, table_name=table_name)
# sql_to_dsv(sql, schema_name, table_name, conn, dsv_file="sample.txt", delimiter=",", header=True,
#            enclose_quotes='"')
