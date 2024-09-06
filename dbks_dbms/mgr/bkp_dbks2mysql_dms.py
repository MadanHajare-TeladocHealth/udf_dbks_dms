import math
from pyspark.sql import SparkSession
from common.logmgr import get_logger
from db import mysql_connect
import pandas as pd
import sqlalchemy

from dbks_dbms.stg_tbl_mgr.mysql_tbl_mgr import MysqlTblMgr
from meta_fw.services.meta_config import MetaFrameworkConfig
from user_config.user_config_mgr import UserConfig

main_logger = get_logger()


class Dbks2MysqlDmsMgrAttr:
    src_sql_key = "src_sql"
    src_sch_key = "src_sch"
    src_tbl_key = "src_tbl"
    tgt_sch_key = "tgt_sch"
    tgt_tbl_key = "tgt_tbl"
    dt_col_key = "dt_col"
    st_dt_key = "st_dt"
    end_dt_key = "end_dt"
    ld_type_key = 'load_type'
    user_dt_flg_key = "user_dt_flg"
    tgt_db_key = "tgt_db"
    tgt_port_key = "tgt_port"
    tgt_user_key = "tgt_user"
    tgt_pass_key = "tgt_pass"
    key_cols_key = "key_cols"


class Dbks2MysqlDmsMgr:
    """ Databricks loader manager class is for managing all the inputs required to perform the data migration tasks"""

    def __init__(self, ip_dict, user_conf_obj: UserConfig
                 , meta_conf_obj: MetaFrameworkConfig
                 , tbl_obj: MysqlTblMgr):
        self.src_sql = user_conf_obj.cust_src_sql
        # self.src_sch = user_conf_obj.src_sch
        self.src_sch = meta_conf_obj.ms_obj.src_sch
        self.src_tbl = user_conf_obj.src_tbl
        self.dt_col = user_conf_obj.user_dt_col
        self.user_dt_flg = user_conf_obj.user_dt_flg
        self.ld_type = user_conf_obj.ld_type
        self.key_cols_list = user_conf_obj.key_cols_list
        self.part_rec_cnt = user_conf_obj.part_rec_count
        self.tgt_sch = meta_conf_obj.ms_obj.tgt_sch
        self.tgt_db = meta_conf_obj.ms_obj.tgt_db
        self.tgt_port = meta_conf_obj.ms_obj.tgt_port
        self.tgt_tbl = meta_conf_obj.ms_obj.tgt_tbl

        self.tgt_user = ip_dict[Dbks2MysqlDmsMgrAttr.tgt_user_key]
        self.tgt_pass = ip_dict[Dbks2MysqlDmsMgrAttr.tgt_pass_key]

        self.stg_sch = tbl_obj.stg_sch
        self.stg_tbl = tbl_obj.stg_tbl

        self.tbl_mgr_obj = tbl_obj

        self.tgt_conn_engine: sqlalchemy.engine = self.get_tgt_conn()

        # self.dbks_master_url = master_url
        self.spark = self.get_spark_obj()
        self._get_dbks_tbl_exists_flag(self.src_sch, self.src_tbl)

        if self.user_dt_flg:
            self.st_dt = ip_dict[Dbks2MysqlDmsMgrAttr.st_dt_key]
            self.end_dt = ip_dict.get(Dbks2MysqlDmsMgrAttr.end_dt_key)

            if self.end_dt is None:
                self.end_dt = self._get_end_dt_from_src_tbl()
            else:
                main_logger.info(f"End date for extraction :{self.end_dt}")
        else:
            self.st_dt = self._get_start_dt_from_tgt_tbl()
            self.end_dt = self._get_end_dt_from_src_tbl()

        self.src_sql = self._prepare_src_sql(rec_limit=None)

    @staticmethod
    def get_spark_obj(app_name="dbks2mysql_loader"):
        main_logger.info(f"Creating spark session with app name :{app_name}")
        spark = SparkSession.builder.getOrCreate()
        return spark

    @staticmethod
    def _get_dbks_tbl_exists_flag(src_sch, src_tbl):
        _dbks_tbl_check_sql = f"SHOW TABLES IN {src_sch} like '" + src_tbl + "'"
        main_logger.info(f"SQL to check databricks table :{_dbks_tbl_check_sql}")
        spark = SparkSession.builder.getOrCreate()
        res = [_.asDict() for _ in spark.sql(_dbks_tbl_check_sql).limit(5).collect()]

        if len(res) == 1:
            main_logger.info(
                f"Table {res[0]['database']}.{res[0]['tableName']} exists. source tbl validation completed")
            return True
        else:
            main_logger.info(f"Exact table not found for given {src_sch}.{src_tbl}. {res}")
            return False

    def _get_end_dt_from_src_tbl(self):
        sql = f"select date_format(max({self.dt_col}),'yyyy-MM-dd HH:mm:ss' ) as end_dt " \
              f"from {self.src_sch}.{self.src_tbl}"
        main_logger.info(f"Get end data for data extraction from source table :{self.src_sch}.{self.src_tbl}")
        df = self.spark.sql(sql)
        pd_df = df.toPandas()
        res_dict_list = pd_df.to_dict('records')
        if len(res_dict_list) > 0:
            end_dt_dict = res_dict_list[0]
            main_logger.info(end_dt_dict['end_dt'])
            return end_dt_dict['end_dt']
        else:
            return '9999-12-31'

    def _prepare_src_sql(self, rec_limit=10000):
        if self.src_sql in [None, ""]:

            filter_cond = f" and {self.dt_col} between '{self.st_dt}' and '{self.end_dt}' "
            rec_limit_clause = f" limit {rec_limit}" if rec_limit is not None else ' '
            src_sql = f"""
            select * from 
            {self.src_sch}.{self.src_tbl} 
            where 1=1
            {filter_cond}
            {rec_limit_clause} 
            """
            main_logger.info(f"Source sql prepared for extraction :{src_sql}")
        else:
            main_logger.info(f"Overriding sql with user provided sql {self.src_sql}")
            src_sql = self.src_sql
        return src_sql

    def _get_ext_count(self):
        main_logger.info(f"Getting rec count for sql  :{self.src_sql}")
        main_logger.info(f"Spark object :{self.spark}")
        df = self.spark.sql(self.src_sql)
        main_logger.info(f"Count of records for data extraction: {df.count()}")
        return df.count()

    def get_number_of_partitions(self):
        rec_count = self._get_ext_count()
        est_partition = math.ceil(rec_count / self.part_rec_cnt)
        main_logger.info(f"Number of records in databricks table {self.src_sch}.{self.src_tbl}: {rec_count} "
                         f"estimated partitions: {est_partition}")
        return est_partition

    def provision_data_load(self):
        if self.ld_type.upper() in ["APPEND", "TRUNC_AND_LOAD"]:
            main_logger.info("Provisioning data load from dbks to stage table")
            # self.tbl_mgr_obj.drop_and_create_stg_tbl()

            est_part = self.get_number_of_partitions()
            extracted_df = self.get_source_df(repart_num=est_part)
            self.load_df_to_mysql_tbl(extracted_df)
            main_logger.info("Provisioning data load from dbks to stage table completed")
            df_cnt = pd.read_sql(f"select count(*) as cnt from {self.stg_sch}.{self.stg_tbl}", self.tgt_conn_engine)
            print(f"Record count after data load in table {self.stg_sch}.{self.stg_tbl} {df_cnt.to_dict('records')}")
            #
            # if load_type.upper() == "TRUNC_AND_LOAD":
            #     self.delete_all_rec_in_tbl(self.tgt_sch, self.tgt_tbl)
            #     print(f"Table {tgt_sch}.{tgt_tbl} truncated for load type :{load_type}")
            # conn.close()
            # print(
            #     f"Appending data to target table using upsert operation between {stg_sch}.{stg_tbl} to {tgt_sch}.{tgt_tbl}")
            self.upsert_stg_to_tgt()

        # else:
        #     raise Exception(f"Unhandled data load type :{load_type}")

    def load_df_to_mysql_tbl(self, extracted_df):
        main_logger.info(
            f"Writing df with partitions {extracted_df.rdd.getNumPartitions()} to table {self.stg_sch}.{self.stg_tbl}")
        try:
            main_logger.info(f"writing to table {self.tgt_db}:{self.tgt_port}/{self.stg_sch}.{self.stg_tbl}")
            extracted_df.write \
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{self.tgt_db}:{self.tgt_port}/{self.stg_sch}") \
                .option("dbtable", self.stg_tbl) \
                .option("user", self.tgt_user) \
                .option("password", self.tgt_pass) \
                .option("useSSL", "false") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .mode("append") \
                .save()
            main_logger.info(f"Data successfully written into table {self.stg_sch}.{self.stg_tbl}")
        except Exception as e:
            main_logger.error(f"Error in writing data from df to  {self.stg_sch}.{self.stg_tbl}")
            raise e

    def delete_all_rec_in_tbl(self, sch, tbl):
        # sql=f"truncate table {tgt_sch}.{tgt_tbl}"
        sql = f"delete from {sch}.{tbl} where 1=1"
        # curr = self.tgt_conn.cursor()
        main_logger.info(f"Executing {sql}")
        # with self.tgt_conn_engine.begin() as connection:
        with self.tgt_conn_engine.connect() as connection:
            try:
                trans = connection.begin()
                main_logger.info(f"deleting all records in table table {sch}.{tbl}")
                connection.execute(sqlalchemy.text(sql))
                trans.commit()
            except Exception as e:
                trans.rollback()
                main_logger.error(f"Error in deleting all records in table {sch}.{tbl}")
                raise e

    def get_tgt_conn(self):
        main_logger.info(f"Creating Target connection with default db :{self.stg_sch}")
        tgt_conn_engine = mysql_connect.get_mysql_alchemy_engine(host=self.tgt_db,
                                                                 port=self.tgt_port,
                                                                 ip_user=self.tgt_user,
                                                                 ip_password=self.tgt_pass,
                                                                 default_db=self.stg_sch,
                                                                 )
        return tgt_conn_engine

    def get_source_df(self, repart_num=200):
        df = self.spark.sql(self.src_sql)
        main_logger.info(f"Before repartition, No of partitions: {df.rdd.getNumPartitions()}")
        df = df.repartition(repart_num)
        main_logger.info(f"After repartition , No of Partitions: {df.rdd.getNumPartitions()}")
        return df

    def _get_start_dt_from_tgt_tbl(self):
        _sql = f"select date_format(max({self.dt_col}),'yyyy-MM-dd HH:mm:ss' ) as start_dt" \
               f" from {self.tgt_sch}.{self.tgt_tbl}"
        df = pd.read_sql(_sql, self.tgt_conn_engine)
        rec = df.to_dict('records')
        if len(rec) > 0:
            start_dt = rec[0]['start_dt']
        else:
            start_dt = '1900-01-01'
        return start_dt

    def upsert_stg_to_tgt(self):
        main_logger.info(f"Starting upsert operation between "
                         f"stg:{self.stg_sch}.{self.stg_tbl} and tgt: {self.tgt_sch}.{self.tgt_tbl} "
                         f"in {self.tgt_db}:{self.tgt_port}")
        # conn=get_mysql_conn(env)
        if self.ld_type.upper() == "TRUNC_AND_LOAD":
            self.delete_all_rec_in_tbl(self.tgt_sch, self.tgt_tbl)
        else:
            main_logger.info(f"Skipped target table truncation with load type :{self.ld_type}")
        join_key_cond_str = " and ".join([f"stg.{_}=tgt.{_}" for _ in self.key_cols_list])
        del_sql = f"delete tgt from {self.tgt_sch}.{self.tgt_tbl} as tgt " \
                  f"join {self.stg_sch}.{self.stg_tbl} as stg on {join_key_cond_str}"
        ins_sql = f"insert into {self.tgt_sch}.{self.tgt_tbl} select * from {self.stg_sch}.{self.stg_tbl}"

        # with self.tgt_conn_engine.begin() as connection:
        with self.tgt_conn_engine.connect() as connection:
            try:
                trans = connection.begin()
                main_logger.info(f"Delete step sql :{del_sql}")
                result = connection.execute(sqlalchemy.text(del_sql))
                rows_affected = result.rowcount
                main_logger.info(f"Number of deleted rows in {self.tgt_sch}.{self.tgt_tbl}: {rows_affected}")
                result = connection.execute(sqlalchemy.text(ins_sql))
                rows_affected = result.rowcount
                main_logger.info(f"Number of inserted rows in {self.tgt_sch}.{self.tgt_tbl}: {rows_affected}")
                # connection.commit()
                trans.commit()
            except Exception as e:
                # connection.rollback()
                trans.rollback()
                main_logger.error(f"Error in upsert operation between {self.stg_sch}.{self.stg_tbl} "
                                  f"and {self.tgt_sch}.{self.tgt_tbl}")
                raise e
        main_logger.info("Upsert operation completed successfully")
