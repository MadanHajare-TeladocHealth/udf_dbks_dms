import pandas as pd
import sqlalchemy

from dbks_db_cred.cred_mgr import DbksCredMgr
from meta_fw.services.meta_config import MetaFrameworkConfig
from user_config.user_config_mgr import UserConfig, ClsLdType
from dbks_dbms.stg_tbl_mgr.redshift_tbl_mgr import RedshiftTblMgr
from dbks_dbms.db_settings import redshift_settings as settings
from db import redshift_connect_mgr
from common.logmgr import get_logger
from pyspark.sql import SparkSession
import redshift_connector

main_logger = get_logger()


class RedshiftWriter:
    redshift_upsert_schema = 'data_sciences'
    redshift_upsert_proc_nm = 'py_dms_upsert_table'
    redshift_upd_tgt_hash_proc_nm = 'py_dms_upd_tgt_row_hash'

    def __init__(self,
                 cred_obj: DbksCredMgr,
                 tbl_obj: RedshiftTblMgr,
                 user_conf_obj: UserConfig, spark_obj: SparkSession):

        self.tgt_db = user_conf_obj.tgt_db
        self.tgt_port = user_conf_obj.tgt_port

        self.tgt_user = cred_obj.redshift_user
        self.tgt_pass = cred_obj.redshift_pass

        self.stg_sch = tbl_obj.stg_sch
        self.stg_tbl = tbl_obj.stg_tbl

        # self.tgt_sch = mfc_obj.ms_obj.tgt_schema
        # self.tgt_tbl = mfc_obj.ms_obj.tgt_tbl_nm
        self.tgt_sch = user_conf_obj.tgt_sch
        self.tgt_tbl = user_conf_obj.tgt_tbl

        self.ld_type = user_conf_obj.ld_type
        self.key_cols_list = user_conf_obj.key_cols_list
        self.part_rec_cnt = user_conf_obj.part_rec_count

        self.spark = spark_obj
        self.tgt_conn_engine = self.get_sql_alchemy_engine()

    def write_data_to_redshift_tbl(self, extracted_df):
        conn = self.get_sql_alchemy_engine()

        if self.ld_type.upper() in [ClsLdType.APPEND, ClsLdType.TRUNC_AND_LOAD]:
            # self.drop_stage_table_before_load()
            self._load_df_to_redshift_stg_tbl(extracted_df)
            main_logger.info("Provisioning data load from dbks to stage table completed")
            df_cnt = pd.read_sql(f"select count(*) as cnt from {self.stg_sch}.{self.stg_tbl}", conn)
            main_logger.info(f"Record count after data load in table "
                             f"{self.stg_sch}.{self.stg_tbl} {df_cnt.to_dict('records')}")
            self._upsert_stg_to_tgt()

        else:
            raise Exception(f"Unhandled data load type :{self.ld_type}. valid load types :{ClsLdType.__dict__}")

    def _load_df_to_redshift_stg_tbl(self, extracted_df):
        main_logger.info(
            f"Writing df with partitions {extracted_df.rdd.getNumPartitions()} to table {self.stg_sch}.{self.stg_tbl}")
        try:
            main_logger.info(f"writing to table {self.tgt_db}:{self.tgt_port}/{self.stg_sch}.{self.stg_tbl}")
            # self.spark.sql("DROP TABLE IF EXISTS stg_ld_dbks_agg_fin_monthly_history_18232")
            # main_logger.info(self.spark.sql(f"desc {self.stg_sch}.{self.stg_tbl}"))
            # extracted_df.show(10)
            extracted_df.write.format("com.databricks.spark.redshift") \
                .option("url", f"jdbc:redshift://{self.tgt_db}:{self.tgt_port}/{settings.DEF_DB}") \
                .option("user", self.tgt_user) \
                .option("password", self.tgt_pass) \
                .option("dbtable", f"{self.stg_sch}.{self.stg_tbl}") \
                .option("tempdir", settings.tempS3Dir) \
                .option('aws_iam_role', settings.RS_SETUP_TEMP_S3IAM) \
                .option("autoenablessl", "false") \
                .option("tempformat", "csv") \
                .option("nullString", "") \
                .option("nullInt", "") \
                .mode("append") \
                .save()
            main_logger.info(f"Data successfully written into table {self.stg_sch}.{self.stg_tbl}")
        except Exception as e:
            main_logger.error(f"Error in writing data from df to  {self.stg_sch}.{self.stg_tbl}")
            raise e

    # def write_data_to_redshift_tbl(self, extracted_df):
    #     conn = self.get_tgt_conn()
    #     if self.ld_type.upper() in ["APPEND", "TRUNC_AND_LOAD"]:
    #         self._load_df_to_redshift_stg_tbl(extracted_df)
    #         main_logger.info("Provisioning data load from dbks to stage table completed")
    #         df_cnt = pd.read_sql(f"select count(*) as cnt from {self.stg_sch}.{self.stg_tbl}", conn)
    #         print(f"Record count after data load in table {self.stg_sch}.{self.stg_tbl} {df_cnt.to_dict('records')}")
    #         self._upsert_stg_to_tgt(conn)
    #     else:
    #         raise Exception(f"Unhandled data load type :{self.ld_type}")

    # def _upsert_stg_to_tgt(self, conn):
    #     _upsert_keys = ",".join(self.key_cols_list)
    #     _proc_schema = settings.DEF_REDSHIFT_PROC_SCHEMA
    #     # Define cursor
    #     cur = conn.cursor()
    #
    #     upsert_proc_args = f"'{self.tgt_sch}','{self.tgt_tbl}'," \
    #                        f"'{_upsert_keys}','{self.stg_sch}','{self.stg_tbl}'"
    #     proc_to_exe_with_args = f"""CALL {_proc_schema}.{self.redshift_upsert_proc_nm}({upsert_proc_args})"""
    #     main_logger.info(f"Executing {proc_to_exe_with_args}")
    #     cur.execute(proc_to_exe_with_args)
    #     conn.commit()
    #
    #     main_logger.info(f"Committed transaction after Executing {proc_to_exe_with_args}")
    #     dq = conn.notices
    #     redshift_connect_mgr.log_redshift_conn_resp(dq)

    def _upsert_stg_to_tgt(self):
        main_logger.info(f"Starting upsert operation between "
                         f"stg:{self.stg_sch}.{self.stg_tbl} and tgt: {self.tgt_sch}.{self.tgt_tbl} "
                         f"in {self.tgt_db}:{self.tgt_port}")
        # conn=get_mysql_conn(env)
        if self.ld_type.upper() == ClsLdType.TRUNC_AND_LOAD:
            self.delete_all_rec_in_tbl(self.tgt_sch, self.tgt_tbl)
        else:
            main_logger.info(f"Skipped target table truncation with load type :{self.ld_type}")

        tgt_alias = f"{self.tgt_sch}.{self.tgt_tbl}"
        stg_alias = f"{self.stg_sch}.{self.stg_tbl}"
        join_key_cond_str = " and ".join([f"{tgt_alias}.{_}={stg_alias}.{_}" for _ in self.key_cols_list])

        del_sql = f"delete from {self.tgt_sch}.{self.tgt_tbl} " \
                  f" using {self.stg_sch}.{self.stg_tbl} where {join_key_cond_str}"
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

    def get_sql_alchemy_engine(self):
        engine: sqlalchemy.Engine = redshift_connect_mgr.get_redshift_conn(host=self.tgt_db,
                                                                           port=self.tgt_port,
                                                                           db_user=self.tgt_user,
                                                                           db_password=self.tgt_pass
                                                                           )
        return engine

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

    # def drop_stage_table_before_load(self):
    #     with self.tgt_conn_engine.connect() as connection:
    #         try:
    #             trans = connection.begin()
    #             main_logger.info(f"Dropping stage table :{self.stg_sch}.{self.stg_tbl}")
    #             drop_sql = f"Drop table if exists {self.stg_sch}.{self.stg_tbl}"
    #             result = connection.execute(sqlalchemy.text(drop_sql))
    #             trans.commit()
    #         except Exception as e:
    #             # connection.rollback()
    #             trans.rollback()
    #             main_logger.error(f"Error in upsert operation between {self.stg_sch}.{self.stg_tbl} "
    #                               f"and {self.tgt_sch}.{self.tgt_tbl}")
    #             raise e
    #     main_logger.info("Stage table dropped before loading new delta load")
