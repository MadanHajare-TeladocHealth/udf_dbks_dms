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

        self.tgt_db = tbl_obj.tgt_db
        self.tgt_port = tbl_obj.tgt_port

        self.tgt_user = cred_obj.redshift_user
        self.tgt_pass = cred_obj.redshift_pass

        self.stg_sch = tbl_obj.stg_sch
        self.stg_tbl = tbl_obj.stg_tbl

        # self.tgt_sch = mfc_obj.ms_obj.tgt_schema
        # self.tgt_tbl = mfc_obj.ms_obj.tgt_tbl_nm
        self.tgt_sch = user_conf_obj.tgt_sch
        self.tgt_tbl = user_conf_obj.tgt_tbl

        self.ld_type = 'APPEND'
        self.key_cols_list = user_conf_obj.key_cols_list
        self.part_rec_cnt = 5000

        self.spark = spark_obj
        self.common_columns=None
        self.tgt_conn_engine = tbl_obj.tgt_conn_engine

    def write_data_to_redshift_tbl(self, extracted_df):
        if self.ld_type.upper() in [ClsLdType.APPEND, ClsLdType.TRUNC_AND_LOAD]:
            # self.drop_stage_table_before_load()
            self._load_df_to_redshift_stg_tbl(extracted_df)
            self._upsert_stg_to_tgt()
            main_logger.info("Provisioning data load from dbks to stage table completed")
        else:
            raise Exception(f"Unhandled data load type :{self.ld_type}. valid load types :{ClsLdType.__dict__}")

    def _load_df_to_redshift_stg_tbl(self, extracted_df):
        try:
            main_logger.info(f"Creating table {self.stg_sch}.{self.stg_tbl} ...")
            rs = self.tgt_conn_engine
            #extracted_df=extracted_df.where("length(service_desc)<=256")
            override_column_types=None
            override_column_types = ','.join([col_name + ' VARCHAR(4000)' for col_name , col_type in extracted_df.dtypes if col_type == 'string'])
            rs.create_table_redshift(df=extracted_df, database=rs.database, table=self.stg_sch + '.' + self.stg_tbl, mode='overwrite',format='jdbc',column_types=override_column_types)
            main_logger.info(f"Table {self.stg_sch}.{self.stg_tbl} created successfully")
            
            tgt_empty_df = rs.sql_query(f'select * from {self.tgt_sch}.{self.tgt_tbl} where 1=2')
            self.common_columns =  list(set(extracted_df.columns).intersection(set(tgt_empty_df.columns)))
            main_logger.info(f"Common columns between source and target table : {self.common_columns}")           
        except Exception as e:
            main_logger.error(f"Error in writing data from df to  {self.stg_sch}.{self.stg_tbl}")
            raise e


    def _upsert_stg_to_tgt(self):
        rs = self.tgt_conn_engine
        main_logger.info(f"""Starting upsert operation between
                         stg:{self.stg_sch}.{self.stg_tbl} and tgt: {self.tgt_sch}.{self.tgt_tbl}
                         with key columns : {self.key_cols_list}
                        """)
        
        on_clause = ' and '.join([ f"{rs.database}.{self.tgt_sch}.{self.tgt_tbl}.{_} =  {rs.database}.{self.stg_sch}.{self.stg_tbl}.{_}" for _ in self.key_cols_list])

        upd_set_clause=','.join([f" {_}= {rs.database}.{self.stg_sch}.{self.stg_tbl}.{_} " for _ in self.common_columns])

        ins_cols = ','.join(self.common_columns)
        ins_vals = ','.join([f" {rs.database}.{self.stg_sch}.{self.stg_tbl}.{_} " for _ in self.common_columns])

        query = f"""
        MERGE INTO {rs.database}.{self.tgt_sch}.{self.tgt_tbl}
        USING {rs.database}.{self.stg_sch}.{self.stg_tbl}
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {upd_set_clause}
        WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})
        """

        main_logger.info(f"Executing upsert query : {query}")        
        rs.run_dml_redshift(query=query)        
        main_logger.info("Upsert operation completed successfully")

