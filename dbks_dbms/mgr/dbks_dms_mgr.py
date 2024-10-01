import datetime as dt
import sqlalchemy
import hashlib

from common.logmgr import get_logger
from dbks_db_cred.cred_mgr import DbksCredMgr

from dbks_dbms.db_util.mysql_util import MysqlPreProc
from dbks_dbms.db_util.dbks_util import DbksPreproc
from dbks_dbms.db_util.redshift_util import RedshiftPreProc

from dbks_dbms.mysql_rw.mysql_reader import MysqlReader
from dbks_dbms.redshift_rw.redshift_reader import RedshiftReader

from dbks_dbms.stg_tbl_mgr.mysql_tbl_mgr import MysqlTblMgr
from dbks_dbms.mysql_rw.mysql_writer import MysqlWriter

from dbks_dbms.stg_tbl_mgr.redshift_tbl_mgr import RedshiftTblMgr
from dbks_dbms.redshift_rw.redshift_writer import RedshiftWriter

from meta_fw.services.meta_config import MetaFrameworkConfig
from meta_fw.services.meta_db_helper import MetaDbHelper

from user_config.user_config_mgr import UserConfig

from dbks_dbms.dbks_rw.dbks_reader import DbksSparkReader

from dbks_dbms import settings
# from dbks_dbms.mgr.spark_mgr import DmsSparkMgr
from pyspark.sql import SparkSession

import pyspark.sql.functions as f

from databricks.sdk.runtime import *

main_logger = get_logger()
print("loading database manager")


class DbksDmsMgr:
    def __init__(self,step_info,all_credetails):
        self.step_id = step_info
        self.meta_env = None
  
        self.cred_obj = DbksCredMgr(all_credetails)
        #self.meta_db_obj = MetaDbHelper(self.cred_obj.mysql_user,self.cred_obj.mysql_pass,
                                       # self.cred_obj.sch,self.cred_obj.mysql_ip,self.cred_obj.mysql_port)
        #self.meta_conf_obj = MetaFrameworkConfig(step_id=self.step_id, meta_conn=self.meta_db_obj.meta_conn_engine,
        #                                         meta_sch=self.meta_db_obj.meta_sch)
        self.user_conf_obj = UserConfig(step_info)
        #self.app_name = settings.dms_spark_app_name
        self.watermark=None
        self.spark_obj = self._get_spark_inst()

    def _get_spark_inst(self):
        #main_logger.info(f"Creating spark session with app name :{self.app_name}")
        #spark = SparkSession.builder.getOrCreate()
        #spk_conf = spark.sparkContext.getConf().getAll()
        # main_logger.info(f"Spark configuration {spk_conf}")
        return spark

    def provision_dms(self):
        if self.user_conf_obj.tgt_db_type in ['mysql', 'redshift', 'databricks'] and \
                self.user_conf_obj.src_db_type in ['mysql', 'redshift', 'databricks']:
            main_logger.info(f"Initiating DMS from data migration from {self.user_conf_obj.src_db_type} to "
                             f"{self.user_conf_obj.tgt_db_type}")
            self._pipeline_router()
            main_logger.info(f"DMS from {self.user_conf_obj.src_db_type} to "
                             f"{self.user_conf_obj.tgt_db_type} completed successfully")
        else:
            raise Exception(f"Unhandled data migration from {self.user_conf_obj.src_db_type} to "
                            f"{self.user_conf_obj.tgt_db_type} failed")

    def _pipeline_router(self):
        try:


            #_tgt_sch = self.user_conf_obj.tgt_sch
            #_tgt_tbl = self.user_conf_obj.tgt_tbl

            #reader_dt_rng_dict = self._get_data_extraction_dt_rng(_tgt_sch, _tgt_tbl)

            extract_df = self.get_source_df()
            #print(f" source rows =  {extract_df.count()} ")
            self.watermark=extract_df.agg(f.when(f.count(f.col("udf_updated_dt")) == 0, None).otherwise(f.max(f.col("udf_updated_dt")))).collect()[0][0]
            self.write_df_to_tgt(source_df=extract_df)
            self._log_dms_status('success','ok')
        except Exception as e:
            #print(f"Exception occurred while executing DMS {e.args[0][1:200]}")
            self._log_dms_status('fail',e.args[0][1:200])
            # self.validate_and_sync_data()
    
    def _log_dms_status(self, status, msg):        
        udf_to_rdbms_sync_key=self.step_id["udf_to_rdbms_sync_key"]
        current_status=status
        watermark_value=self.watermark
        udf_created_dt=dt.datetime.now()
        udf_created_by='UDF_SYNC_FRMWRK'
        msg=msg

        content = udf_to_rdbms_sync_key+'~'+str(udf_created_dt)
        hash_object = hashlib.sha256(content.encode('utf-8'))  
        udf_to_rdbms_sync_log_key = hash_object.hexdigest()


        data=(udf_to_rdbms_sync_log_key,udf_to_rdbms_sync_key,current_status,watermark_value,udf_created_dt,udf_created_dt,udf_created_by,udf_created_by,msg)
        schema = spark.table("udf_internal.udf_to_rdbms_sync_log").schema
        spark.createDataFrame([data],schema).write.mode("append").saveAsTable("udf_internal.udf_to_rdbms_sync_log")
        main_logger.info(f"logged status: {status} with watermark {self.watermark} into table udf_internal.udf_to_rdbms_sync_log")




    def _get_data_extraction_dt_rng(self, _tgt_sch, _tgt_tbl):
        if self.user_conf_obj.user_dt_flg is False:
            # delta load. Get start date based on target table
            main_logger.info(f"Reading max time data loaded in target table :{_tgt_sch}.{_tgt_tbl}")
            delta_start_dt = self.get_tgt_tbl_max_ts()
            delta_end_dt = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            reader_dt_rng_dict = {"start_dt": delta_start_dt,
                                  "end_dt": delta_end_dt}
            main_logger.info(f"Delta date range :{reader_dt_rng_dict}")
        else:
            # using user provided date range for migration
            reader_dt_rng_dict = {"start_dt": self.user_conf_obj.st_dt,
                                  "end_dt": self.user_conf_obj.end_dt}
            main_logger.info(f"Using user provided date range :{reader_dt_rng_dict}")
        return reader_dt_rng_dict

    def get_source_df(self):
        row=self.step_id
        src_db=row["src_db"]
        src_tbl=row["src_tbl"]
        tgt_db_typ=row["tgt_db_typ"]
        tgt_db=row["tgt_db"]
        tgt_tbl=row["tgt_tbl"]
        merge_keys=row["merge_keys"]
        udf_to_rdbms_sync_key=row["udf_to_rdbms_sync_key"]
        
        watermark_value = spark.table("udf_internal.udf_to_rdbms_sync_log").where(f"udf_to_rdbms_sync_key='{udf_to_rdbms_sync_key}'").where("current_status='success'").agg({"watermark_value": "max"}).collect()[0][0]


        filter_condition= "1=1" 
        if watermark_value:
            filter_condition=f.col("udf_updated_dt") > f.lit(watermark_value).cast('timestamp')
        main_logger.info(f"filter condition :{filter_condition}")
        source_df=spark.read.format("delta").load(f"/{src_db}/{src_tbl}").where(filter_condition)
        return source_df

    def write_df_to_tgt(self, source_df):
        _tgt_db = self.user_conf_obj.tgt_db_type.lower()

        if _tgt_db == "mysql":

            tbl_obj = MysqlTblMgr( self.user_conf_obj
                                  , self.cred_obj
                                  )

            writer_obj = MysqlWriter(cred_obj=self.cred_obj
                                     , tbl_obj=tbl_obj
                                     , user_conf_obj=self.user_conf_obj
                                     , spark_obj=self.spark_obj)
            writer_obj.write_data_to_mysql_tbl(source_df)

        elif _tgt_db == "redshift":

            rs_tbl_mgr_obj = RedshiftTblMgr(self.user_conf_obj
                                  , self.cred_obj)

            rs_writer_obj = RedshiftWriter(cred_obj=self.cred_obj
                                           , tbl_obj=rs_tbl_mgr_obj
                                           , user_conf_obj=self.user_conf_obj
                                           , spark_obj=self.spark_obj)

            rs_writer_obj.write_data_to_redshift_tbl(source_df)

        else:
            raise Exception(f"Writing to target db :{_tgt_db} is not implemented yet.")

    def get_tgt_tbl_max_ts(self):
        if str(self.user_conf_obj.tgt_db_type).lower() == 'mysql':
            mysql_host = self.user_conf_obj.tgt_db
            mysql_port = self.user_conf_obj.tgt_port
            mysql_sch = self.user_conf_obj.tgt_sch

            mysql_pp = MysqlPreProc(
                cred_obj=self.cred_obj,
                uc_obj=self.user_conf_obj,
                mysql_host=mysql_host,
                mysql_port=mysql_port,
                mysql_sch=mysql_sch
            )

            tgt_tbl_max_ts = mysql_pp.tbl_audit_col_max_dt

        elif str(self.user_conf_obj.tgt_db_type).lower() == 'databricks':
            _tgt_sch = self.user_conf_obj.tgt_sch
            _tgt_tbl = self.user_conf_obj.tgt_tbl
            _dt_col = self.user_conf_obj.user_dt_col

            main_logger.info("Reading delta start date from target table databricks")
            dbks_pp = DbksPreproc(dbks_sch=_tgt_sch
                                  , dbks_tbl=_tgt_tbl
                                  , audit_dt_col=_dt_col)
            tgt_tbl_max_ts = dbks_pp.audit_col_max_dt
        elif str(self.user_conf_obj.tgt_db_type).lower() == 'redshift':
            redshift_pp = RedshiftPreProc(uc_obj=self.user_conf_obj
                                          , cred_obj=self.cred_obj
                                          , rs_host=self.user_conf_obj.tgt_db
                                          , rs_port=self.user_conf_obj.tgt_port
                                          )
            tgt_tbl_max_ts = redshift_pp.tbl_audit_col_max_dt
        else:
            raise Exception(f"Unhandled target DB type :{self.user_conf_obj.tgt_db_type}")

        tgt_tbl_max_ts = '1900-01-01' if tgt_tbl_max_ts is None else tgt_tbl_max_ts
        return tgt_tbl_max_ts

    def validate_and_sync_data(self):
        srf_cnt_df = self.get_src_cnt_for_validation()
        tgt_cnt_df = self.get_tgt_cnt_for_validation()

    def get_src_cnt_for_validation(self):
        pass

    def get_tgt_cnt_for_validation(self):
        pass
