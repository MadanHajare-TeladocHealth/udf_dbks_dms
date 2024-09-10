import datetime as dt
import sqlalchemy

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

main_logger = get_logger()


class DbksDmsMgr:
    def __init__(self,step_info,*all_credetails):
        self.step_id = step_info
        self.meta_env = None
  
        self.cred_obj = DbksCredMgr(all_credetails)
        self.meta_db_obj = MetaDbHelper(self.cred_obj.mysql_user,self.cred_obj.mysql_pass,
                                        self.cred_obj.sch,self.cred_obj.mysql_ip,self.cred_obj.mysql_port)
        #self.meta_conf_obj = MetaFrameworkConfig(step_id=self.step_id, meta_conn=self.meta_db_obj.meta_conn_engine,
        #                                         meta_sch=self.meta_db_obj.meta_sch)
        self.user_conf_obj = UserConfig(step_info)
        #self.app_name = settings.dms_spark_app_name

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

        _tgt_sch = self.user_conf_obj.tgt_sch
        _tgt_tbl = self.user_conf_obj.tgt_tbl

        reader_dt_rng_dict = self._get_data_extraction_dt_rng(_tgt_sch, _tgt_tbl)

        extract_df = self.get_source_df(reader_dt_rng_dict)
        self.write_df_to_tgt(source_df=extract_df)

        # self.validate_and_sync_data()

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

    def get_source_df(self, reader_dt_rng_dict):
        _src_db = self.user_conf_obj.src_db_type.lower()
        main_logger.info(f"Reading data from :{_src_db}")
        if _src_db == "databricks":
            dbks_r_obj = DbksSparkReader(dt_rng_ip_dict=reader_dt_rng_dict
                                         , user_conf_obj=self.user_conf_obj)
            source_df = dbks_r_obj.get_dbks_data_df()
            return source_df

        elif _src_db == "mysql":
            mysql_r_obj = MysqlReader(
                cred_obj=self.cred_obj
                , user_conf_obj=self.user_conf_obj
                , spark_obj=self.spark_obj
                , dt_rng_ip_dict=reader_dt_rng_dict
            )
            source_df = mysql_r_obj.read_mysql_table()
            return source_df
        elif _src_db == "redshift":
            redshift_r_obj = RedshiftReader(dt_rng_ip_dict=reader_dt_rng_dict,
                                            user_conf_obj=self.user_conf_obj,
                                            cred_obj=self.cred_obj,
                                            spark_obj=self.spark_obj
                                            )
            source_df = redshift_r_obj.get_redshift_data_df()
            return source_df
        else:
            raise Exception(f"Extraction failed. Unhandled source db : {_src_db}")

    def write_df_to_tgt(self, source_df):
        _tgt_db = self.user_conf_obj.tgt_db_type.lower()

        if _tgt_db == "mysql":

            tbl_obj = MysqlTblMgr(self.meta_conf_obj.ms_obj
                                  , self.user_conf_obj
                                  , self.cred_obj.mysql_user
                                  , self.cred_obj.mysql_pass)

            writer_obj = MysqlWriter(cred_obj=self.cred_obj
                                     , tbl_obj=tbl_obj
                                     , user_conf_obj=self.user_conf_obj
                                     , spark_obj=self.spark_obj)
            writer_obj.write_data_to_mysql_tbl(source_df)

        elif _tgt_db == "redshift":

            rs_tbl_mgr_obj = RedshiftTblMgr(self.meta_conf_obj.ms_obj
                                            , self.user_conf_obj
                                            , self.cred_obj.redshift_user
                                            , self.cred_obj.redshift_pass)

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
