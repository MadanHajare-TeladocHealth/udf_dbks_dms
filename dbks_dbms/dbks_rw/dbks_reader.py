import math

from meta_fw.services.meta_config import MetaFrameworkConfig
from user_config.user_config_mgr import UserConfig
from dbks_dbms.db_settings import dbks_settings as settings
from common.logmgr import get_logger
import pandas as pd
from pyspark.sql import SparkSession

main_logger = get_logger()


class DbksReaderAttr:
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
    # tgt_db_key = "tgt_db"
    # tgt_port_key = "tgt_port"
    # tgt_user_key = "tgt_user"
    # tgt_pass_key = "tgt_pass"
    # key_cols_key = "key_cols"


class DbksSparkReader:
    def __init__(self,
                 dt_rng_ip_dict,
                 user_conf_obj: UserConfig
                 ):
        self.cust_src_sql = user_conf_obj.cust_src_sql
        self.src_sch = user_conf_obj.src_sch
        self.src_tbl = user_conf_obj.src_tbl
        # self.src_sch = meta_conf_obj.ms_obj.src_schema
        # self.src_tbl = user_conf_obj.src_tbl
        self.dt_col = user_conf_obj.user_dt_col
        self.user_dt_flg = user_conf_obj.user_dt_flg
        self.key_cols_list = user_conf_obj.key_cols_list
        self.part_rec_cnt = user_conf_obj.part_rec_count

        self.spark = self.get_spark_obj()
        self._get_dbks_tbl_exists_flag(self.src_sch, self.src_tbl)

        # if self.user_dt_flg:
        #     self.st_dt = dt_rng_ip_dict["start_dt"]
        #     self.end_dt = dt_rng_ip_dict.get("end_dt")
        #
        #     if self.end_dt is None:
        #         self.end_dt = self._get_end_dt_from_src_tbl()
        #     else:
        #         main_logger.info(f"End date for extraction :{self.end_dt}")
        # else:
        #     self.st_dt = self._get_start_dt_from_tgt_tbl()
        #     self.end_dt = self._get_end_dt_from_src_tbl()

        self.st_dt = dt_rng_ip_dict["start_dt"]
        self.end_dt = dt_rng_ip_dict.get("end_dt")

        self.cust_src_sql = self._prepare_src_sql(rec_limit=settings.REC_LIMIT)

    @staticmethod
    def get_spark_obj(app_name="dbks_dms"):
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

    # def _get_end_dt_from_src_tbl(self):
    #     sql = f"select date_format(max({self.dt_col}),'yyyy-MM-dd HH:mm:ss' ) as end_dt " \
    #           f"from {self.src_sch}.{self.src_tbl}"
    #     main_logger.info(f"Get end data for data extraction from source table :{self.src_sch}.{self.src_tbl}")
    #     df = self.spark.sql(sql)
    #     pd_df = df.toPandas()
    #     res_dict_list = pd_df.to_dict('records')
    #     if len(res_dict_list) > 0:
    #         end_dt_dict = res_dict_list[0]
    #         main_logger.info(end_dt_dict['end_dt'])
    #         return end_dt_dict['end_dt']
    #     else:
    #         return '9999-12-31'
    #
    # def _get_start_dt_from_tgt_tbl(self):
    #     _sql = f"select date_format(max({self.dt_col}),'yyyy-MM-dd HH:mm:ss' ) as start_dt" \
    #            f" from {self.tgt_sch}.{self.tgt_tbl}"
    #     df = pd.read_sql(_sql, self.tgt_conn_engine)
    #     rec = df.to_dict('records')
    #     if len(rec) > 0:
    #         start_dt = rec[0]['start_dt']
    #     else:
    #         start_dt = '1900-01-01'
    #     return start_dt

    def _prepare_src_sql(self, rec_limit):
        if self.cust_src_sql in [None, ""]:

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
            main_logger.info(f"Overriding sql with user provided sql {self.cust_src_sql}")
            src_sql = self.cust_src_sql
        return src_sql

    def get_dbks_data_df(self):
        main_logger.info("Reading data from dbks to dataframe")
        est_part = self.get_number_of_partitions()
        extracted_df = self.get_source_df(repart_num=est_part)
        return extracted_df

    def get_number_of_partitions(self):
        rec_count = self._get_ext_count()
        est_partition = math.ceil(rec_count / self.part_rec_cnt)
        main_logger.info(f"Number of records in databricks table {self.src_sch}.{self.src_tbl}: {rec_count} "
                         f"estimated partitions: {est_partition}")
        return est_partition

    def _get_ext_count(self,):
        cnt_sql = self.cust_src_sql.replace(f"order by {self.dt_col}", '')
        main_logger.info(f"Getting rec count for sql  :{cnt_sql}")
        main_logger.info(f"Spark object :{self.spark}")
        df = self.spark.sql(cnt_sql)
        main_logger.info(f"Count of records for data extraction: {df.count()}")
        return df.count()

    def get_source_df(self, repart_num=200):
        df = self.spark.sql(self.cust_src_sql)
        df_def_part_cnt = df.rdd.getNumPartitions()
        main_logger.info(f"Default dataframe partition count: {df_def_part_cnt}")

        # case to handle if record count is zero
        if df_def_part_cnt < 1:
            repart_num = 1
            main_logger.info(f"default partition :{df_def_part_cnt}. Repartition set to {repart_num}")
            df = df.repartition(repart_num)

        if df_def_part_cnt < repart_num:
            df = df.repartition(repart_num)
            main_logger.info(f"Repartitioning to higher Partitions: {df.rdd.getNumPartitions()}")
        else:
            main_logger.info(f"Using default partition count:{df_def_part_cnt} to reduce data shuffling ")
        return df
