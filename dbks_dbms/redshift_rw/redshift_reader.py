# user = dbutils.secrets.get(scope="de-scope-confg", key="redshift_talend_username")
# pw = dbutils.secrets.get(scope="de-scope-confg", key="redshift_talend_password")
# # Define the JDBC URL for your Redshift cluster
# jdbc_url = "jdbc:redshift://redshift.prod.livongo.com:5439/prod"
#
# # Define the connection properties
# connection_properties = {
#     "user": user,
#     "password": pw,
#     "driver": "com.amazon.redshift.jdbc.Driver"
# }
#
# # Define the table name
# table_name = "select * from data_science_edw.transdim_consult"
#
# # Read data from the Redshift table into a DataFrame
# df = spark.read \
#     .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
#
# # Show the DataFrame
# df.show()
import math
import pandas as pd
from dbks_db_cred.cred_mgr import DbksCredMgr
from meta_fw.services.meta_config import MetaFrameworkConfig
from user_config.user_config_mgr import UserConfig, ClsLdType
from dbks_dbms.db_settings import redshift_settings as settings
from dbks_dbms.db_util import redshift_util
from common.logmgr import get_logger
from pyspark.sql import SparkSession

main_logger = get_logger()


class RedshiftReader:
    def __init__(self,
                 dt_rng_ip_dict,
                 user_conf_obj: UserConfig,
                 cred_obj: DbksCredMgr,
                 spark_obj: SparkSession
                 ):
        self.cust_src_sql = user_conf_obj.cust_src_sql
        self.cred_obj = cred_obj
        self.src_sql = user_conf_obj.cust_src_sql
        # self.src_sch = user_conf_obj.src_sch
        self.src_sch = user_conf_obj.src_sch
        self.src_tbl = user_conf_obj.src_tbl
        self.src_db = user_conf_obj.src_db
        self.src_port = user_conf_obj.src_port

        self.dt_col = user_conf_obj.user_dt_col
        self.user_dt_flg = user_conf_obj.user_dt_flg
        self.key_cols_list = user_conf_obj.key_cols_list
        self.part_rec_cnt = user_conf_obj.part_rec_count
        self.load_type = user_conf_obj.ld_type
        self.spark = spark_obj
        # self._get_redshift_tbl_exists_flag(self.src_sch, self.src_tbl)

        self.st_dt = dt_rng_ip_dict["start_dt"]
        self.end_dt = dt_rng_ip_dict.get("end_dt")

        self.cust_src_sql = self._prepare_src_sql(rec_limit=settings.REC_LIMIT)

    def _prepare_src_sql(self, rec_limit):
        if self.cust_src_sql in [None, ""]:
            if self.load_type == ClsLdType.TRUNC_AND_LOAD:
                filter_cond = ' and 1=1'
            else:
                filter_cond = f" and ({self.dt_col} between '{self.st_dt}' and '{self.end_dt}' )"

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

    # def _get_redshift_tbl_exists_flag(self):
    #     _dbks_tbl_check_sql = f"""
    #     SELECT * FROM pg_tables
    #     where schemaname ='{self.src_sch}'
    #     and tablename ='{self.src_tbl}'
    #     """
    #     main_logger.info(f"SQL to check redshift table :{_dbks_tbl_check_sql}")
    #     spark = SparkSession.builder.getOrCreate()
    #
    #     username=self.cred_obj.redshift_user
    #     password=self.cred_obj.redshift_pass
    #     # jdbc_url = f"jdbc:redshift://redshift.prod.livongo.com:5439/data_science_edw_staging?user={username}&password={password}"
    #     jdbc_url = f"jdbc:redshift://redshift.prod.livongo.com:5439/prod"
    #     tempS3Dir = "s3://livongo-ds-temp"
    #     RS_SETUP_TEMP_S3IAM = 'arn:aws:iam::113931794823:role/Redshift_Databricks_S3'
    #     jdbc_properties = {"user": username,"password": password}
    #     rs_spark = SparkSession.builder.getOrCreate()
    #     # dmes_df = rs_spark.sql("select * from de_currstate_report.daily_member_eligibility_snapshot")
    #
    #     # Read data from a query
    #     df = (rs_spark.read
    #           .format("redshift")
    #           .option("query", "select x, count(*) <your-table-name> group by x")
    #           .option("tempdir", "s3a://<bucket>/<directory-path>")
    #           .option("url", "jdbc:redshift://<database-host-url>")
    #           .option("user", username)
    #           .option("password", password)
    #           .option("forward_spark_s3_credentials", True)
    #           .load()
    #           )
    #
    #     res = [_.asDict() for _ in spark.sql(_dbks_tbl_check_sql).limit(5).collect()]
    #
    #     if len(res) == 1:
    #         main_logger.info(
    #             f"Table {res[0]['database']}.{res[0]['tableName']} exists. source tbl validation completed")
    #         return True
    #     else:
    #         main_logger.info(f"Exact table not found for given {src_sch}.{src_tbl}. {res}")
    #         return False

    def get_redshift_data_df(self):
        main_logger.info("Reading data from dbks to dataframe")
        est_part, src_df = self.get_number_of_partitions()
        extracted_df = self.get_source_with_best_partition(src_df=src_df, repart_num=est_part)
        return extracted_df

    def get_number_of_partitions(self):
        src_df = self._get_src_df()
        rec_cnt = src_df.count()
        main_logger.info(f"Count of records for data extraction: {rec_cnt}")
        est_partition = math.ceil(rec_cnt / self.part_rec_cnt)
        main_logger.info(f"Number of records in databricks table {self.src_sch}.{self.src_tbl}: {rec_cnt} "
                         f"estimated partitions: {est_partition}")
        return est_partition, src_df

    def _get_src_df(self):
        # removing order by to get record count
        cnt_sql = self.cust_src_sql.replace(f"order by {self.dt_col}", '')
        main_logger.info(f"Getting rec count for sql  :{cnt_sql}")

        username = self.cred_obj.redshift_user
        password = self.cred_obj.redshift_pass
        _driver = settings.REDSHIFT_DRIVER
        _iam_role = settings.RS_SETUP_TEMP_S3IAM
        url = f"jdbc:redshift://{self.src_db}:{self.src_port}/{settings.DEF_DB}"

        main_logger.info(f"Driver to connect {_driver}")
        main_logger.info(f"Url to connect {url}")

        df = (self.spark.read
              .format(_driver)
              .option("url", url)
              .option("user", username)
              .option("password", password)
              .option("query", cnt_sql)
              .option("tempdir", settings.tempS3Dir)
              .option('aws_iam_role', _iam_role)
              .option("autoenablessl", "false")
              .load()
              )
        if df.isEmpty():
            main_logger.info(f"source dataframe from table :{self.src_sch}.{self.src_tbl} is empty")
        return df

    @staticmethod
    def get_source_with_best_partition(src_df, repart_num=200):

        df_def_part_cnt = src_df.rdd.getNumPartitions()
        main_logger.info(f"Default dataframe partition count: {df_def_part_cnt}")

        # case to handle if record count is zero
        if df_def_part_cnt < 1:
            repart_num = 1
            main_logger.info(f"default partition :{df_def_part_cnt}. Repartition set to {repart_num}")
            src_df = src_df.repartition(repart_num)

        if df_def_part_cnt < repart_num:
            src_df = src_df.repartition(repart_num)
            main_logger.info(f"Repartitioning to higher Partitions: {src_df.rdd.getNumPartitions()}")
        else:
            main_logger.info(f"Using default partition count:{df_def_part_cnt} to reduce data shuffling ")
        return src_df
