import pandas as pd
import pyspark.sql

from common.logmgr import get_logger
from dbks_db_cred.cred_mgr import DbksCredMgr
from dbks_dbms.db_settings import mysql_settings as settings
from meta_fw.services.meta_config import MetaFrameworkConfig
from user_config.user_config_mgr import UserConfig, ClsLdType
from db.mysql_connect import get_mysql_alchemy_engine

main_logger = get_logger()


class MysqlReader:
    def __init__(self,
                 cred_obj: DbksCredMgr,
                 user_conf_obj: UserConfig,
                 spark_obj: pyspark.sql.SparkSession,
                 dt_rng_ip_dict: dict):

        # self.user_conf_obj = user_conf_obj
        # self.mfc_obj = mfc_obj
        self.dt_col = user_conf_obj.user_dt_col
        self.cust_src_sql = user_conf_obj.cust_src_sql
        self.db_user = cred_obj.mysql_user
        self.db_pass = cred_obj.mysql_pass

        self.src_tbl = user_conf_obj.src_tbl
        self.src_sch = user_conf_obj.src_sch

        self.ld_type = user_conf_obj.ld_type
        self.key_cols_list = user_conf_obj.key_cols_list

        self.src_host = user_conf_obj.src_db
        self.src_port = user_conf_obj.src_port
        self.src_sch = user_conf_obj.src_sch
        self.src_pk_col = user_conf_obj.src_pk_col

        self.part_size_mb = user_conf_obj.part_size_mb

        self.spark_obj = spark_obj

        self.st_dt = dt_rng_ip_dict["start_dt"]
        self.end_dt = dt_rng_ip_dict.get("end_dt")

        self.cust_src_sql = self._prepare_src_sql(rec_limit=settings.REC_LIMIT)
        self.mysql_conn_engine = self.get_mysql_conn_engine()

    def read_mysql_table(self):
        driver = "org.mariadb.jdbc.Driver"

        url = f"jdbc:mysql://{self.src_host}:{self.src_port}/{self.src_sch}"
        main_logger.info(f"Source data url :{url}")

        partition_config = self.get_partition_config()

        mysql_source_df = self.spark_obj.read \
            .format("jdbc") \
            .option("driver", driver) \
            .option("url", url) \
            .option("dbtable", f"({self.cust_src_sql}) as src_sql") \
            .option("user", self.db_user) \
            .option("password", self.db_pass) \
            .option("numPartitions", partition_config["numPartitions"]) \
            .option("partitionColumn", partition_config["partitionColumn"]) \
            .option("lowerBound", partition_config["lowerBound"]) \
            .option("upperBound", partition_config["upperBound"]) \
            .load()
        # mysql_source_df.show()
        if mysql_source_df.isEmpty():
            main_logger.info(f"source dataframe from table :{self.src_sch}.{self.src_tbl} is empty")
        else:
            main_logger.info(f"Source dataframe created with schema :{mysql_source_df.printSchema()}")

        return mysql_source_df

    def _prepare_src_sql(self, rec_limit):
        if self.cust_src_sql in [None, ""]:
            if self.ld_type == ClsLdType.TRUNC_AND_LOAD:
                filter_cond = ' and 1=1'
            else:
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

    def get_read_partition_col(self):
        _get_auto_id_col_sql = f"""
        SELECT COLUMN_NAME as auto_incr_col 
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{self.src_tbl}'
            and TABLE_SCHEMA = '{self.src_sch}'
            AND IS_NULLABLE = 'NO'
            AND INSTR(EXTRA , 'auto_increment') > 0
    """

        if self.src_pk_col is None:
            main_logger.info(f"executing sql to get the primary key column :{_get_auto_id_col_sql}")
            auto_id_df = pd.read_sql(sql=_get_auto_id_col_sql, con=self.mysql_conn_engine)
            if auto_id_df.empty:
                raise Exception(
                    "Int Primary key column neither defined in the source table nor provided in the user config")
            else:
                main_logger.info(auto_id_df.to_string())
                partition_col = auto_id_df["auto_incr_col"][0]
        else:
            partition_col = self.src_pk_col
            main_logger.info(f"Using user defined column {partition_col} for reading from source in parallel ")

        main_logger.info(f"Partition column to read :{partition_col}")
        return partition_col

    def get_min_max_rng_from_src_tbl(self, partition_key_col):

        _min_max_sql = f"""select min({partition_key_col}) lower_limit
                            ,max({partition_key_col}) upper_limit
                            ,count(*) tbl_cnt from ({self.cust_src_sql}) as a"""
        main_logger.info(f"Sql to get min, max and cnt of table :{_min_max_sql}")
        min_max_df = pd.read_sql(_min_max_sql, con=self.mysql_conn_engine)

        lower_limit = min_max_df['lower_limit'][0]
        upper_limit = min_max_df['upper_limit'][0]
        tbl_cnt = min_max_df['tbl_cnt'][0]

        return lower_limit, upper_limit, tbl_cnt

    def get_partition_config(self):
        read_partition_col = self.get_read_partition_col()
        lower_limit, upper_limit, tbl_cnt = self.get_min_max_rng_from_src_tbl(partition_key_col=read_partition_col)
        est_part_cnt = self.get_estimate_partition_count()
        main_logger.info("Read partition config summary")
        main_logger.info(f"Auto id column                   :{read_partition_col}")
        main_logger.info(f"upper limit                      :{upper_limit}")
        main_logger.info(f"lower limit                      :{lower_limit}")
        main_logger.info(f"Table count                      :{tbl_cnt}")
        main_logger.info(f"Estimated read partition count   :{est_part_cnt}")
        read_partition_config = {
            "numPartitions": est_part_cnt,
            "partitionColumn": read_partition_col,
            "lowerBound": lower_limit,
            "upperBound": upper_limit
        }
        return read_partition_config

    def get_estimate_partition_count(self):
        _tbl_stats_sql = f"""
        select tbl_schema,tbl_nm
        ,round(data_size_in_mb/optimal_part_size) est_part_cnt
        ,round(data_size_in_mb/optimal_part_size)+round(mod(data_size_in_mb/optimal_part_size,8)) rnd_to_8th_part_cnt
        ,AVG_ROW_LENGTH
        from (
        SELECT
            TABLE_SCHEMA AS tbl_schema
            , TABLE_NAME AS tbl_nm
            , ROUND((DATA_LENGTH) / 1024 / 1024) AS data_size_in_mb
            ,{self.part_size_mb} as optimal_part_size
            ,AVG_ROW_LENGTH
        from 
            information_schema.tables
        WHERE
            1=1
          and TABLE_SCHEMA = '{self.src_sch}'
          and table_name ='{self.src_tbl}'
          ) as x
        """
        main_logger.info(f"Executing sql to get stats to estimate partition:{_tbl_stats_sql}")
        tbl_stats_df = pd.read_sql(_tbl_stats_sql, con=self.mysql_conn_engine)
        est_part_cnt = tbl_stats_df["rnd_to_8th_part_cnt"][0]
        main_logger.info(f"Estimated partition count  derived:{est_part_cnt}")
        if est_part_cnt < 8:
            est_part_cnt = 8
        return int(est_part_cnt)

    def get_mysql_conn_engine(self):
        main_logger.info("Creating Source connection for getting partition configs to read data in parallel")
        mysql_conn_engine = get_mysql_alchemy_engine(
            host=self.src_host,
            port=self.src_port,
            ip_user=self.db_user,
            ip_password=self.db_pass,
            default_db=self.src_sch
        )
        return mysql_conn_engine


if __name__ == '__main__':
    conn = get_mysql_alchemy_engine(
        host='vm-cor-mbm-pr-eu2-ro.private.pr.internal.teladoc.com',
        port=3306,
        ip_user='talend',
        ip_password='XXXXXXX',
        default_db='member_master'
    )
    sql = """SELECT COLUMN_NAME as auto_incr_col
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'mrn_renewal_histories'
    and TABLE_SCHEMA = 'member_master'
    AND IS_NULLABLE = 'NO'
    AND EXTRA like 'auto_increment\%'"""

    df = pd.read_sql(sql=sql, con=conn)
    print(df.to_string())
