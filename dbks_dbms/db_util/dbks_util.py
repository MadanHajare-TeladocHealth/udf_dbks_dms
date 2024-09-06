from common.logmgr import get_logger
from pyspark.sql import SparkSession
import dbks_dbms.settings as gen_set
import datetime as dt
main_logger = get_logger()


class DbksPreproc:
    app_name = gen_set.dms_spark_app_name

    def __init__(self, dbks_sch, dbks_tbl,audit_dt_col="updated_at"):
        self.dbks_sch = dbks_sch
        self.dbks_tbl = dbks_tbl
        self.audit_dt_col = audit_dt_col
        self.spark = self.get_spark_obj()
        self.audit_col_max_dt = self._get_max_audit_col_dt()

    @staticmethod
    def get_spark_obj():
        main_logger.info(f"Creating spark session with app name :{DbksPreproc.app_name}")
        spark = SparkSession.builder.getOrCreate()
        return spark

    def _get_max_audit_col_dt(self):
        sql = f"select date_format(max({self.audit_col}),'yyyy-MM-dd HH:mm:ss' ) as max_dt " \
              f"from {self.dbks_sch}.{self.dbks_tbl}"
        main_logger.info(f"Get end data for data extraction from source table :{self.dbks_sch}.{self.dbks_tbl}")
        df = self.spark.sql(sql)
        pd_df = df.toPandas()
        res_dict_list = pd_df['max_dt'].dt.strftime('%Y-%m-%d %H:%M:%S').to_dict('records')
        if len(res_dict_list) > 0:
            end_dt_dict = res_dict_list[0]
            main_logger.info(end_dt_dict['max_dt'])
            return end_dt_dict['max_dt']
        else:
            return '9999-12-31'
