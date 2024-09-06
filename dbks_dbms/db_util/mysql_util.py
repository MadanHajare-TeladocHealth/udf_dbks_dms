from common.logmgr import get_logger
from meta_fw.services.meta_config import MetaFrameworkConfig
from user_config.user_config_mgr import UserConfig
from dbks_db_cred.cred_mgr import DbksCredMgr
from db.mysql_connect import get_mysql_alchemy_engine
import pandas as pd

main_logger = get_logger()


class MysqlPreProc:
    def __init__(self, cred_obj: DbksCredMgr, uc_obj: UserConfig,
                 mysql_host: str, mysql_port, mysql_sch: str):
        # self.mfc_obj = mfc_obj
        self.uc_obj = uc_obj
        self.cred_obj = cred_obj
        self.host = mysql_host
        self.port = mysql_port
        self.def_db = mysql_sch
        self.user_dt_col = uc_obj.user_dt_col
        self.mysql_conn_engine = get_mysql_alchemy_engine(host=self.host,
                                                          port=self.port,
                                                          default_db=self.def_db,
                                                          ip_user=self.cred_obj.mysql_user,
                                                          ip_password=self.cred_obj.mysql_pass
                                                          )
        self.tbl_audit_col_max_dt = self._get_max_audit_dt_value_from_mysql_tbl()
        # self.dbks2mysql_ip_dict = self._prepare_dbks2mysql_loader_ip()

    def _get_max_audit_dt_value_from_mysql_tbl(self):

        target_sch = self.uc_obj.tgt_sch
        target_tbl = self.uc_obj.tgt_tbl
        user_dt_col = self.uc_obj.user_dt_col

        sql = f"""select 
              date_format(MAX({user_dt_col}),'%Y-%m-%d %H:%i:%s') as max_tgt_tbl_dt
              from {target_sch}.{target_tbl}"""
        main_logger.info(f"Reading max time stamp from mysql table :{sql}")

        df = pd.read_sql(sql, self.mysql_conn_engine.raw_connection())
        rec = df.to_dict("records")
        main_logger.info(f"records :{rec}")
        tbl_audit_col_max_dt = rec[0]['max_tgt_tbl_dt'] if len(rec) > 0 else '1900-01-01'
        main_logger.info(f"start date for extraction {tbl_audit_col_max_dt}")
        return tbl_audit_col_max_dt

    # def _prepare_dbks2mysql_loader_ip(self):
    #     if self.uc_obj.user_dt_flg:
    #         start_dt = self.uc_obj.st_dt
    #         end_dt = self.uc_obj.end_dt
    #     else:
    #         _tgt_max_dt = self._get_max_audit_dt_value_from_mysql_tbl()
    #         start_dt = _tgt_max_dt
    #         end_dt = None
    #     dbks2mysql_loader_ip = {
    #         Dbks2MysqlDmsMgrAttr.st_dt_key: start_dt,
    #         Dbks2MysqlDmsMgrAttr.end_dt_key: end_dt,
    #     }
    #
    #     return dbks2mysql_loader_ip

    # def get_mysql_tbl_max_audit_col_dt(self, mysql_conn_engine, tbl_sch, tbl_tbl, audit_col="updated_at"):
    #     max_dt_sql = f"select max({audit_col}) as max_audit_col_dt from {tbl_sch}.{tbl_tbl}"
    #     df = pd.read_sql(max_dt_sql, mysql_conn_engine)
    #     main_logger.info(f"Reading max date time audit column using sql :{max_dt_sql}")
    #
    #     df = pd.read_sql(max_dt_sql, mysql_conn_engine)
    #     rec = df.to_dict("records")
    #
    #     max_tgt_tbl_dt = rec[0]['max_audit_col_dt'] if len(rec) > 0 else '1900-01-01'
    #     main_logger.info(f"start date for extraction {max_tgt_tbl_dt}")
    #     return max_tgt_tbl_dt
