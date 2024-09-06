from common.logmgr import get_logger
from meta_fw.services.meta_config import MetaFrameworkConfig
from user_config.user_config_mgr import UserConfig
from dbks_db_cred.cred_mgr import DbksCredMgr
from db.redshift_connect_mgr import get_redshift_conn
import pandas as pd

main_logger = get_logger()


class RedshiftPreProc:
    def __init__(self, uc_obj: UserConfig, cred_obj: DbksCredMgr
                 , rs_host, rs_port):
        # self.mfc_obj = mfc_obj
        self.uc_obj = uc_obj
        self.cred_obj = cred_obj

        # self.dbks2mysql_ip_dict = self._prepare_dbks2mysql_loader_ip()
        self.redshift_sql_alchemy_engine = get_redshift_conn(host=rs_host,
                                                             port=rs_port,
                                                             db_user=self.cred_obj.redshift_user,
                                                             db_password=self.cred_obj.redshift_pass
                                                             )
        self.tbl_audit_col_max_dt = self._get_max_audit_dt_value_from_tgt_tbl()

    def _get_max_audit_dt_value_from_tgt_tbl(self):
        target_sch = self.uc_obj.tgt_sch
        target_tbl = self.uc_obj.tgt_tbl

        sql = f"select max({self.uc_obj.user_dt_col}) as max_tgt_tbl_dt from {target_sch}.{target_tbl}"
        main_logger.info(f"Reading start date for extraction :{sql}")

        df = pd.read_sql(sql, self.redshift_sql_alchemy_engine)
        rec = df.to_dict("records")

        max_tgt_tbl_dt = rec[0]['max_tgt_tbl_dt'] if len(rec) > 0 else '1900-01-01'
        main_logger.info(f"start date for extraction {max_tgt_tbl_dt}")
        return max_tgt_tbl_dt

    def _prepare_dbks2mysql_loader_ip(self):
        if self.uc_obj.user_dt_flg:
            start_dt = self.uc_obj.st_dt
            end_dt = self.uc_obj.end_dt
        else:
            _tgt_max_dt = self._get_max_audit_dt_value_from_tgt_tbl()
            start_dt = _tgt_max_dt
            end_dt = None
        dbks2mysql_loader_ip = {
            "st_dt": start_dt,
            "end_dt": end_dt,
        }

        return dbks2mysql_loader_ip
