import pandas as pd
import sqlalchemy

from common.logmgr import get_logger
from meta_fw import meta_fw_settings as mfs
from meta_fw.dao import meta_step, meta_job, meta_step_email

main_logger = get_logger()


class MetaFrameworkConfig:
    def __init__(self, step_id, meta_conn, meta_sch):
        self.step_id = step_id
        # self.meta_conn: pymysql.connect = meta_conn
        self.meta_conn_engine: sqlalchemy.engine = meta_conn
        self.meta_sch = meta_sch
        self.ms_obj: meta_step.MetaStepRecord = None
        self.mj_obj: meta_job.MetaJobRecord = None
        self.mse_obj: meta_step_email.MetaStepEmail = None
        self._get_meta_objects()

    def _get_meta_sql(self, meta_table):

        if meta_table == "meta_step":
            meta_sql = mfs.meta_step_sql.format(step_id=self.step_id, schema=self.meta_sch)
        elif meta_table == "meta_job":
            meta_sql = mfs.meta_job_sql.format(step_id=self.step_id, schema=self.meta_sch)
        elif meta_table == "meta_step_email":
            meta_sql = mfs.meta_step_email_sql.format(step_id=self.step_id, schema=self.meta_sch)
        else:
            raise Exception(f"unhandled meta_table :{meta_table}")

        main_logger.info(f"sql for meta table {meta_table} :{meta_sql}")
        return meta_sql

    def _get_meta_table_rec_dict(self, meta_table):
        meta_table_ext_sql = self._get_meta_sql(meta_table)

        # if self.meta_conn_engine.connect().closed:
        #     self.meta_conn_engine.engine.connect()
        df = pd.read_sql(sql=meta_table_ext_sql, con=self.meta_conn_engine.raw_connection())

        if df.shape[0] > 1:
            main_logger.error("Step dict found:{df.to_string()}")
            raise Exception(f" invalid meta step records found expected 1. Found :{df.to_dict('records')}")
        elif df.empty:
            return df.to_dict('records')
        else:
            main_logger.info(f"Meta _step dict :{df.to_dict('records')}")
            return df.to_dict('records')[0]

    def _get_meta_objects(self):
        meta_step_dict = self._get_meta_table_rec_dict(meta_table="meta_step")
        meta_job_dict = self._get_meta_table_rec_dict(meta_table="meta_job")
        meta_step_email_dict = self._get_meta_table_rec_dict(meta_table="meta_step_email")

        self.ms_obj = meta_step.MetaStepRecord(meta_step_dict)
        self.mj_obj = meta_job.MetaJobRecord(meta_job_dict)
        if meta_step_email_dict:
            self.mse_obj = meta_step_email.MetaStepEmail(meta_step_email_dict)
        else:
            main_logger.info("Meta step email not configured")
        return True


if __name__ == '__main__':
    import os

    from meta_fw.services import meta_db_helper


    def fxr_meta_db_obj(env="uat"):
        db_user = os.environ.get("db_user")
        db_pass = os.environ.get("db_pass")
        # meta_schema = "currstate_report_uat" if env == "uat" else "currstate_report"
        host = "datasunrise01.aws2.teladoc.com" if env == "uat" else "edwaws.aws2.teladoc.com"
        port = 3311 if env == "uat" else 3306
        meta_db_obj = meta_db_helper.MetaDbHelper(meta_env="uat", db_user=db_user, db_pass=db_pass)
        return meta_db_obj


    metadb_obj = fxr_meta_db_obj("prod")
    # mfc_obj = MetaFrameworkConfig(step_id=18068, meta_conn=metadb_obj.meta_conn_engine, meta_sch=metadb_obj.meta_sch)
    mfc_obj = MetaFrameworkConfig(step_id=7864, meta_conn=metadb_obj.meta_conn_engine, meta_sch=metadb_obj.meta_sch)
    main_logger.info(f"Target table in meta step {mfc_obj.ms_obj.tgt_tbl}")
