from meta_fw import meta_fw_settings as mfs
from db import mysql_connect
from common.logmgr import get_logger

main_logger = get_logger()


class MetaDbHelper:
    def __init__(self, meta_env, db_user, db_pass):
        self.meta_env = meta_env
        self.db_user = db_user
        self.db_pass = db_pass
        self.meta_conn_engine = None
        self.meta_sch = None
        self.meta_host = None
        self.meta_port = None

        self.start_here()

    def _set_meta_schema(self):
        if self.meta_env.upper() in ["PROD", "DEV"]:
            self.meta_sch = mfs.DEFAULT_META_FRAMEWORK_SCHEMA
        elif self.meta_env.upper() in ["UAT"]:
            self.meta_sch = mfs.DEFAULT_META_FRAMEWORK_SCHEMA_UAT
        else:
            raise Exception(f"Invalid environment to configure meta framework schema for connection:{self.meta_env}")

    def _set_meta_conn(self):

        self.meta_host, self.meta_port = mysql_connect.get_edw_mysql_env_host_port(self.meta_env)

        main_logger.info(f"setting up meta_connection using :{self.meta_host},{self.meta_port}")
        self.meta_conn_engine = mysql_connect.get_mysql_alchemy_engine(host=self.meta_host,
                                                                       port=self.meta_port,
                                                                       ip_user=self.db_user,
                                                                       ip_password=self.db_pass,
                                                                       default_db=self.meta_sch
                                                                       )

    def start_here(self):
        self._set_meta_schema()
        self._set_meta_conn()
