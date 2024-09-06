from meta_fw.dao.meta_step import MetaStepRecord
from user_config.user_config_mgr import UserConfig
from common.logmgr import get_logger
from db.mysql_connect import get_mysql_alchemy_engine
from dbks_dbms.db_settings import mysql_settings

import sqlalchemy

main_logger = get_logger()


class MysqlTblMgr:
    tgt_to_stg_sch_map_dict = mysql_settings.TGT_TO_STG_SCH_MAP_DICT

    def __init__(self, meta_step_obj: MetaStepRecord, user_conf_obj: UserConfig, mysql_user, mysql_pass):
        self.step_id = meta_step_obj.step_id
        self.stg_sch = user_conf_obj.stg_sch
        self.tgt_tbl = user_conf_obj.tgt_tbl
        self.tgt_sch = user_conf_obj.tgt_sch
        self.tgt_db = user_conf_obj.tgt_db
        self.tgt_port = user_conf_obj.tgt_port
        self.key_col_list = user_conf_obj.key_cols_list

        self.tgt_db_user = mysql_user
        self.tgt_db_pass = mysql_pass

        self.stg_tbl = None
        self.hist_sch = None
        self.hist_tbl = None
        self.tgt_conn_engine: sqlalchemy.engine = None

        self.set_up_stg_sch_tbl()
        self.set_up_tgt_conn()
        main_logger.info(f"Stage schema and table derived :{self.stg_sch}.{self.stg_tbl}")

        self.drop_and_create_stg_tbl()

    def set_up_tgt_conn(self):
        self.tgt_conn_engine = get_mysql_alchemy_engine(host=self.tgt_db,
                                                        port=self.tgt_port,
                                                        ip_user=self.tgt_db_user,
                                                        ip_password=self.tgt_db_pass,
                                                        default_db=self.tgt_sch
                                                        )
        main_logger.info("Target connection created for validating target and stage table")

    def set_up_stg_sch_tbl(self):
        self.stg_tbl = f"stg_{self.tgt_tbl}_{self.step_id}"
        try:
            if str(self.stg_sch).upper() in ['NONE','NULL','']:
                self.stg_sch = self.tgt_to_stg_sch_map_dict[self.tgt_sch]
        except KeyError as e:
            main_logger.error(f"Target schema {self.tgt_sch} to stage table schema mapping not found")
            raise e

    def drop_and_create_stg_tbl(self):
        drop_stg_ddl = f"drop table if exists {self.stg_sch}.{self.stg_tbl}"
        create_stg_ddl = f"create table if not exists {self.stg_sch}.{self.stg_tbl} as " \
                         f"select * from {self.tgt_sch}.{self.tgt_tbl} where 1=2"
        create_idx_sql = f"""CREATE UNIQUE INDEX idx_key_index_for_dms 
                            ON {self.stg_sch}.{self.stg_tbl} ({",".join(self.key_col_list)});"""
        with self.tgt_conn_engine.begin() as connection:
            try:
                main_logger.info(f"Executing :{drop_stg_ddl}")
                connection.execute(sqlalchemy.text(drop_stg_ddl))
                main_logger.info(f"Stage table {self.stg_sch}.{self.stg_tbl} dropped")
                main_logger.info(f"Executing :{create_stg_ddl}")
                connection.execute(sqlalchemy.text(create_stg_ddl))
                main_logger.info(f"Stage table {self.stg_sch}.{self.stg_tbl} created if not exist")
                # connection.execute(sqlalchemy.text(create_idx_sql))
                # main_logger.info(f"Stage table {self.stg_sch}.{self.stg_tbl} index created for data migration")
                # main_logger.info("Skipping stage table creation due to access issue")
            except Exception as e:
                # connection.rollback()
                main_logger.error(f"Error recreating stage table {self.stg_sch}.{self.stg_tbl}")
                raise e
