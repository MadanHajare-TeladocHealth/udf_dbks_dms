from meta_fw.dao.meta_step import MetaStepRecord
from user_config.user_config_mgr import UserConfig
from common.logmgr import get_logger
from db import redshift_connect_mgr
from dbks_dbms.db_settings import redshift_settings

import sqlalchemy

main_logger = get_logger()


class RedshiftTblMgr:
    tgt_to_stg_sch_map_dict = redshift_settings.TGT_TO_STG_SCH_MAP_DICT

    def __init__(self, meta_step_obj: MetaStepRecord, user_conf_obj: UserConfig, redshift_user, redshift_pass):
        self.step_id = meta_step_obj.step_id

        self.stg_sch = user_conf_obj.stg_sch
        self.tgt_tbl = user_conf_obj.tgt_tbl
        self.tgt_sch = user_conf_obj.tgt_sch
        self.tgt_db = user_conf_obj.tgt_db
        self.tgt_port = user_conf_obj.tgt_port
        self.key_col_list = user_conf_obj.key_cols_list

        self.tgt_db_user = redshift_user
        self.tgt_db_pass = redshift_pass

        self.stg_tbl = None
        self.hist_sch = None
        self.hist_tbl = None
        self.tgt_conn_engine: sqlalchemy.engine = None

        self.set_up_tgt_conn()
        self.set_up_stg_sch_tbl()

    def set_up_tgt_conn(self):
        main_logger.info(f"setting up redshift target connection using host :{self.tgt_db}:{self.tgt_port}")
        self.tgt_conn_engine = redshift_connect_mgr.get_redshift_conn(host=self.tgt_db,
                                                                      port=self.tgt_port,
                                                                      db_user=self.tgt_db_user,
                                                                      db_password=self.tgt_db_pass
                                                                      )
        main_logger.info("Target connection created for validating target and stage table")

    def set_up_stg_sch_tbl(self):
        self.stg_tbl = f"stg_{self.tgt_tbl}_{self.step_id}"
        try:
            self.stg_sch = self.tgt_to_stg_sch_map_dict[self.tgt_sch] if self.stg_sch is None else self.stg_sch
            main_logger.info(f"Stage schema and table derived :{self.stg_sch}.{self.stg_tbl}")
            redshift_connect_mgr.create_table_like(engine=self.tgt_conn_engine
                                                   , src_sch=self.tgt_sch
                                                   , src_tbl=self.tgt_tbl
                                                   , tgt_sch=self.stg_sch
                                                   , tgt_tbl=self.stg_tbl
                                                   , drop_stg_flg=True
                                                   )
        except KeyError as e:
            main_logger.error(f"Target schema {self.tgt_sch} to stage table schema mapping not found")
            raise e
