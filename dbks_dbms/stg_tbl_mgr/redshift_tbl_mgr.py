from meta_fw.dao.meta_step import MetaStepRecord
from user_config.user_config_mgr import UserConfig
from common.logmgr import get_logger
from db import redshift_connect_mgr
#from dbks_dbms.db_settings import redshift_settings

import sqlalchemy

main_logger = get_logger()


class RedshiftTblMgr:
    #tgt_to_stg_sch_map_dict = redshift_settings.TGT_TO_STG_SCH_MAP_DICT

    def __init__(self,  user_conf_obj: UserConfig, cred_obj):
        #self.step_id = meta_step_obj.step_id
        self.stg_sch = user_conf_obj.tgt_sch
        self.tgt_tbl = user_conf_obj.tgt_tbl
        self.tgt_sch = user_conf_obj.tgt_sch
        self.tgt_db = cred_obj.redshift_ip
        self.tgt_port = cred_obj.redshift_port
        self.key_col_list = user_conf_obj.key_cols_list

        self.tgt_db_user = cred_obj.redshift_user
        self.tgt_db_pass = cred_obj.redshift_pass

        self.stg_tbl = user_conf_obj.tgt_tbl+'_udfsync_stg'
        self.hist_sch = None
        self.hist_tbl = None
        self.tgt_conn_engine = None

        self.set_up_tgt_conn()
        self.set_up_stg_sch_tbl()

    def set_up_tgt_conn(self):
        main_logger.info(f"setting up redshift target connection using host :{self.tgt_db}:{self.tgt_port}")
        self.tgt_conn_engine = redshift_connect_mgr.get_redshift_conn(host=self.tgt_db,
                                                                      port=self.tgt_port,
                                                                      db_user=self.tgt_db_user,
                                                                      db_password=self.tgt_db_pass
                                                                     )
       
        #main_logger.info("Target connection created for validating target and stage table")

    def set_up_stg_sch_tbl(self):
        main_logger.info(f"setting up stage table {self.stg_sch}.{self.stg_tbl}")
        try:
            cursor = self.tgt_conn_engine.cursor()
            cursor.execute(f"SELECT 1  FROM information_schema.tables  WHERE table_schema = '{self.stg_sch}' AND table_name = '{self.stg_tbl}'")
            found = cursor.fetchone()
            main_logger.info(f"metadata result {found}")
            if  found is None:
                main_logger.info(f"creating stage table {self.stg_sch}.{self.stg_tbl}")        
                stg_ddl = f" create table {self.stg_sch}.{self.stg_tbl} as select * from {self.tgt_sch}.{self.tgt_tbl} where 1=2 "
                cursor.execute(stg_ddl)
                main_logger.info(f"Stage table {self.stg_sch}.{self.stg_tbl} created")
            else:
                main_logger.info(f"Stage table {self.stg_sch}.{self.stg_tbl} already exists")
            cursor.close()
            

        except KeyError as e:
            main_logger.error(f"Target schema {self.tgt_sch} to stage table schema mapping not found")
            raise e
        except Exception as e:
            main_logger.info(f"Error setting up stage table {self.stg_sch}.{self.stg_tbl} {e}")
