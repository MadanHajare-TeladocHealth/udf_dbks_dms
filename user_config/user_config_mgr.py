import json

import pandas as pd

from common.logmgr import get_logger

main_logger = get_logger()


class ClsLdType:
    TRUNC_AND_LOAD = 'TRUNC_AND_LOAD'
    APPEND = 'APPEND'
    VALIDATE_AND_SYNC = 'VALIDATE_AND_SYNC'


class ClsUserConfAttr:
    user_dt_col_key = "USER_DT_COL"
    user_dt_flg_key = "USER_DT_FLG"

    src_db_type_key = "SRC_DB_TYPE"
    src_db_key = "SRC_DB"
    src_port_key = "SRC_PORT"
    src_table_key = "SRC_TABLE"
    src_schema_key = "SRC_SCHEMA"

    tgt_db_type_key = "TGT_DB_TYPE"
    tgt_db_key = "TGT_DB"
    tgt_port_key = "TGT_PORT"
    stg_schema_key = "STG_SCHEMA"
    stg_table_key = "STG_TABLE"
    tgt_schema_key = "TGT_SCHEMA"
    tgt_table_key = "TGT_TABLE"

    load_type_key = "LOAD_TYPE"
    part_rec_cnt_key = "PART_REC_CNT"
    src_sql_key = "SRC_SQL"
    key_cols_key = "KEY_COLS"

    src_pk_col_key = "SRC_PK_COL"
    part_size_mb = "SPARK_PARTITION_SIZE_MB"


class UserConfig:
    def __init__(self, step_info):
        #self.user_config_str = user_conf_str
        self.cust_src_sql = None

        self.src_db_type = "databricks"
        self.src_db = step_info["src_db"]
        self.src_port = None
        self.src_tbl = step_info["src_tbl"]
        self.src_sch = None

        self.tgt_db_type = step_info["tgt_db_typ"]
        self.tgt_db = step_info["tgt_db"]
        self.tgt_port = None
        self.tgt_tbl = step_info["tgt_tbl"]
        self.tgt_sch = None
        self.stg_sch = None
        self.stg_tbl = None

        self.user_dt_flg = False
        self.user_dt_col = None
        self.st_dt = None
        self.end_dt = None

        self.ld_type = None
        self.key_cols_list = step_info["merge_keys"]
        self.part_rec_count = None
        self.src_pk_col = None
        self.part_size_mb = 128+2
        #self.extract_dict_from_config_str()

    def extract_dict_from_config_str(self):
        conf_dict = json.loads(self.user_config_str)
        main_logger.info(conf_dict)

        try:
            self.user_dt_col = conf_dict[ClsUserConfAttr.user_dt_col_key]
            self.user_dt_flg = True if conf_dict[ClsUserConfAttr.user_dt_flg_key] == 'Y' else False

            self.src_db_type = conf_dict[ClsUserConfAttr.src_db_type_key]
            self.src_db = conf_dict[ClsUserConfAttr.src_db_key]
            self.src_port = conf_dict[ClsUserConfAttr.src_port_key]
            self.src_tbl = conf_dict[ClsUserConfAttr.src_table_key]
            self.src_sch = conf_dict[ClsUserConfAttr.src_schema_key]

            self.tgt_db_type = conf_dict[ClsUserConfAttr.tgt_db_type_key].lower()
            self.tgt_db = conf_dict[ClsUserConfAttr.tgt_db_key]
            self.tgt_port = conf_dict[ClsUserConfAttr.tgt_port_key]
            self.stg_sch = conf_dict[ClsUserConfAttr.stg_schema_key]
            self.tgt_sch = conf_dict[ClsUserConfAttr.tgt_schema_key]
            self.tgt_tbl = conf_dict[ClsUserConfAttr.tgt_table_key]

            self.ld_type = conf_dict[ClsUserConfAttr.load_type_key]

            if self.part_size_mb <= int(conf_dict.get(ClsUserConfAttr.part_size_mb,128)):
                self.part_size_mb=int(conf_dict.get(ClsUserConfAttr.part_size_mb, 128 + 2))
                main_logger.info(f"Using user provided config partition size :{self.part_size_mb}")

            self.src_pk_col = conf_dict.get(ClsUserConfAttr.src_pk_col_key)
        except KeyError as ke:
            main_logger.error("Missing user config key columns for databricks migration")
            raise ke
        except Exception as e:
            main_logger.error("Error in setting user config details for data migration")
            raise e

        self.stg_tbl = conf_dict.get(ClsUserConfAttr.stg_table_key)
        self.part_rec_count = conf_dict.get(ClsUserConfAttr.part_rec_cnt_key, 50000)
        self.cust_src_sql = conf_dict.get(ClsUserConfAttr.src_sql_key)
        self.key_cols_list = self.get_validated_key_cols_list(conf_dict.get(ClsUserConfAttr.key_cols_key))

        if self.user_dt_flg:
            try:
                self.st_dt = conf_dict["START_DT"]
                self.end_dt = conf_dict.get("END_DT")
            except KeyError as e:
                main_logger.error("Either START_DT missing for generating delta sql")
                raise e
        else:
            main_logger.info("Start and end date to be derived for delta load")

    @staticmethod
    def get_validated_key_cols_list(key_col_str):
        if key_col_str is None:
            raise Exception("Missing key column KEY_COLS config to upsert data between stage and target table")
        else:
            key_col_list = key_col_str.split(",")
            main_logger.info(f"key column list for upsert operation :{key_col_list}")
            return key_col_list


if __name__ == '__main__':
    user_conf_str = '{' \
                    '"SRC_SQL":"select * from de_currstate_report.daily_member_eligibility_snapshot where updated_at between \'2022-01-01\' and \'2023-01-01\'"' \
                    ',"SRC_SCH":"de_currstate_report"' \
                    ',"SRC_TBL":"daily_member_eligibility_snapshot"' \
                    ',"USER_DT_FLG":"N"' \
                    ',"USER_DT_COL":"updated_at"' \
                    ',"START_DT":"2023-01-01"' \
                    ',"END_DT":"2023-02-01"}'

    uc_obj = UserConfig(user_conf_str)
    uc_obj.extract_dict_from_config_str()
