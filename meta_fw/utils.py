import datetime as dt
import os
import sys
from pathlib import Path

import pandas as pd
import psutil

# import settings
from common.logmgr import get_logger

main_logger = get_logger()
2

def check_validation_col_status(df: pd.DataFrame, val_col_nm="status"):
    original_df_columns_list = df.columns
    upper_case_df_column_list = df.columns.str.upper()
    col_mapper_dict = dict(zip(upper_case_df_column_list, original_df_columns_list))

    df.columns = upper_case_df_column_list

    val_col_nm_upper_case = val_col_nm.upper()
    if val_col_nm_upper_case not in upper_case_df_column_list:
        main_logger.critical(f"Validation column {val_col_nm} not found in the DQ sql. \n"
                             f"Please add 'Result' column with DQ check as either with PASS/FAIL/NULL status")
        dq_result_has_fail_flag = None
    else:
        main_logger.info(f"Validation column check completed. "
                         f"Validation Column name `{col_mapper_dict[val_col_nm_upper_case]}`")

        res_list = df[val_col_nm_upper_case].values.tolist()
        dq_result_has_fail_flag = any(['FAIL' in str(each_col_val).upper() for each_col_val in res_list])
        if dq_result_has_fail_flag:
            main_logger.info("DQ check validation has failures. Flag set for raising exception")
        else:
            main_logger.info("DQ check validation has no failures")
    return dq_result_has_fail_flag


def convert_src_qr_additional_clause_key_val_to_dict(col_string):
    src_sql_add_clause_dict = {}
    for each_key_val in str(col_string).split(","):
        if '=' in each_key_val:
            key, val = each_key_val.split("=")
            src_sql_add_clause_dict.update({key: val})
        else:
            main_logger.info(f"Skipping config param {each_key_val}")
    main_logger.info(f"Additional config dict :{src_sql_add_clause_dict}")
    return src_sql_add_clause_dict


def check_if_step_already_running(job_id, step_id):
    curr_process_id = os.getpid()
    _platform = sys.platform.upper()
    if _platform in ['LINUX', 'UNIX']:
        for p in psutil.process_iter():
            cmd_line_list_resp = p.cmdline()
            if len(cmd_line_list_resp) > 9:
                if p.cmdline()[0] == "python3" and 'py_dms.zip' in p.cmdline()[1]:
                    running_job_id, running_step_id = p.cmdline()[7:9]
                    if str(job_id) == str(running_job_id) and str(step_id) == str(running_step_id):
                        if p.pid == curr_process_id:
                            main_logger.info(f"Validated background step {step_id} "
                                             f"for current process {curr_process_id}")
                        else:
                            main_logger.warn(f"Already meta step {step_id} is running in background")
                            raise SystemExit(0)
        main_logger.info(f"Job id {job_id} Step id {step_id} is not running in background. BG check completed")
    else:
        main_logger.info(f"Skipping background process check platform for {_platform}")


def get_dms_rpt_sch_by_env(curr_env):
    if curr_env.upper() == 'UAT':
        _val_schema = settings.DEFAULT_UAT_RPT_SCHEMA
    else:
        _val_schema = settings.DEFAULT_PROD_RPT_SCHEMA

    main_logger.info(f"Validation table schema :{_val_schema}")
    return _val_schema


def get_dms_def_meta_sch_by_env(curr_env):
    if curr_env.upper() == "UAT":
        def_meta_conn_src_sch = settings.DEFAULT_META_FRAMEWORK_SCHEMA
    else:
        def_meta_conn_src_sch = settings.DEFAULT_META_FRAMEWORK_SCHEMA_UAT
    return def_meta_conn_src_sch


def empty_file_content(dat_file):
    if dat_file is not None:
        try:
            main_logger.info(f"Trying to delete the file if exists{dat_file}")
            # os.remove(dat_file)
            Path(dat_file).unlink()
            main_logger.info(f"{dat_file} deleted")
        except Exception as e:
            main_logger.warning(f"Error while deleting file {dat_file}: {e}")

        open(dat_file, "w").close()
        dat_file_stat = os.stat(dat_file)
        datfile_c_time = dt.datetime.strftime(dt.datetime.fromtimestamp(dat_file_stat.st_ctime),
                                              '%Y-%m-%d %H:%M:%S %Z%z')
        main_logger.info(f"Created empty file :{dat_file} {os.stat(dat_file)}; "
                         f"Size :{dat_file_stat.st_size} "
                         f"c_time : {datfile_c_time}")
    else:
        raise Exception(f"No dsv file provided for writing {dat_file}")


def get_dt_rng_col_trans_by_db_type(db_type, dt_rng_col, tbl_freq, col_prefix=None):
    full_col_name = dt_rng_col if col_prefix is None else f"{col_prefix}.{dt_rng_col}"
    if tbl_freq.lower() not in [settings.FREQ_DAILY, settings.FREQ_MONTHLY]:
        main_logger.warning(f"Invalid table frequency {tbl_freq}. Please override or configure valid freq."
                            f"Valid frequency are {[settings.FREQ_DAILY, settings.FREQ_MONTHLY]}. "
                            f"Default is 'daily'")
    else:
        main_logger.info(f"Valid table freq provided for transforming dt rng column {full_col_name}")
    if db_type.lower() == 'mysql':
        if tbl_freq.lower() in ['monthly', "", None]:
            _dt_rng_col_trans = f"date_format({full_col_name},'%Y%m')"
        else:
            # _dt_rng_col_trans = f"date_format({full_col_name},'%Y-%m-%d')"
            _dt_rng_col_trans = f" date({full_col_name}) "  # for performance reason
    elif db_type.lower() == 'redshift':
        if tbl_freq.lower() in ['monthly', "", None]:
            _dt_rng_col_trans = f"to_char({full_col_name},'YYYYMM')"
        else:
            # _dt_rng_col_trans = f"to_char({full_col_name},'YYYY-MM-DD')"
            _dt_rng_col_trans = f" date({full_col_name}) "  # for performance reason
    else:
        raise Exception(f"Invalid db_type {db_type} to get date range column transformation")

    return _dt_rng_col_trans


if __name__ == '__main__':
    convert_src_qr_additional_clause_key_val_to_dict(None)
