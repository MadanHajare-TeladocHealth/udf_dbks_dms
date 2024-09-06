from common.logmgr import get_logger
from dbks_dbms.mgr.dbks_dms_mgr import DbksDmsMgr
from ip_mgr.ip_args import PositionalArgs
from env_mgr.env_mgr import EnvMgr
import sys
import os
from pathlib import Path
import datetime as dt
import settings

import test_setup

main_logger = get_logger()


def get_full_log_file():
    env_path = env_obj.curr_env
    process_id = os.getpid()
    log_ts = dt.datetime.strftime(dt.datetime.now(), '%Y%m%d%H%I%S')
    _log_file = f"trigger_dbks_{pos_arg_obj.job_id}_{pos_arg_obj.step_id}_{process_id}_{log_ts}.log"

    if env_obj.curr_platform == "WIN":
        log_file_path = f"C:/tmp/{settings.LINUX_LOG_BASE_PATH}{env_path}/log/{settings.PROJ_DIR_NM}"
    else:
        log_file_path = f"{settings.LINUX_LOG_BASE_PATH}{env_path}/log/{settings.PROJ_DIR_NM}"

    if not Path(log_file_path).exists():
        main_logger.info(f"Creating log path folder {log_file_path}")
        os.makedirs(log_file_path, mode=0o775, exist_ok=True)
    else:
        main_logger.info(f"Log path already exist {log_file_path}")

    _full_log_file = Path(log_file_path).joinpath(_log_file)

    return _full_log_file


def start_dbks_dms(step_id, meta_env, mysql_user, mysql_pass, redshift_user, redshift_pass):
    main_logger.info(f"Processing meta step id : {step_id} from environment :{meta_env}")
    master_obj = DbksDmsMgr(step_id, meta_env, mysql_user, mysql_pass, redshift_user, redshift_pass)
    master_obj.provision_dms()


if __name__ == '__main__':

    if sys.platform.upper().startswith("WIN") and len(sys.argv) == 1:
        # for testing from windows
        test_setup.set_cmd_line_arg()

    pos_arg_obj = PositionalArgs(pos_arg_list=sys.argv[1:])
    env_obj = EnvMgr(fid_db_user=pos_arg_obj.mysql_user, mysql_pass=pos_arg_obj.mysql_password,
                     redshift_pass=pos_arg_obj.edw_redshift_password)

    log_file = get_full_log_file()
    main_logger = get_logger(log_file=log_file)

    start_dbks_dms(pos_arg_obj.step_id, env_obj.curr_env,
                   pos_arg_obj.mysql_user, pos_arg_obj.mysql_password,
                   pos_arg_obj.edw_redshift_user, pos_arg_obj.edw_redshift_password)

