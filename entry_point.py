from common.logmgr import get_logger
from dbks_dbms.mgr.dbks_dms_mgr import DbksDmsMgr
from ip_mgr.ip_args import PositionalArgs
from env_mgr.env_mgr import EnvMgr
import sys
import os
from pathlib import Path
import datetime as dt
import settings

#import test_setup

main_logger = get_logger()


def start_dbks_dms(step_info,mysql_credetails,redshift_credentails):

    main_logger.info(f"Processing meta step id : {step_info['src_tbl']}")
    master_obj = DbksDmsMgr(step_info,mysql_credetails,redshift_credentails)
    #master_obj.provision_dms()
    return master_obj

