from pathlib import Path
import os
import sys

import settings as conf
from common.logmgr import get_logger
import socket

main_logger = get_logger()


class EnvMgr:
    _dev_db_id = os.environ.get('db_user')
    _dev_mysql_db_key = os.environ.get('db_pass')

    def __init__(self, fid_db_user, mysql_pass, redshift_pass):
        self._fid_db_user = fid_db_user
        self._mysql_pass = mysql_pass
        self._redshift_pass = redshift_pass

        self._host_name = None
        self._direct_db_host = None
        self._direct_db_port = None
        self._tunnel_db_host = None
        self._tunnel_db_port = None

        self.install_dir = None
        self.proj_folder = None

        self.curr_platform: str = None
        self.curr_env = None
        self.curr_env_db_host = None
        self.curr_env_db_user = None
        self.curr_env_mysql_db_password = None
        # self.curr_env_redshift_db_password = None
        # self.curr_env_redshift_db_user = None
        self.curr_env_db_port = None

        self.set_env_details()

    def set_env_details(self):
        self.set_current_platform()
        self.set_curr_env()
        self.init_env_attr(curr_env=self.curr_env)
        self.set_meta_step_db_conf()
        self.log_env_details()

    def set_current_platform(self, mock_platform=None):
        if mock_platform is not None:
            self.curr_platform = mock_platform
            main_logger.info(f"Overriding platform as {mock_platform}")
        else:
            if sys.platform.upper().startswith("WIN"):
                self.curr_platform = "WIN"
            elif sys.platform.upper().startswith("LINUX"):
                self.curr_platform = "LINUX"
            else:
                raise Exception("Unhandled platform")
            main_logger.info(f"Platform detected {self.curr_platform}")

    def init_env_attr(self, curr_env):
        _curr_env = curr_env.lower()
        if _curr_env in conf.environ_dict.keys():
            self._host_name = conf.environ_dict[_curr_env]['host_name']
            self._direct_db_host = conf.environ_dict[_curr_env]['direct_db_host']
            self._direct_db_port = conf.environ_dict[_curr_env]['direct_db_port']
            self._tunnel_db_host = conf.environ_dict[_curr_env]['tunnel_db_host']
            self._tunnel_db_port = conf.environ_dict[_curr_env]['tunnel_db_port']
            self.install_dir = conf.environ_dict[_curr_env]['install_dir']
            self.proj_folder = conf.environ_dict[_curr_env]['proj_folder']
        else:
            raise Exception(f"Environment Config not found :{_curr_env}")

    def set_curr_env(self, mock_hostname=None):

        host_name = socket.gethostname() if mock_hostname is None else mock_hostname
        main_logger.info(f"Running on hostname :{host_name}")

        if self.curr_env is None and self.curr_platform == conf.WIN_PLT_KEY:
            self.curr_env = conf.CONNECT_LOCAL_TO
            main_logger.info(f"Connecting from local. overriding environment : {self.curr_env}")

        else:

            if '/uat/' in conf.BASE_PATH:
                self.curr_env = 'uat'
            elif '/prod/' in conf.BASE_PATH:
                self.curr_env = 'prod'
            elif '/dev/' in conf.BASE_PATH:
                self.curr_env = 'dev'
            else:
                main_logger.info(f"Unable to derive environment from base path {conf.BASE_PATH}")

            if self.curr_env is None:
                raise Exception(
                    f"Error in identifying current environment. {host_name}:{conf.BASE_PATH} "
                    f"Not found in configuration")
            else:
                main_logger.info(f"Environment identified based on script path as {self.curr_env}")

    def set_meta_step_db_conf(self):
        if self.curr_platform == conf.WIN_PLT_KEY:

            # set tunnel host and port for windows development connection
            self.curr_env_db_host = self._tunnel_db_host
            self.curr_env_db_port = self._tunnel_db_port

            # set source and target user credentials
            if None not in [self._dev_db_id, self._dev_mysql_db_key]:
                self.curr_env_db_user = self._dev_db_id
                self.curr_env_mysql_db_password = self._dev_mysql_db_key
            else:
                raise Exception("Missing developer DB credentials in environment variable 'db_user' ,'db_password'")
        elif self.curr_platform == conf.LNX_PLT_KEY:

            # set direct host and port for linux environment
            self.curr_env_db_host = self._direct_db_host
            self.curr_env_db_port = self._direct_db_port

            # set source and target user credentials
            if None not in [self._fid_db_user, self._mysql_pass, self._redshift_pass] \
                    or '' in [self._fid_db_user, self._mysql_pass, self._redshift_pass]:
                self.curr_env_db_user = self._fid_db_user
                self.curr_env_mysql_db_password = self._mysql_pass
            else:
                raise Exception(f"Missing or invalid Functional DB credentials "
                                f"{self._fid_db_user, self._mysql_pass, self._redshift_pass}")

    def log_env_details(self):
        for each_var, each_val in self.__dict__.items():
            if not each_var.startswith("_"):
                if 'password' in each_var:
                    main_logger.info(f"{each_var:<50}:{len(each_val) * '*'}")
                else:
                    main_logger.info(f"{each_var:<50}:{each_val}")
