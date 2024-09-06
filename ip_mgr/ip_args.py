from common.logmgr import get_logger

main_logger = get_logger()


def _process_additional_args(add_arg_str: str):
    add_arg_list = add_arg_str.split(',')
    add_arg_dict = {}
    for each_key_value_pair in add_arg_list:
        if '=' in each_key_value_pair and len(each_key_value_pair.split("=")) == 2:
            key, val = each_key_value_pair.split("=")
            add_arg_dict.update({key: val})
            main_logger.info(f"Pre processed addtional argument {key}:{val}")
        else:
            main_logger.info(f"Ignoring the additional invalid arg element {each_key_value_pair}")

    return add_arg_dict


def get_cmd_line_args(cmd_line_args_list: list):
    keys = ['db_host', 'db_port', 'db_name', 'db_user', 'db_password',
            'job_id', 'step_id', 'job_run_id', 'step_run_id', 'lower_limit', 'upper_limit',
            'filename', 'smtp_host', 'smtp_port', 'smtp_user', 'smtp_pass', 'add_arg']

    if len(cmd_line_args_list) != len(keys):
        main_logger.info(f"\nExpected position arguments length : {len(keys)} : {keys} ."
                         f"\nReceived position arguments length : {len(cmd_line_args_list)} : {cmd_line_args_list}")
        raise Exception("Error : Positional arguments mismatch")
    else:
        _temp_dict = dict(zip(keys, cmd_line_args_list[:len(keys)]))

    add_arg_dict = _process_additional_args(_temp_dict['add_arg'])
    _temp_dict.update(add_arg_dict)

    for k, v in _temp_dict.items():
        main_logger.info(f"Input arg {k}:{v if k not in ['db_password1', 'smtp_pass1'] else '*' * len(v)}  ")
    return _temp_dict


class PosArgAtt:
    db_host_key = 'db_host'
    db_port_key = 'db_port'
    db_name_key = 'db_name'
    db_user_key = 'db_user'
    mysql_password_key = 'db_password'
    job_id_key = 'job_id'
    step_id_key = 'step_id'
    job_run_id_key = 'job_run_id'
    step_run_id_key = 'step_run_id'
    lower_limit_key = 'lower_limit'
    upper_limit_key = 'upper_limit'
    filename_key = 'filename'
    smtp_host_key = 'smtp_host'
    smtp_port_key = 'smtp_port'
    smtp_user_key = 'smtp_user'
    smtp_pass_key = 'smtp_pass'
    add_arg_key = 'add_arg'
    api_token_key = 'api_token'
    edw_redshift_password_key = 'edw_redshift_password'
    aws_access_key_id_key = 'aws_access_key_id'
    aws_access_secret_key_id_key = 'aws_access_secret_key_id'


class PositionalArgs:
    def __init__(self, pos_arg_list):
        self.db_host = pos_arg_list[0]
        self.db_port = pos_arg_list[1]
        self.db_name = pos_arg_list[2]
        self.mysql_user = pos_arg_list[3]
        self.mysql_password = pos_arg_list[4]
        self.job_id = pos_arg_list[5]
        self.step_id = pos_arg_list[6]
        self.job_run_id = pos_arg_list[7]
        self.step_run_id = pos_arg_list[8]
        self.lower_limit = pos_arg_list[9]
        self.upper_limit = pos_arg_list[10]
        self.filename = pos_arg_list[11]
        self.smtp_host = pos_arg_list[12] if len(pos_arg_list) > 12 else None
        self.smtp_port = pos_arg_list[13] if len(pos_arg_list) > 13 else None
        self.smtp_user = pos_arg_list[14] if len(pos_arg_list) > 14 else None
        self.smtp_pass = pos_arg_list[15] if len(pos_arg_list) > 15 else None
        self.add_arg = pos_arg_list[16] if len(pos_arg_list) > 16 else None
        self.edw_redshift_user = pos_arg_list[3]  # same user id for mysql and redshift
        # added newly
        self.api_token = pos_arg_list[17] if len(pos_arg_list) > 17 else None
        self.edw_redshift_password = pos_arg_list[18] if len(pos_arg_list) > 18 else None
        self.aws_access_key_id = pos_arg_list[19] if len(pos_arg_list) > 19 else None
        self.aws_access_secret_key_id = pos_arg_list[20] if len(pos_arg_list) > 20 else None

    def get_email_smtp_input_dict(self):
        return {
            PosArgAtt.smtp_host_key: self.smtp_host,
            PosArgAtt.smtp_port_key: self.smtp_port,
            PosArgAtt.smtp_user_key: self.smtp_user,
            PosArgAtt.smtp_pass_key: self.smtp_pass
        }
