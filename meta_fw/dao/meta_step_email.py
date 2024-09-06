from common.logmgr import get_logger

main_logger = get_logger()


class MetaStepEmailAttr:
    email_config_id_key = 'email_config_id'
    email_config_name_key = 'email_config_name'
    description_key = 'description'
    is_active_flg_key = 'is_active_flg'
    job_id_key = 'job_id'
    job_name_key = 'job_name'
    step_id_key = 'step_id'
    step_name_key = 'step_name'
    email_from_key = 'email_from'
    email_to_key = 'email_to'
    email_cc_key = 'email_cc'
    email_bcc_key = 'email_bcc'
    email_sub_key = 'email_sub'
    test_flag_key = 'test_flag'
    test_email_to_key = 'test_email_to'
    table_title_key = 'table_title'
    text_content_key = 'text_content'
    signature_key = 'signature'
    trans_flag_key = 'trans_flag'
    trans_col_key = 'trans_col'
    created_at_key = 'created_at'
    created_by_key = 'created_by'
    updated_at_key = 'updated_at'
    updated_by_key = 'updated_by'


class MetaStepEmail:
    def __init__(self, meta_step_email_dict):
        self.email_config_id = meta_step_email_dict[MetaStepEmailAttr.email_config_id_key]
        self.email_config_name = meta_step_email_dict[MetaStepEmailAttr.email_config_name_key]
        self.description = meta_step_email_dict[MetaStepEmailAttr.description_key]
        self.is_active_flg = meta_step_email_dict[MetaStepEmailAttr.is_active_flg_key]
        self.job_id = meta_step_email_dict[MetaStepEmailAttr.job_id_key]
        self.job_name = meta_step_email_dict[MetaStepEmailAttr.job_name_key]
        self.step_id = meta_step_email_dict[MetaStepEmailAttr.step_id_key]
        self.step_name = meta_step_email_dict[MetaStepEmailAttr.step_name_key]

        self.email_from = meta_step_email_dict[MetaStepEmailAttr.email_from_key]
        self.email_to = meta_step_email_dict[MetaStepEmailAttr.email_to_key]
        self.email_cc = meta_step_email_dict[MetaStepEmailAttr.email_cc_key]
        self.email_bcc = meta_step_email_dict[MetaStepEmailAttr.email_bcc_key]
        self.email_sub = meta_step_email_dict[MetaStepEmailAttr.email_sub_key]

        self.signature = meta_step_email_dict[MetaStepEmailAttr.signature_key]
        self.text_content = meta_step_email_dict[MetaStepEmailAttr.text_content_key]
        self.table_title = meta_step_email_dict[MetaStepEmailAttr.table_title_key]

        self.trans_flag = meta_step_email_dict[MetaStepEmailAttr.trans_flag_key]
        self.trans_col = meta_step_email_dict[MetaStepEmailAttr.trans_col_key]

        self.test_flag = meta_step_email_dict[MetaStepEmailAttr.test_flag_key]
        self.test_email_to = meta_step_email_dict[MetaStepEmailAttr.test_email_to_key]

        self.created_at = meta_step_email_dict[MetaStepEmailAttr.created_at_key]
        self.created_by = meta_step_email_dict[MetaStepEmailAttr.created_by_key]
        self.updated_at = meta_step_email_dict[MetaStepEmailAttr.updated_at_key]
        self.updated_by = meta_step_email_dict[MetaStepEmailAttr.updated_by_key]
