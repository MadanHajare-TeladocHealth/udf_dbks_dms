from common import logmgr
main_logger = logmgr.get_logger()


class MetaStepAtt:
    step_seq_no_key = 'step_seq_no'
    job_id_key = 'job_id'
    job_name_key = 'job_name'
    step_id_key = 'step_id'
    step_name_key = 'step_name'
    step_desc_key = 'step_desc'
    is_active_flg_key = 'is_active_flg'
    src_db_type_key = 'src_db_type'
    src_db_key = 'src_db'
    src_port_key = 'src_port'
    src_schema_key = 'src_schema'
    src_dir_key = 'src_dir'
    src_qry_key = 'src_qry'
    src_qry_add_clause_key = 'src_qry_add_clause'
    tgt_db_type_key = 'tgt_db_type'
    tgt_db_key = 'tgt_db'
    tgt_port_key = 'tgt_port'
    tgt_schema_key = 'tgt_schema'
    tgt_tbl_nm_key = 'tgt_tbl_nm'
    tgt_dml_type_key = 'tgt_dml_type'
    tgt_idx_key = 'tgt_idx'
    hist_tbl_nm_key = 'hist_tbl_nm'
    tgt_dir_key = 'tgt_dir'

class MetaStepRecord:
    def __init__(self, meta_step_rec_dict):
        self.step_seq_no = meta_step_rec_dict[MetaStepAtt.step_seq_no_key]
        self.job_id = meta_step_rec_dict[MetaStepAtt.job_id_key]
        self.job_name = meta_step_rec_dict[MetaStepAtt.job_name_key]
        self.step_id = meta_step_rec_dict[MetaStepAtt.step_id_key]
        self.step_name = meta_step_rec_dict[MetaStepAtt.step_name_key]
        self.step_desc = meta_step_rec_dict[MetaStepAtt.step_desc_key]
        self.is_active_flg = meta_step_rec_dict[MetaStepAtt.is_active_flg_key]
        self.src_db_type = meta_step_rec_dict[MetaStepAtt.src_db_type_key]
        self.src_db = meta_step_rec_dict[MetaStepAtt.src_db_key]
        self.src_port = meta_step_rec_dict[MetaStepAtt.src_port_key]
        self.src_sch = meta_step_rec_dict[MetaStepAtt.src_schema_key]
        self.src_dir = meta_step_rec_dict[MetaStepAtt.src_dir_key]
        self.src_qry = meta_step_rec_dict[MetaStepAtt.src_qry_key]
        self.src_qry_add_clause = meta_step_rec_dict[MetaStepAtt.src_qry_add_clause_key]
        self.tgt_db_type = meta_step_rec_dict[MetaStepAtt.tgt_db_type_key]
        self.tgt_db = meta_step_rec_dict[MetaStepAtt.tgt_db_key]
        self.tgt_port = meta_step_rec_dict[MetaStepAtt.tgt_port_key]
        self.tgt_sch = meta_step_rec_dict[MetaStepAtt.tgt_schema_key]
        self.tgt_tbl = meta_step_rec_dict[MetaStepAtt.tgt_tbl_nm_key]
        self.tgt_dml_type = meta_step_rec_dict[MetaStepAtt.tgt_dml_type_key]
        self.tgt_idx = meta_step_rec_dict[MetaStepAtt.tgt_idx_key]
        self.hist_tbl_nm = meta_step_rec_dict[MetaStepAtt.hist_tbl_nm_key]
        self.tgt_dir = meta_step_rec_dict[MetaStepAtt.tgt_dir_key]

    def get_src_qr_additional_clause_key_val_to_dict(self):
        src_sql_add_clause_dict = {}
        for each_key_val in str(self.src_qry_add_clause).split(";"):
            if '=' in each_key_val:
                key, val = each_key_val.split("=")
                src_sql_add_clause_dict.update({key: val})
            else:
                main_logger.info(f"Skipping config param {each_key_val}")
        main_logger.info(f"Additional config dict :{src_sql_add_clause_dict}")
        return src_sql_add_clause_dict
