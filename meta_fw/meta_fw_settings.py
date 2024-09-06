DEFAULT_META_FRAMEWORK_SCHEMA = "currstate_report"
DEFAULT_META_FRAMEWORK_SCHEMA_UAT = "currstate_report_uat"
meta_step_sql = "SELECT step_seq_no,job_id,job_name,step_id,step_name,step_desc,is_active_flg, " \
                " src_db_type,src_db,src_port,src_schema,src_dir,src_qry,src_qry_add_clause, " \
                " tgt_db_type,tgt_db,tgt_port,tgt_schema,tgt_tbl_nm, " \
                " tgt_dml_type,tgt_idx,hist_tbl_nm,tgt_dir " \
                " FROM {schema}.meta_step " \
                " where " \
                " step_id = {step_id} " \
                " ORDER by step_seq_no "

meta_job_sql = """
-- meta_job
select job_id, job_name, frequency, is_active_flg, created_at, created_by, updated_at, updated_by
from {schema}.meta_job 
where job_id = (select distinct job_id from {schema}.meta_step where step_id ={step_id})
ORDER by job_id
"""
meta_step_email_sql = """
-- meta_step_email 
SELECT email_config_id, email_config_name, description, is_active_flg, job_id, job_name, step_id, step_name, 
email_from, email_to, email_cc, email_bcc, email_sub, test_flag, test_email_to, table_title, text_content, signature, 
trans_flag,trans_col,
created_at, created_by, updated_at, updated_by
FROM {schema}.meta_step_email
WHERE step_id={step_id}
order by email_config_id
"""
