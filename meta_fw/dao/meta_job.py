class MetaJobAtt:
    job_id_key = 'job_id'
    job_name_key = 'job_name'
    frequency_key = 'frequency'
    is_active_flg_key = 'is_active_flg'
    created_at_key = 'created_at'
    created_by_key = 'created_by'
    updated_at_key = 'updated_at'
    updated_by_key = 'updated_by'


class MetaJobRecord:
    def __init__(self, meta_job_rec_dict):
        self.job_id = meta_job_rec_dict[MetaJobAtt.job_id_key]
        self.job_name = meta_job_rec_dict[MetaJobAtt.job_name_key]
        self.frequency = meta_job_rec_dict[MetaJobAtt.frequency_key]
        self.is_active_flg = meta_job_rec_dict[MetaJobAtt.is_active_flg_key]
        self.created_at = meta_job_rec_dict[MetaJobAtt.created_at_key]
        self.created_by = meta_job_rec_dict[MetaJobAtt.created_by_key]
        self.updated_at = meta_job_rec_dict[MetaJobAtt.updated_at_key]
        self.updated_by = meta_job_rec_dict[MetaJobAtt.updated_by_key]
