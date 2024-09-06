tempS3Dir = "s3://livongo-ds-temp"
RS_SETUP_TEMP_S3IAM = 'arn:aws:iam::113931794823:role/Redshift_Databricks_S3'
# REC_LIMIT = 30000
REC_LIMIT = None
DEF_DB = 'prod'
REDSHIFT_DRIVER = "com.databricks.spark.redshift"
DEF_REDSHIFT_PROC_SCHEMA = 'data_sciences'

TGT_TO_STG_SCH_MAP_DICT = {
    'data_science_edw': 'data_science_edw_staging'
}