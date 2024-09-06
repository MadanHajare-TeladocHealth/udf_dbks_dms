DEF_REDSHIFT_DB = 'prod'
DEF_REDSHIFT_PROC_SCHEMA = 'data_sciences'

# for connection from local desktop to PROD db. Only for local
MYSQL_PROD_PRIMARY_TUNNEL_HOST = 'datasunrise01.aws2.teladoc.com'
MYSQL_PROD_PRIMARY_TUNNEL_PORT = '3310'

# for connection from application servers (linux box) to PROD.
MYSQL_PROD_PRIMARY_DIRECT_HOST = 'edwaws.aws2.teladoc.com'
MYSQL_PROD_PRIMARY_DIRECT_PORT = '3306'

# for connection from local to UAT DB. Works on both local and linux
MYSQL_HA_HOST = 'datasunrise01.aws2.teladoc.com'
MYSQL_HA_PORT = '3311'

MYSQL_DEV_HOST = 'datasunrise02.dev.us3.teladoc.com'
MYSQL_DEV_PORT = '3309'

DEV_ENV_KEY = 'dev'
UAT_ENV_KEY = 'uat'
PROD_ENV_KEY = 'prod'


DEVELOPER_EMAIL = "vinodh.pakkiyyasamy@teladoc.health.com"