import os
import sys
from pathlib import Path

LOCAL_PATH = Path(__file__).parent.absolute().as_posix()
BASE_PATH = Path(__file__).parent.parent.absolute().as_posix() if not sys.platform.upper().startswith(
    "WIN") else LOCAL_PATH
DATA_LOCAL_PATH = Path("C:/tmp/data").as_posix()
DATA_PATH = "/opt/di_feeds/internal/de_ops/edw_copy_to_redshift/mvp_validation"
LINUX_LOG_BASE_PATH = "/opt/di_feeds/internal/de_ops/"
PROJ_DIR_NM = os.path.basename(Path(__file__).parent.parent) if not sys.platform.upper().startswith("WIN") else \
    os.path.basename(Path(__file__).parent)

DEF_PROD_DBKS_CLUSTER_ID = "1229-173116-afqwgjzn"  # Date Engineering Cluster
DEF_PROD_DBKS_HOST = "https://dbc-05a84502-3a5e.cloud.databricks.com"

# Platforms the code configured to work with
LNX_PLT_KEY = 'LINUX'
WIN_PLT_KEY = 'WIN'

# path
LNX_PLT_DATA_PATH = "/opt/di_feeds/internal/de_ops/edw_copy_to_redshift/mvp_validation/"
WIN_PLT_DATA_PATH = "c:/tmp" + LNX_PLT_DATA_PATH
DEF_S3_PATH = 's3://livongo-ds-dataimport/tdoc/edw/mvp/agg/'

environ_dict: dict = {
    'dev': {
        'host_name': ['diawsdev1.dev.us3.teladoc.com'],
        # 'direct_db_host': 'edwawsdev1.dev.us3.teladoc.com',
        'direct_db_host': 'edwawsdev1.dev.aws.teladoc.com',
        'direct_db_port': '3306',
        # 'tunnel_db_host': 'edwawsdev1.dev.us3.teladoc.com',
        'tunnel_db_host': 'edwawsdev1.dev.aws.teladoc.com',
        'tunnel_db_port': '3306',
        'install_dir': '/opt/di_feeds/internal/de_ops/dev',  # for future
        'proj_folder': 'meta_step_py_update'  # for future
    },
    'uat': {
        'host_name': ['ditacaws1.aws2.teladoc.com', 'diaws2.aws2.teladoc.com', 'diaws3.aws2.teladoc.com'],
        # 'direct_db_host': 'edwaws1.aws2.teladoc.com',
        # 'direct_db_port': '3306',
        'direct_db_host': 'datasunrise01.aws2.teladoc.com',
        'direct_db_port': '3311',
        'tunnel_db_host': 'datasunrise01.aws2.teladoc.com',
        'tunnel_db_port': '3311',
        'install_dir': '/opt/di_feeds/internal/de_ops/uat',  # for future
        'proj_folder': 'meta_step_py_update'  # for future
    },
    'prod': {
        'host_name': ['ditacaws1.aws2.teladoc.com'],
        'direct_db_host': 'edwaws.aws2.teladoc.com',
        'direct_db_port': '3306',
        'tunnel_db_host': 'datasunrise01.aws2.teladoc.com',
        'tunnel_db_port': '3310',
        'install_dir': '/opt/di_feeds/internal/de_ops/prod',
        'proj_folder': 'meta_step_py_update'
    }
}

DEFAULT_META_FRAMEWORK_SCHEMA = 'currstate_report'
DEFAULT_META_FRAMEWORK_SCHEMA_UAT = 'currstate_report_uat'

# DEFAULT REDSHIFT DB
# DEF_REDSHIFT_DB='dev'
# DEF_REDSHIFT_DB='test'
DEF_REDSHIFT_DB = 'prod'

# variable to set local development environment connect to  DEV/UAT/PROD environment
# CONNECT_LOCAL_TO = 'UAT'
CONNECT_LOCAL_TO = 'PROD'
