class DbksCredMgr:
    def __init__(self, meta_db_user, meta_db_pass, mysql_user, mysql_pass, redshift_user, redshift_pass):
        self.meta_db_user = meta_db_user
        self.meta_db_pass = meta_db_pass
        self.mysql_user = mysql_user
        self.mysql_pass = mysql_pass
        self.redshift_user = redshift_user
        self.redshift_pass = redshift_pass
