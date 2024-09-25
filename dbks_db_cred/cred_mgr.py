class DbksCredMgr:
    def __init__(self, all_credetails):
        mysql_ip,mysql_port,mysql_username,mysql_password,redshift_ip,redshift_port,redshift_username,redshift_password=all_credetails        
        #=redshift_credentails
        self.meta_db_user = None
        self.meta_db_pass = None
        self.mysql_ip=mysql_ip
        self.mysql_port=mysql_port
        self.mysql_user = mysql_username
        self.mysql_pass = mysql_password
        self.redshift_user = redshift_username
        self.redshift_pass = redshift_password
        self.redshift_ip=redshift_ip
        self.redshift_port=redshift_port

        self.sch='DW'
