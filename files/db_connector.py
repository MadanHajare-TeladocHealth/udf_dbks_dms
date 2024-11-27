from databricks.sdk.runtime import *
#from spark_instance import *
from pyspark import keyword_only
import psycopg2


class DataConnector: 
    '''
    Class to connect to a database
    '''

    def __init__(self, format, database, credentials=None):
        '''
        Initialize the connector

        Parameters
        ----------
        format : String
            Format of the database (mysql, redshift)
        database : String
            Name of the database (prod, DW)
        credentials : Dictionary, optional
            Dictionary with database credentials
        '''
        
        self.format = format.lower()
        self.database = database
        
        # load credentials to work from databricks using secrets
        if credentials is None:

            credentials = {}

            if self.contains_keywords(self.format, ['redshift', 'rs', 'aws']):
                credentials['redshift_host'] = dbutils.secrets.get(scope='ds-rs-pii-prod', key='HOST_IP')
                credentials['redshift_port'] = dbutils.secrets.get(scope='ds-rs-pii-prod', key='HOST_PORT')
                credentials['redshift_user'] = dbutils.secrets.get(scope='ds-rs-pii-prod', key='USERNAME')
                credentials['redshift_password'] = dbutils.secrets.get(scope='ds-rs-pii-prod', key='PASSWORD')
                credentials['tempdir'] = "s3a://livongo-ds-temp"

            elif self.contains_keywords(self.format, ['mysql', 'ms', 'dw', 'edw']): 
                credentials['edw_host'] = dbutils.secrets.get(scope='ds-edw-prod', key='HOST_NAME')
                credentials['edw_port'] = dbutils.secrets.get(scope='ds-edw-prod', key='HOST_PORT')
                credentials['edw_user'] = dbutils.secrets.get(scope='ds-edw-prod', key='USERNAME') 
                credentials['edw_password'] = dbutils.secrets.get(scope='ds-edw-prod', key='PASSWORD') 

        self.credentials = credentials
     
        
    def sql_query(self, query, returnPandas=False):
            '''
            Execute a sql query

            Parameters
            ----------
            query : String
                Query to execute
            returnPandas : Boolean, optional
                If True, return a pandas dataframe, else return a spark dataframe
            '''

            if self.contains_keywords(self.format, ['redshift', 'rs', 'aws']):
                
                df = (spark.read
                        .format('redshift')
                        .option("host", self.credentials['redshift_host'])
                        .option("port", self.credentials['redshift_port'])
                        .option("database", self.database)
                        .option("user", self.credentials['redshift_user'])
                        .option("password", self.credentials['redshift_password'])
                        .option('tempdir', self.credentials['tempdir'])
                        .option("autoenablessl", False)
                        .option("forward_spark_s3_credentials", True)
                        .option("query", query)
                        .load()
                )
            elif self.contains_keywords(self.format, ['mysql', 'ms', 'dw', 'edw']): 
        
                df = (spark.read
                        .format('mysql')
                        .option("database", self.database)
                        .option("host", self.credentials['edw_host'])
                        .option("port", self.credentials['edw_port'])
                        .option("user", self.credentials['edw_user'])
                        .option("password", self.credentials['edw_password'])
                        .option("query", query)
                        .load()
                )

            if returnPandas:
                return df.toPandas()

            else:
                return df

    # auxiliary functions
    def contains_keywords(self, text, keywords):
        text = text.lower()
        return any(keyword in text for keyword in keywords)

        
    @keyword_only
    def create_table_redshift(self, df, table, database='prod', host=None, username=None, 
                              password=None, port=None, mode='overwrite', format=None,column_types=None):
        '''
        Save the spark dataframe as a redshift table
        
        Parameters
        ----------
        df : Spark dataframe
        table : String
            Name of the table
        database : String, optional
            Name of the database
        host: String, optional
            Host
        username : String, optional
            Name of the user
        password : String, optional
            Password
        port : String, optional
            Port
        mode : String, optional
            Writing mode (append, overwrite, ignore, errorifexists)
        format : String, optional
            Format when writing the table from Spark (com.databricks.spark.redshift)
        '''

        if username is None:
            username = self.credentials['redshift_user']
        if password is None:
            password = self.credentials['redshift_password']
        if host is None:
            host = self.credentials['redshift_host']
        if port is None:
            port = self.credentials['redshift_port']
        if format is None:
            format = 'com.databricks.spark.redshift'
        
        (df.write.format(format)
            .options(
                url='jdbc:redshift://'+ host + ':' + port + '/' + database, 
                dbtable=table,
                user=username,
                password=password, 
                tempdir='s3a://livongo-ds-temp', 
                autoenablessl='false', 
                forward_spark_s3_credentials='true',
                createTableColumnTypes=column_types
                )
            .mode(mode).save()
        )
        print(f'Creating table {table}...')
    

    @keyword_only
    def drop_table_redshift(self, table, database=None, host=None, 
                            username=None, password=None, port=None):
        '''
        Drop a redshift table

        Parameters
        ----------
        database : String
            Name of the database
        table : String
            Name of the table
        host: String
            Name of the host
        username : String, optional
            Name of the user
        password : String, optional
            Password
        '''
        
        if database is None:
            database = self.database
        
        if username is None:
            username = self.credentials['redshift_user']
        if password is None:
            password = self.credentials['redshift_password']
        if host is None:
            host = self.credentials['redshift_host']
        if port is None:
            port = self.credentials['redshift_port']

        conn_string = f'host={host} port={port} dbname={database} user={username} password={password}'
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        query = f'DROP TABLE IF EXISTS {table}'
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()

        print(f'Dropping table {table}...')

    @keyword_only
    def run_dml_redshift(self, query, database=None, host=None, 
                            username=None, password=None, port=None):
        '''
        Drop a redshift table

        Parameters
        ----------
        database : String
            Name of the database
        table : String
            Name of the table
        host: String
            Name of the host
        username : String, optional
            Name of the user
        password : String, optional
            Password
        '''
        
        if database is None:
            database = self.database
        
        if username is None:
            username = self.credentials['redshift_user']
        if password is None:
            password = self.credentials['redshift_password']
        if host is None:
            host = self.credentials['redshift_host']
        if port is None:
            port = self.credentials['redshift_port']

        conn_string = f'host={host} port={port} dbname={database} user={username} password={password}'
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        print(f'Executing query ...')
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()

                
