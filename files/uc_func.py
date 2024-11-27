import os
import textwrap
from spark_instance import *
from pyspark import keyword_only
from pyspark.sql import functions as F
from typing import List, Union, Optional
from databricks.sdk.runtime import *
from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql.utils import AnalysisException
import logging
import json


class UnityCatalogProject:
    """
    Class to manage projects in Azure Databricks. It sets up necessary schemas in unity catalog
    based on the provided project, team, and environment settings. The schemas are tagged with
    defined privacy settings by default in the 'pii' catalog (versus 'npii').
    
    Environment variables required:
    - env: Set to 'prodtrain' or other environment types
    - team: The team name
    - project: The project name

    Parameters
    ----------
    privacy_level: str, optional
        Set to 'pii' or 'npii'. Defaults to 'pii'.
    """
    def __init__(
        self,
        privacy_level: str = 'pii',
        debug=False
    ):
        logging.basicConfig()
        self.logger = logging.getLogger('Unity Catalog Project Logger')
        self.logger.setLevel(logging.INFO)
        if debug:
            self.logger.setLevel(logging.DEBUG)
        self.logger.root.handlers[0].setFormatter(
            logging.Formatter(fmt='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
        self.privacy_level = privacy_level.lower()
        self.__check_environment_settings()

        # Set project variables to environment variables
        self.__env = os.getenv('env').lower()
        self.__team = os.getenv('team').upper()
        self.__project = os.getenv('project')

        # Check that the user is the a part of the team their cluster is specified as
        self.location_config = self.__define_location_config()
        self.feature_store = FeatureEngineeringClient()

        # Get the team's package name and service principal app id
        self.__team_package, self.__team_service_principal = self.__get_team_info()
        self.__creation_enabled = True
        self.__check_user_team()
        

    @keyword_only
    def create_project_schemas(
        self, 
        create_feature_schema: bool, 
        create_analytics_schema: bool, 
        comment: str):
        """
        Create and configure project schemas for feature and analytics data based on the environmental settings, privacy level,
        and project specification. Only creates schemas if specified to do so and if they do not already exist.

        Parameters
        ---------   
        create_feature_schema : bool
            Boolean flag indicating whether to create the feature schema.
        create_analytics_schema : bool
            Boolean flag indicating whether to create the analytics schema. 
        comment : str
            Comment to be added to the schema. Should be a short description of what the schema is for.
        """
        try:
            # Ensure the environment configuration is present
            if not self.location_config.get(self.__env):
                raise ValueError(f"No environment configuration found for {self.__env}")
            if not self.__creation_enabled:
                raise PermissionError("User has not been granted creation privileges.")

            # Create and configure feature schema if required
            if create_feature_schema:
                self.__create_and_configure_schema(
                    schema_type='feature',
                    comment=comment)
                
            # Create and configure analytics schema if required
            if create_analytics_schema:
                # Establish schema names and external locations
                self.__create_and_configure_schema(
                    schema_type='analytics',
                    comment=comment)

        except ValueError as ve:
            self.logger.exception(f"ValueError: {ve}")
        except Exception as e:
            self.logger.exception(f"An unexpected error occurred: {e}")

    @keyword_only
    def create_table(
        self,
        df,
        table: str,
        table_type: str = "delta",
        schema_type: str = "feature",
        comment: str = None,
        mode: str = 'overwrite',
        writing_schema_option: str = 'overwriteSchema',
        primary_keys: Optional[Union[str, List[str]]] = None,
        timestamp_keys: Optional[Union[str, List[str]]] = None,
        partition_columns: Optional[Union[str, List[str]]] = None,
    ):
        """
        Create a table (delta or feature store) in the specified schema in Databricks. Keyword-only.

        Parameters
        ----------
        df : DataFrame
            Spark dataframe to be saved.
        table : str
            Name of the table.
        table_type : str, optional
            Type of the table ('delta' or 'feature_store'), defaults to 'delta'.
        schema_type : str, optional
            Type of schema ('analytics' or 'feature') applies only to delta tables.
            Feature store tables are always in the 'feature' schema. Defaults to 'feature'.
        comment : str, optional
            Description of the table.
        mode : str, optional
            Writing mode. For delta tables ('overwrite', 'append', 'ignore', 'error'),
            defaults to 'overwrite'. For feature store tables ('merge', 'overwrite'),
            defaults to 'overwrite'.
        writing_schema_option : str, optional
            Spark writing schema option for delta tables ('mergeSchema', 'overwriteSchema'),
            defaults to 'overwriteSchema', see Spark documentation.
        primary_keys : list
            List of primary keys for feature store or delta table.
        timestamp_keys : list
            List of timestamp keys for feature store table.
        partition_columns : list
            List of partition columns for feature store table.
        """

        if table_type not in ['delta', 'feature_store']:
            raise ValueError("Table type must be 'delta' or 'feature_store'.")

        if not self.__creation_enabled:
            raise PermissionError("User has not been granted creation privileges.")

        if primary_keys:
            if isinstance(primary_keys, str):
                primary_keys = [primary_keys]

        # order columns with primary keys first
        if primary_keys:
            df = df.select([F.col(c) for c in primary_keys] + [F.col(c) for c in df.columns if c not in primary_keys])

        if table_type == "delta":
            self.__create_delta_table(
                df=df,
                table=table,
                schema_type=schema_type,
                primary_keys=primary_keys,
                comment=comment,
                mode=mode,
                writing_schema_option=writing_schema_option
            )
        elif table_type == "feature_store":
            self.__create_fstore_table(
                df=df,
                table=table,
                primary_keys=primary_keys,
                comment=comment,
                mode=mode,
                timestamp_keys=timestamp_keys,
                partition_columns=partition_columns,
            )
    

    def get_feature_schema(self):
        """
        Get the path of the feature schema 
        """
        feature_schema = get_schema_prefix(self.__env) + "feature" + self.privacy_level + "." + self.__project
        return feature_schema if spark.catalog.databaseExists(feature_schema) else None
    

    def get_ana_schema(self):
        """
        Get the path of the analysis schema 
        """
        ana_schema = get_schema_prefix(self.__env) + "ana" + self.privacy_level + "." + self.__project
        return ana_schema if spark.catalog.databaseExists(ana_schema) else None
    

    def get_project_info(self):
        """
        Print the project's information
        """
        project_info = f"""Environment: {self.__env}\nTeam: {self.__team}\nProject: {self.__project}\nPrivacy: {self.privacy_level}"""
        if self.get_feature_schema() : project_info += f"\nFeature Schema: {self.get_feature_schema()}"
        if self.get_ana_schema() : project_info += f"\nAnalysis Schema: {self.get_ana_schema()}"
        print(textwrap.dedent(project_info))

    
    
    def get_feature_tables(self):
        """
        Print tables for feature schema (if it exists)
        """
        if self.get_feature_schema():
            spark.sql(f"SHOW TABLES IN {self.get_feature_schema()}").display(truncate=False)


    def get_ana_tables(self):
        """
        Print tables for analysis schema (if it exists)
        """
        if self.get_ana_schema():
            spark.sql(f"SHOW TABLES IN {self.get_ana_schema()}").display(truncate=False)


    def drop_project_schemas(self, drop_feature = True, drop_analytics = True):
        """
        Delete existing project schemas along with all data contained within

        Parameters
        ----------
        drop_feature : bool, optional
            Decide to drop the feature schema. Defaults to True.
        drop_analytics : bool, optional
            Decide to drop the analytics schema. Defaults to True.
        """
        try:
            if self.__env == 'prod':
                raise PermissionError("Dropping schemas is not permitted in prod.")
            if self.get_feature_schema() and drop_feature:
                self.logger.info(f'Dropping {self.get_feature_schema()}...')
                spark.sql(f"DROP SCHEMA IF EXISTS {self.get_feature_schema()} CASCADE")
            if self.get_ana_schema() and drop_analytics:
                self.logger.info(f'Dropping {self.get_ana_schema()}...')
                spark.sql(f"DROP SCHEMA IF EXISTS {self.get_ana_schema()} CASCADE")
        except PermissionError as e:
            raise PermissionError("Dropping schemas is only accessible by admins. Make a ticket on the MLPO board if you would like to drop a schema.")
        


    def get_schema_access(self, schema):
        """
        Returns df schema access

        Parameters
        ----------
        schema : str
            Name of schema
        """
        return spark.sql(f"SHOW GRANT ON SCHEMA {schema}").display(truncate=False)   
    

    def get_table_access(self, table):
        """
        Returns df of table access

        Parameters
        ----------
        table : str
            Name of table
        """
        return spark.sql(f"SHOW GRANT ON TABLE {table}").display(truncate=False)  
    
    
    def get_schema_owner(self, schema):
        """
        Returns the account of the schema owner.

        Parameters
        ----------
        schema : str
            Name of schema
        """
        return (spark.sql(f"DESC SCHEMA EXTENDED {schema}") 
                .filter('database_description_item = "Owner"')
                .select('database_description_value')
                .collect()[0][0]
        )


    def get_volume_owner(self, volume):
        """
        Returns the account of the volume owner.

        Parameters
        ----------
        volume : str
            Name of volume
        """
        return (spark.sql(f"DESCRIBE VOLUME {volume}").select('owner').first()['owner'])
    

    def get_table_owner(self, table):
        """
        Returns the account of the table owner.

        Parameters
        ----------
        table : str
            Name of table
        """
        return (spark.sql(f"DESC TABLE EXTENDED {table}")
                .filter('col_name = "Owner"')
                .select('data_type')
                .collect()[0][0]
        )


    def get_object_owner(self, object_type, object_name):
        """
        Returns the account of the table owner.

        Parameters
        ----------
        object_type : str
            Type of object. Accepted values: 'schema', 'volume', 'table'
        object_name: str
            Name of object
        """
        if object_type == 'schema':
            return self.get_schema_owner(object_name)
        elif object_type == 'volume':
            return self.get_volume_owner(object_name)
        elif object_type == 'table':
            return self.get_table_owner(object_name)
        else:
            print(f"Object type {object_type} not recognized.")



    def comment_schema(self, schema_name, comment):
        """
        Add a comment to the schema

        Parameters
        ----------
        schema_name : str
            Name of schema
        comment: str
            Description of schema's purpose
        """
        owner = self.get_schema_owner(schema_name)
        if owner == self.__get_current_user():
            spark.sql(f"ALTER SCHEMA {schema_name} SET DBPROPERTIES ('comment' = '{comment}')")
        else:
            self.logger.info(f"You are not the owner of {schema_name}, you can't comment it!")


    def comment_table(self, schema_table, comment):
        """
        Add a comment to the table

        Parameters
        ----------
        table_name : str
            Name of table
        comment: str
            Description of table's purpose
        """
        owner = self.get_table_owner(schema_table)
        if owner == self.__get_current_user():
            spark.sql(f"ALTER TABLE {schema_table} SET TBLPROPERTIES ('comment' = '{comment}')")
        else:
            self.logger.info(f"You are not the owner of {schema_table}, you can't comment it!")
        


    # Get files path for CICD pipeline
    def get_files_path(self):
        '''
        Returns the CI/CD pipeline files path for a project
        '''
        if os.getenv('volume_path') is None:
            return '../files/'
        else: 
            return os.getenv('volume_path')+'/files/'


    # Auxiliary functions
    def __get_current_user(self):
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')


    def __check_user_team(self):
        # Get username of user running notebook
        curr_user = self.__get_current_user()
        if curr_user != self.__team_service_principal:
            if not self.__team in self.__get_user_teams(curr_user):
                self.logger.info(f"The current user is not a member of the {os.environ.get('team')} team (https://confluence.teladoc.net/display/LVGO/Azure+DS+Team+Mappings). Schema and table creation will be disabled.")
                self.__creation_enabled = False
        else:
            self.logger.info(f"Team service principal ({curr_user}) verified. Skipping user team check..")


    # Create schema with tags
    def __create_and_tag_schema(self, schema, managed_location):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} MANAGED LOCATION '{managed_location}'")
        self.__tag_schema(schema)


    def __get_user_teams(self, user_email):
        # Return the 
        out = spark.sql(f"SELECT * FROM ptanapii.ml_operations.ds_user_to_team WHERE email == '{self.__get_current_user()}'")
        return out.select('team').rdd.flatMap(lambda x: x).collect()


    def __get_team_manager_and_tech_leads(self):
        out = spark.sql(f"""SELECT * FROM ptanapii.ml_operations.ds_user_to_team WHERE team == '{self.__team}' AND role in ('manager', 'tech_lead')""")
        return out.select('email').rdd.flatMap(lambda x: x).collect()
    

    def __get_team_info(self):
        # Returns team package and service principal for the current environment and privacy level
        team_info = spark.sql(f"SELECT * FROM ptanapii.ml_operations.sp_package_mapping WHERE team = '{self.__team}' AND env = '{self.__env}' AND privacy = '{self.privacy_level}'")
        
        # Ensures only one package exists for the current environment variables
        assert(team_info.count() == 1), 'ERROR: team package lookup returned an unexpcted number of results.'

        # Fetch team package and service principal app id from returned row
        team_package, team_service_principal = team_info.select('package_name').first()['package_name'], team_info.select('sp_uuid').first()['sp_uuid']

        # Validated that this 
        if not team_package: self.logger.warning('WARNING: No team package found for user.')
        if not team_service_principal: self.logger.warning('WARNING: No team service principal found for user.')
        return team_package, team_service_principal


    def __get_object_permissions(self, object_type, role):
        # Return the appropriate schema permission for the designated role
        permissions_df = spark.sql(
            f"""
            SELECT permissions 
            FROM ptanapii.ml_operations.permission_hierarchy
            WHERE role = '{role}'
            AND environment = '{self.__env}'
            AND object_type = '{object_type}'
            """)
        return json.loads(permissions_df.first()['permissions'])

    def __grant_object_permissions(self, role, object_type, object_name, users):
        # Grant schema permissions based on role
        permissions = self.__get_object_permissions(object_type, role)
        self.logger.debug(f"Granting {','.join(permissions)} on {object_name} to {users}...")
        if isinstance(users, str):
            spark.sql(f"GRANT {','.join(permissions)} ON {object_type} {object_name} TO `{users}`")
        elif isinstance(users, list):
            for user in users:
                try:
                    spark.sql(f"GRANT {','.join(permissions)} ON {object_type} {object_name} TO `{user}`")
                except AnalysisException as e:
                    self.logger.info(e)
        else:
            raise TypeError(f'Error: Type {type(users)} is not accepted by this function.')

    def __transfer_object_ownership(self, object_type, object_name, new_owner):
        # Transfer volume ownership to service principal
        self.logger.info(f"Promoting ({new_owner}) to owner of {object_name}...")
        spark.sql(f"ALTER {object_type} {object_name} SET OWNER TO `{new_owner}`")


    # Permission assignment wrapper functions

    def __assign_object_permissions(self, object_type, object_name):
        # Check if schema owner is the current user
        if self.get_object_owner(object_type, object_name) == self.__get_current_user():
            # Grant schema permissions to team members, tech leads and managers, and creator
            self.__grant_object_permissions('member', object_type, object_name, self.__team_package)
            self.__grant_object_permissions('tech_lead', object_type, object_name, self.__get_team_manager_and_tech_leads())
            self.__grant_object_permissions('creator', object_type, object_name, self.__get_current_user())

            # Transfer ownership to team service principal
            self.__transfer_object_ownership(object_type, object_name, self.__team_service_principal)
        else:
            self.logger.debug(f'{object_type} permissions already set. Skipping...')


    def __check_environment_settings(self):
        # Ensure variables are assigned correctly
        required_vars = ['env', 'team', 'project']
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        if missing_vars:
            raise EnvironmentError(f"Missing environment variables : {', '.join(missing_vars)}. Please define these in your cluster variables.")
        assert(self.privacy_level.lower() in ['pii', 'npii']), "ERROR: Specified privacy level must be 'npii' or 'pii'."


    def __create_and_configure_schema(
        self, 
        schema_type: str, 
        comment: str = None):
        """
        Helper method to create a schema and grant permissions if it doesn't already exist.

        :param schema_name: Name of the schema to create.
        :param location: Location URL for the schema.
        """

        env_config = self.location_config.get(self.__env)
        if schema_type == 'feature':
            schema_name = f"{get_schema_prefix(self.__env)}feature{self.privacy_level}.{self.__project}"
            schema_location = env_config[self.privacy_level]['feature']
            volume_location = env_config[self.privacy_level]['volume_feature'] + self.__project
        elif schema_type == 'analytics':
            schema_name = f"{get_schema_prefix(self.__env)}ana{self.privacy_level}.{self.__project}"
            schema_location = env_config[self.privacy_level]['analytics']
            volume_location = env_config[self.privacy_level]['volume_analytics'] + self.__project

        if not spark.catalog.databaseExists(schema_name):
            self.logger.info(f"No existing schema detected. Creating {schema_name}...")
            self.__create_and_tag_schema(schema_name, schema_location)
        else:
            self.logger.info("Existing schema detected. Skipping creation...")
        
        self.__create_and_configure_volume(
            schema_name = schema_name, 
            volume_name = f"{schema_name}.{self.__project}_volume",
            volume_location = volume_location)
        
        # Comment schema with description
        if self.get_schema_owner(schema_name) == self.__get_current_user():
            self.comment_schema(schema_name, comment)

        # Update schema permissions and trasnfer ownership
        self.__assign_object_permissions('schema', schema_name)


    def __update_schema_permissions(self):
        self.logger.info('Updating Schema permissions...')
        if self.get_feature_schema():
            print(self.get_feature_schema())
            self.__assign_object_permissions('schema', self.get_feature_schema())
            self.__assign_object_permissions('volume', self.get_feature_schema() + f'.{self.__project}_volume')
        if self.get_ana_schema():
            self.__assign_object_permissions('schema', self.get_ana_schema())
            self.__assign_object_permissions('volume', self.get_ana_schema() + f'.{self.__project}_volume')

    def __create_and_configure_volume(
        self, 
        schema_name:str, 
        volume_name: str,
        volume_location: str):
        """
        Helper method to create a volume and grant permissions if it doesn't already exist.

        :param schema_name: Name of the schema.
        :param volume_name: Name of the volume to create.
        :param volume_location: Location URL for the volume.
        """
        self.__create_project_volume(
            schema_name=schema_name, 
            volume_name=volume_name,
            volume_location=volume_location)
        self.__assign_object_permissions('volume', volume_name)
        
    
    def __define_location_config(self):
        return {
            'prodtrain': {
                'pii': {
                    'feature': 'abfss://featurestrpiidsdata@studfanapiipteu206.dfs.core.windows.net/',
                    'analytics': 'abfss://analyticsstrpiidsdata@studfanapiipteu206.dfs.core.windows.net/',
                    'volume_feature': 'abfss://featurestrpiids@studfanapiipteu206.dfs.core.windows.net/',
                    'volume_analytics': 'abfss://analyticsstrpiids@studfanapiipteu206.dfs.core.windows.net/'
                },
                'npii': {
                    'feature': 'abfss://featurestrnpiidsdata@studfananpiipteu205.dfs.core.windows.net/',
                    'analytics': 'abfss://analyticsstrnpiidsdata@studfananpiipteu205.dfs.core.windows.net/',
                    'volume_feature': 'abfss://featurestrnpiids@studfananpiipteu205.dfs.core.windows.net/',
                    'volume_analytics': 'abfss://analyticsstrnpiids@studfananpiipteu205.dfs.core.windows.net/'
                }
            },
            'prod': {
                'pii': {
                    'feature': 'abfss://featurestrpiidsdata@studfanapiipreu206.dfs.core.windows.net/',
                    'analytics': 'abfss://analyticsstrpiidsdata@studfanapiipreu206.dfs.core.windows.net/',
                    'volume_feature': 'abfss://featurestrpiids@studfanapiipreu206.dfs.core.windows.net/',
                    'volume_analytics': 'abfss://analyticsstrpiids@studfanapiipreu206.dfs.core.windows.net/'
                },
                'npii': {
                    'feature': 'abfss://featurestrnpiidsdata@studfananpiipreu205.dfs.core.windows.net/',
                    'analytics': 'abfss://analyticsstrnpiidsdata@studfananpiipreu205.dfs.core.windows.net/',
                    'volume_feature': 'abfss://featurestrnpiids@studfananpiipreu205.dfs.core.windows.net/',
                    'volume_analytics': 'abfss://analyticsstrnpiids@studfananpiipreu205.dfs.core.windows.net/'
                }
            }
        }


    def __create_project_volume(
        self, 
        schema_name:str, 
        volume_name:str, 
        volume_location:str):
        """
        Create a single volume in the feature schema of the project. One schema should have only one volume.

        - If there already exists more than one volume, raise an error. Possible solution is to ask for admin help for deletion.
        - If the volume is in wrong location, raise an error. Possible solution is to ask for admin help for deletion.
        """
        try:
            if len(spark.sql(f"""SHOW VOLUMES IN {schema_name}""").toPandas()) > 1:
                self.logger.exception("Schema already has more than one volume. One schema should have only one volume, please do necessary deletion.")
            volume_info = spark.sql(f"""DESCRIBE VOLUME {volume_name}""").toPandas()
            if volume_info['storage_location'].values[0] != volume_location:
                self.logger.exception("Wrong volume location. Please do necessary deletion.")
            else:
                self.logger.info(f"Volume {volume_name} already exists.")
        except:
            # Create a single volume
            sql_query = f"""
            CREATE EXTERNAL VOLUME {volume_name}
            LOCATION '{volume_location}'
            """
            spark.sql(sql_query)
            self.logger.info(f"Volume {volume_name} created.")
            # Add tags
            spark.sql(f"ALTER VOLUME {volume_name} SET TAGS ('team'='{self.__team}','project'='{self.__project}')")

    
    @keyword_only
    def __create_delta_table(
        self, 
        df, 
        table: str, 
        primary_keys: Optional[Union[str, List[str]]] = None,
        schema_type: str = "feature",
        comment: str = None, 
        mode: str = 'overwrite',
        writing_schema_option: str = 'overwriteSchema'):
        """
        Create a Delta table in the specified schema in Databricks. Keyword-only.

        Parameters
        ----------
        df : DataFrame
            Spark dataframe to be saved.
        table : str
            Name of the table.
        primary_keys : list
            List of primary keys.
        schema_type : str, optional
            Type of the schema ('analytics' or 'feature'), defaults to 'feature'.
        comment : str, optional
            Description of the table.
        mode : str, optional
            Writing mode ('overwrite', 'append', 'ignore', 'error'), defaults to 'overwrite', see Spark documentation.
        writing_schema_option : str, optional
            Spark writing schema option ('mergeSchema', 'overwriteSchema'), defaults to 'overwriteSchema', see Spark documentation.
        """

        if not table:
            raise ValueError("Table name must be provided.")

        schema = self.__get_schema(schema_type)
        if not schema:
            raise ValueError("Schema must be created. Call create_project_schemas() before writing any tables.")
        self.__update_schema_permissions()

        full_table_name = f"{schema}.{table}"
        
        (df.write.format("delta")
            .option(writing_schema_option, "true")
            .option("enableChangeDataFeed", "true") 
            .mode(mode)
            .saveAsTable(full_table_name)
        )
        
        self.__tag_table(full_table_name)
        if comment:
            self.comment_table(full_table_name, comment)

        spark.sql(f"OPTIMIZE {full_table_name}")

        if primary_keys: 
            self.__add_table_primary_keys(full_table_name, primary_keys)

        if self.get_table_owner(full_table_name) == self.__get_current_user():
            self.__assign_object_permissions('table', full_table_name)
        self.logger.info(f"Delta Table {full_table_name} created with mode {mode}.")


    @keyword_only
    def __create_fstore_table(self, df, table: str, 
                            primary_keys: Optional[Union[str, List[str]]] = None,
                            timestamp_keys: Optional[Union[str, List[str]]] = None, 
                            partition_columns: Optional[Union[str, List[str]]] = None, 
                            comment: str = None,
                            mode: str = 'overwrite'):
        
        """
        Create a feature store table in the specified schema in Databricks. Keyword-only.

        Parameters
        ----------
        df : DataFrame
            Spark dataframe to be saved.
        table : str
            Name of the table.
        primary_keys : list
            List of primary keys.
        timestamp_keys : list
            List of timestamp keys.
        partition_columns : list
            List of partition columns.
        comment : str, optional
            Description of the table.
        mode : str, optional
            Writing mode ('overwrite', 'merge),  defaults to 'overwrite'. "overwrite" updates the whole table, while "merge"`` will update the rows in ``df`` into the feature table.
        """
        
        if not primary_keys:
            raise ValueError("Primary keys must be provided.")
        
        if not table:
            raise ValueError("Table name must be provided.")
        
        schema = self.get_feature_schema()
        if not schema:
            raise ValueError("Schema must be created. Call create_project_schemas() before writing any tables.")

        full_table_name = f"{schema}.{table}"
        
        if mode == 'overwrite' and spark.sql(f"SHOW TABLES IN {schema} LIKE '{table}'").count() > 0:
            owner = self.get_table_owner(full_table_name)
            if owner == self.__get_current_user():
                spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            else:
                self.logger.info(f"You can't overwrite table {full_table_name} as you are not the owner, create a new table or use mode='merge'!")

        feature_table_exists = self.__check_feature_store_table_exists(full_table_name)

        if feature_table_exists:
            self.feature_store.write_table(
                name=full_table_name,
                df=df,
            )
        else:
            feature_table = self.feature_store.create_table(
                name=full_table_name,
                primary_keys=primary_keys,
                timestamp_keys=timestamp_keys,
                partition_columns=partition_columns,
                df=df,
                schema=df.schema
            )

        self.__tag_table(full_table_name)

        if self.get_table_owner(full_table_name) == self.__get_current_user():
            self.__assign_object_permissions('table', full_table_name)

        if comment:
            self.comment_table(full_table_name, comment)

        spark.sql(f"OPTIMIZE {full_table_name}")
        
        if feature_table_exists:
            self.logger.info(f"Feature Store Table {full_table_name} created with mode {mode}.")


    def __get_schema(self, schema_type: str):
        """
        Get the schema based on the schema type.

        Parameters
        ----------
        schema_type : str
            Type of the schema ('analytics' or 'feature').

        Returns
        -------
        str
            The schema if it exists, None otherwise.
        """
        prefix = get_schema_prefix(self.__env)
        schema_suffix = "feature" if schema_type == 'feature' else "ana"
        schema = f"{prefix}{schema_suffix}{self.privacy_level}.{self.__project}"
        
        return schema if spark.catalog.databaseExists(schema) else None


    def __add_table_primary_keys(self, table: str, primary_keys: Union[str, List[str]]):
        
        owner = self.get_table_owner(table)
        if owner == self.__get_current_user():
            if primary_keys:
                if isinstance(primary_keys, str):
                    primary_keys = [primary_keys]
                text_keys = ', '.join(primary_keys)
                for key in primary_keys:
                    spark.sql(f'ALTER TABLE {table} ALTER COLUMN {key} SET NOT NULL')
                self.__drop_pk_constraint(table)
                table_name = table.split('.')[-1]
                spark.sql(f'ALTER TABLE {table} ADD CONSTRAINT {table_name}_pk PRIMARY KEY({text_keys})')
        else: 
            self.logger.info(f"You are not the owner of {table}, you can't add primary keys to it!")


    def __drop_pk_constraint(self, table: str):
        constraint = None
        desc = spark.sql(f"DESC TABLE EXTENDED {table}")
        desc = desc.filter(F.col("col_name").rlike('_pk'))
        if desc.count() > 0:
            constraint = desc.select("col_name").collect()[0][0]
        if constraint:
            spark.sql(f'ALTER TABLE {table} DROP CONSTRAINT {constraint}')


    def __check_feature_store_table_exists(self, full_table_name):
        try:
            self.feature_store.get_table(name=full_table_name)
            return True
        except Exception as e:
            return False


    # Tag schema with team and project
    def __tag_schema(self, schema_name):
        spark.sql(f"ALTER SCHEMA {schema_name} SET TAGS ('team'='{self.__team}','project'='{self.__project}')")


    # Tag table with team and project
    def __tag_table(self, table_name):
        spark.sql(f"ALTER TABLE {table_name} SET TAGS ('team'='{self.__team}','project'='{self.__project}')")



def get_schema_prefix(env):
    if env == 'prodtrain' : return 'pt'
    elif env == 'prod' : return 'prod'
    else : raise ValueError(f"'{env}' is not an accepted environment. Environment must be either 'prod' or 'prodtrain'.")
