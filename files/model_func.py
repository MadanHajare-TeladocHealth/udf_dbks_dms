import mlflow
from mlflow.tracking import MlflowClient
import os
from pyspark import keyword_only
from databricks.feature_engineering import FeatureEngineeringClient
from uc_func import get_schema_prefix


class UnityCatalogModel():
    """
    Class for model registry

    
    Parameters
    ----------
    model_name : str
        The name of the model. Defaults to None.
    task : str
        The task of the model. Defaults to None.
    privacy_level : str
        The privacy level of the model. Defaults to 'pii'.
    """
    def __init__(self, model_name: str = None, task: str = None, privacy_level: str = 'pii'):
        self.__check_environment_settings()

        self.privacy_level = privacy_level

        self.env = os.getenv('env')
        self.team = os.getenv('team')
        self.project = os.getenv('project')
        self.schema = self.__get_schema(schema_type='feature')

        mlflow.set_registry_uri('databricks-uc')
        self.client = MlflowClient()
        self.model_name = self.__complete_model_name(model_name)
        self.task = task


    # Unlike the standard model registry, the /latest command is not usable in unity catalog, so this function uses the mlflow client to get the latest model version
    def get_model_latest_version(self, model_name=None):
        if model_name is None:
            model_name = self.model_name
        
        model_version_infos = None 
        model_version_infos = self.client.search_model_versions("name = '%s'" % model_name)

        if model_version_infos:
            new_model_version = str(max([int(model_version_info.version) for model_version_info in model_version_infos]))
            return new_model_version
        else:
            print(f'Model {model_name} not found!')


    @keyword_only
    def log_model(self, model, model_name=None, flavor=None, artifact_path: str = None, training_set=None, task: str = None, 
                  infer_input_example: bool = False):
        """
        Log a model in the model registry

        Parameters
        ----------
        model : mlflow model
            Model to be logged
        model_name : str, optional
            The name of the model. Defaults to None.
        flavor:
            Type of machine learning framework or library used to build the model. Some examples: mlflow.sklearn, mlflow.xgboost, mlflow.spark.
            Defaults to None.
        artifact_path : str, optional
            The path to the model artifact. Defaults to None.
        training_set : FeatureStoreTrainingSet
            Feature store training set. Defaults to None.
        task: str, optional
            The task of the model. Defaults to None.
        infer_input_example: bool, optional
            Whether to infer the input example. Defaults to False.


        Example
        -------
        from pyspark.ml import Pipeline
        from pyspark.ml.classification import LogisticRegression
        from pyspark.ml.feature import HashingTF, Tokenizer
        from model_func import UnityCatalogModel
        
        uc_modeling = UnityCatalogModel(model_name='test_model', task='classifier')
        
        hashingTF = HashingTF(inputCol="words", outputCol="features")
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        lr = LogisticRegression(maxIter=10, regParam=0.001)

        pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])
        model = pipeline.fit(train_df)

        uc_modeling.log_model(
            model=model,
            artifact_path="wine_quality_prediction",
            flavor=mlflow.spark,
            training_set=training_set
        )
        """

        if model_name is None:
            model_name = self.model_name
        if task is None:
            task = self.task

        fe = FeatureEngineeringClient()
        fe.log_model(
            model=model,
            artifact_path=artifact_path,
            flavor=flavor,
            training_set=training_set,
            registered_model_name=model_name,
            infer_input_example=infer_input_example
        )

        self.tag_latest_model(model_name, task)


    # get the uri of the latest model version
    def get_lastest_model_uri(self, model_name=None):

        if model_name is None:
            model_name = self.model_name
            
        version = None
        version = self.get_model_latest_version(model_name)

        if version:
            return f'models:/{model_name}/{version}'
    

    # All models in the model registry should be tagged with at minimum a team name and project name. For models, it is also best to include a task to define what the model is meant to do (ie: regression/classification). This can be done through accessing the mlflow client and using the function set_registered_model_tag.
    def tag_latest_model(self, model_name: str = None, task: str = None):

        if model_name is None:      
            model_name = self.model_name
    
        if task is None:
            task = self.task

        # set model tag
        self.client.set_registered_model_tag(model_name, 'team', os.environ.get('team'))
        self.client.set_registered_model_tag(model_name, 'project', os.environ.get('project'))
        self.client.set_registered_model_tag(model_name, 'task', task)

        # Set model version tag
        version = self.get_model_latest_version(model_name)
        self.client.set_model_version_tag(model_name, version, 'team', os.environ.get('team'))
        self.client.set_model_version_tag(model_name, version, 'project', os.environ.get('project'))
        self.client.set_model_version_tag(model_name, version, 'task', task)
        self.client.set_registered_model_alias(model_name, os.environ.get('env'), version)


    # model promotion function from prod train unity catalog to prod
    @keyword_only
    def model_promote(self, model_name, task, privacy_level):

        pt_feature_schema = f"ptfeature" + privacy_level + "." + os.environ.get('project')
        pr_feature_schema = f"prodfeature" + privacy_level + "." + os.environ.get('project')

        environment_from = f'{pt_feature_schema}.{model_name}'
        environment_to = f'{pr_feature_schema}.{model_name}'
        
        pt_alias = 'best'
        alias = "prod"

        copied_model_version = self.client.copy_model_version(
            src_model_uri = f"models:/{environment_from}@{pt_alias}",
            dst_name = environment_to,
        )
        self.tag_latest_model(environment_to, task)
        return copied_model_version


    @keyword_only
    def delete_model(self, model_name=None, version=None):
        """
        Delete a model from the model registry.

        Parameters
        ----------
        model_name : str, optional
            The name of the model. Defaults to None.
        version : int 
            The version of the model. Defaults to None.
        """

        if self.env == 'prod':
            raise PermissionError("Deleting models from prod is not permitted!")
        if model_name is None:
            model_name = self.model_name

        if version is None:
            raise ValueError("Model version is required!")
    
        self.client.delete_registered_model(model_name, version=str(version))
        print(f"Deleting model {model_name} version {version}!")


    def __check_environment_settings(self):
        required_vars = ['env', 'team', 'project']
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        if missing_vars:
            raise EnvironmentError(f"Missing environment variables : {', '.join(missing_vars)}. Please define these in your cluster variables.")
        

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
        prefix = get_schema_prefix(self.env)
        schema_suffix = "feature" if schema_type == 'feature' else "ana"
        schema = f"{prefix}{schema_suffix}{self.privacy_level}.{self.project}"
        
        return schema


    def __complete_model_name(self, model_name):

        if model_name is None:
            model_name = self.model_name

        return f"{self.schema}.{model_name}" 