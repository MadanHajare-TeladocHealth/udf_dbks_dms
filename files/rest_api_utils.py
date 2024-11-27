import requests
import os
from databricks.sdk.runtime import *


class DatabricksRestMessenger():
    """
    Wrapper class to manage communication with the the databricks via REST API. Automatically configures credentials, url, and headers for REST API requests.
    """
    def __init__(self):
        self.base_url = self._configure_base_url()
        self.headers = self._define_headers()


    def _configure_base_url(self):
        """
        Helper method to create a schema and grant permissions if it doesn't already exist.
        """
        # base url for databricks
        if os.environ.get('env') == 'prodtrain':
            return 'https://adb-4022166418081681.1.azuredatabricks.net/api/2.0/'
        elif os.environ.get('env') == 'prod':
            return 'https://adb-5718777928560124.4.azuredatabricks.net/api/2.0/'
        else:
            raise ValueError(f"No environment defined. Please set env=prodtrain as an environment variable in your cluster variables.")
        

    def _define_headers(self):
        """
        Helper method to create a schema and grant permissions if it doesn't already exist.
        """
        return {
            "Authorization": f"Bearer {dbutils.secrets.get(scope='ml_platform', key='sp_pat')}",
            "Content-Type": "application/json",
        }


    def _get_reader_pack(self): 
        """
        Retrieves the user list of the data science group from the environment.
        """
        if os.environ.get('env') == 'prodtrain':
            return "AZ-UDF-DataScience-Reader-Pr-Training"
        elif os.environ.get('env') == 'prod':
            return "AZ-UDF-DataScience-Reader-Prod-NA"
        else:
            raise ValueError(f"No environment defined. Please set env=prodtrain as an environment variable in your cluster variables.")


    def update_endpoint_permissions(
        self, 
        endpoint_id:str, 
        reader_pack:str = None
        ):
        """
        Update the config of endpoint to be queryable by all Datascience.
        """
        if not reader_pack:
            reader_pack = self._get_reader_pack()
        url = self.base_url + "permissions/serving-endpoints/" + endpoint_id
        payload = {
            "access_control_list": [
                {
                    "group_name": reader_pack,
                    "permission_level": "CAN_QUERY"
                }
            ]
        }
        response = requests.request("PATCH", url, headers=self.headers, json=payload)
        print(f"Endpoint Access updated:{response.json()}")


    def _validate_json_response(self, json_payload):
        """
        Reads the json response from server and throws an error if the payload contains an error code.
        """
        if not ("error_code" in json_payload.json()):
            pass
        else:
            raise requests.exceptions.RequestException(f'The server has returned an error: {json_payload.json()}')


    def get_existing_endpoints(self, details=False):
        """
        Returns existing endpoints in databricks. Available as just endpoing names or with details.
        :param details: toggle to print out endpoint details
        """
        # get the list of endpoint already present
        endpoint_list_resp = requests.get(self.base_url + "serving-endpoints", headers=self.headers, data='').json()['endpoints']
        return endpoint_list_resp if details else [endpoint['name'] for endpoint in endpoint_list_resp]


    def create_model_endpoint(
        self,
        endpoint_name,
        uc_model_name,
        model_task,
        model_version,
        uc_inference_table,
        compute_type='CPU',
        workload_type="Small",
        scale_zero=True,
        traffic=100
        ):
        """
        Creates a serving endpoint for models.
        """
        # Split UC catalog names into separate pieces
        infer_catalog_name,infer_schema,infer_table_name = uc_inference_table.split('.')
        _,_,model_name=uc_model_name.split('.')

        if endpoint_name not in self.get_existing_endpoints(details=False):
            endpoint_details = {
                "name": endpoint_name,
                "config": {
                    "served_entities": [
                    {
                        "name": f"{model_name}_{model_version}",
                        "entity_name": uc_model_name,
                        "entity_version": model_version,
                        "workload_size": workload_type,
                        "workload_type": compute_type,
                        "scale_to_zero_enabled": scale_zero,
                    }
                    ],
                    '''"auto_capture_config": {
                        "catalog_name": infer_catalog_name,
                        "schema_name": infer_schema,
                        "table_name_prefix":infer_table_name,
                    },'''
                    "traffic_config": {
                        "routes": [
                            {
                                "served_model_name": f"{model_name}_{model_version}",
                                "traffic_percentage": traffic
                            }
                        ]
                    }
                },
                "tags": [
                    {
                    "key": "team",
                    "value": os.environ.get('team')
                    },
                    {
                    "key": "project",
                    "value": os.environ.get('project')
                    },
                    {
                    "key": "task",
                    "value": model_task
                    }
                ]
            }
            endpoint_resp = requests.post(self.base_url + "serving-endpoints", headers=self.headers, json=endpoint_details)
            self._validate_json_response(endpoint_resp)
            print(f"Endpoint '{endpoint_name}' created.")
            endpoint_id = endpoint_resp.json()["id"]
            self.update_endpoint_permissions(endpoint_id)
            # if ("inference_table_config" in endpoint_resp.json()):
            #     print("Inference Tables were successfully enabled with inference_table_config:", endpoint_resp.json()["inference_table_config"])
            # else:
            #     print("Inference Tables were not enabled. Please check the infer_catalog_name,infer_schema and infer_table_name values.")
        else:
            print(f"Endpoint '{endpoint_name}' already exists. Skipping creation...")


    def update_model_endpoint(
        self,
        endpoint_name,
        uc_model_name,
        model_version,
        compute_type='CPU',
        workload_type="Small",
        scale_zero=True,
        traffic=100
        ):
        """
        Updates an existing model endpoint.
        """
        _,_,model_name=uc_model_name.split('.')
        endpoint_update = {
            "served_entities": [
            {
                "name": f"{model_name}_{model_version}",
                "entity_name": uc_model_name,
                "entity_version": model_version,
                "workload_size": workload_type,
                "workload_type": compute_type,
                "scale_to_zero_enabled": scale_zero,
            }
            ],
            "traffic_config": {
                "routes": [
                    {
                        "served_model_name": f"{model_name}_{model_version}",
                        "traffic_percentage": traffic
                    }
                ]
            }
        }
        response = requests.request("PUT", self.base_url+"serving-endpoints/"+endpoint_name+"/config", headers=self.headers, json=endpoint_update)
        self._validate_json_response(response)
        print(f"Endpoint '{endpoint_name}' sucessfully updated.")


