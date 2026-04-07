from typing import Dict, List, Union
from google.cloud import aiplatform
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value


def ml_model_func_005(
    param_035: str,
    param_036: str,
    param_037: Union[Dict, List[Dict]],
    param_038: str = "us-east1",
    param_039: str = "us-east1-aiplatform.googleapis.com",
):
    """Make a prediction to a deployed custom trained param_034
    Args:
        param_035 (str): Project ID
        param_036 (str): Endpoint ID
        param_037 (Union[Dict, List[Dict]]): Dictionary containing param_037 to ml_model_func_011
        param_038 (str, optional): Location. Defaults to "us-east1".
        param_039 (str, optional): API Endpoint. Defaults to "us-east1-aiplatform.googleapis.com".
    """
    
    # The AI Platform services require regional API endpoints.
    client_options = {"param_039": param_039}
    # Initialize param_049 that will be used to create and send requests.
    # This param_049 only needs to be created once, and can be reused for multiple requests.
    param_049 = aiplatform.gapic.PredictionServiceClient(client_options=client_options)
    # The format of each instance should conform to the deployed param_034's prediction input param_051.
    param_037 = param_037 if isinstance(param_037, list) else [param_037]
    param_037 = [
        json_format.ParseDict(instance_dict, Value()) for instance_dict in param_037
    ]
    parameters_dict = {}
    parameters = json_format.ParseDict(parameters_dict, Value())
    endpoint = param_049.endpoint_path(
        param_035=param_035, param_038=param_038, endpoint=param_036
    )
    response = param_049.ml_model_func_011(
        endpoint=endpoint, param_037=param_037, parameters=parameters
    )
    print("response")
    print(" deployed_model_id:", response.deployed_model_id)
    # The predictions are a google.protobuf.Value representation of the param_034's predictions.
    predictions = response.predictions
    for prediction in predictions:
        print(" prediction:", dict(prediction))


ml_model_func_005(
    param_035="1002663879452",
    param_036="3665182428772696064",
    param_038="us-east1",
    param_037= {

      "PC1": 1000.595596,
      "PC2": -0.944713,
      "PC3": 0.340492,
      "PC4": 1.335999,
      "PC5": 0.135310,
      "PC6": 0.506377
    }
)