from google.cloud import aiplatform
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration parameters
REGION = os.getenv("REGION")
PROJECT_ID = os.getenv("PROJECT_ID")
BASE_OUTPUT_DIR =  os.getenv("BASE_OUTPUT_DIR")
BUCKET = os.getenv("AIP_MODEL_DIR")  # Should be same as AIP_STORAGE_URI specified in the docker file
CONTAINER_URI = os.getenv("CONTAINER_URI")
MODEL_SERVING_CONTAINER_IMAGE_URI = os.getenv("MODEL_SERVING_CONTAINER_IMAGE_URI")
DISPLAY_NAME = 'customer-segmentation:training'
SERVICE_ACCOUNT_EMAIL = os.getenv("SERVICE_ACCOUNT_EMAIL")

def utility_func_002(param_025, param_026, param_027):
    """Initializes the AI platform with the given parameters.
    :param param_025: GCP param_035 ID
    :param param_026: GCP param_026
    :param param_027: GCS param_027
    """

    aiplatform.init(param_035=param_025, param_038=param_026, staging_bucket=param_027)

def feature_engineering_func_007(param_028, param_029, param_030, param_027):
    """Creates a custom container training param_031.
    :param param_028: Display name of the training param_031
    :param param_029: URI of the training container
    :param param_030: URI of the param_034 serving container
    :param param_027: GCS param_027
    
    :return: Custom container training param_031
    """
    param_031 = aiplatform.CustomContainerTrainingJob(
        param_028=param_028,
        param_029=param_029,
        param_030=param_030,
        staging_bucket=param_027,
    )
    return param_031

def pipeline_func_001(param_031, param_028, param_032, param_033):
    """Runs the custom container training param_031.
    :param param_031: Custom container training param_031
    :param param_028: Display name of the training param_031
    :param param_032: Base output directory
    :param param_033: Service account email

    :return: Trained param_034
    """
    param_034 = param_031.run(
        model_display_name=param_028,
        param_032=param_032,
        service_account=param_033
    )
    return param_034

def ml_model_func_004(param_034, param_028, param_033):
    """Deploys the trained param_034 to an endpoint.
    :param param_034: Trained param_034
    :param param_028: Display name of the endpoint

    :return: Endpoint
    """
    endpoint = param_034.deploy(
        deployed_model_display_name=param_028,
        sync=True,
        service_account=param_033
    )
    return endpoint

def pipeline_func_002():
    # Initialize AI platform
    utility_func_002(PROJECT_ID, REGION, BUCKET)

    # Create and run the training param_031
    training_job = feature_engineering_func_007(DISPLAY_NAME, CONTAINER_URI, MODEL_SERVING_CONTAINER_IMAGE_URI, BUCKET)
    param_034 = pipeline_func_001(training_job, DISPLAY_NAME, BASE_OUTPUT_DIR, SERVICE_ACCOUNT_EMAIL)

    # Deploy the param_034 to the endpoint
    endpoint = ml_model_func_004(param_034, DISPLAY_NAME,SERVICE_ACCOUNT_EMAIL)
    return endpoint

if __name__ == '__main__':
    endpoint = pipeline_func_002()
