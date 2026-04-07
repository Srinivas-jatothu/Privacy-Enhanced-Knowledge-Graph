"""
Function to download and ingest the data file
"""
import os
import requests

DEFAULT_FILE_URL = "[URL]"

def data_loader_func_002(param_013=DEFAULT_FILE_URL):
    """
    Function to download file from URL
    Args:
        param_013: URL of the file, A default is used if not specified
    Returns:
        zipfile_path: The zipped file path to the data
    """
    # Send an HTTP GET request to the URL
    response = requests.get(param_013, timeout=30)

    print("Downloading Data")

    # Set the root directory variable using a relative path
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    # Path to store the zipfile
    zipfile_path=os.path.join(root_dir, 'data','data.zip')
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Save file to data
        with open(zipfile_path, "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully. Zip file available under {zipfile_path}")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")

    return zipfile_path

if __name__ == "__main__":
    ZIPFILE_PATH = data_loader_func_002("[URL]")
    