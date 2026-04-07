"""
Function to unzip data and make it available
"""
import module_l1_001
import os

# Set the root directory variable using a relative path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

ZIP_FILENAME = os.path.join(ROOT_DIR, 'data','data.zip')
EXTRACT_TO = os.path.join(ROOT_DIR,'data')

def data_loader_func_003(param_023=ZIP_FILENAME, param_024=EXTRACT_TO):
    """
    Function to unzip the downloaded data
    Args:
      param_023: zipfile path, a default is used if not specified
      param_024: Path where the unzipped and extracted data is available
    Returns:
      param_024: filepath where the data is available
    """
    try:
        with zipfile.ZipFile(param_023, 'r') as zip_ref:
            zip_ref.extractall(param_024)
        print(f"File {param_023} successfully unzipped to {param_024}")
    except zipfile.BadZipFile:
        print(f"Failed to unzip {param_023}")
    # Return unzipped file
    unzipped_file =  os.path.join(param_024, 'Online Retail.xlsx')
    return unzipped_file

if __name__ == "__main__":
    UNZIPPED_FILE = data_loader_func_003(ZIP_FILENAME, EXTRACT_TO)
