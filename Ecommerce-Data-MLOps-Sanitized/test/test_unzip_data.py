"""
Function to test the unzip_data functions
"""
import os
from module_l1_014 import module_l1_003

# Define constants or variables for testing
# Set the root directory variable using a relative path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

ZIP_FILENAME = os.path.join(ROOT_DIR, 'data','data.zip')
EXTRACT_TO = os.path.join(ROOT_DIR,'data')
BAD_ZIP_FILENAME = os.path.join(ROOT_DIR, 'data', 'bad.zip')

# Test for successful unzipping
def analysis_func_033():
    """
      Test for successful unzipping
    """
    # Call the function to unzip a valid file
    unzipped_file = unzip_data.data_loader_func_003(ZIP_FILENAME, EXTRACT_TO)

    # Check if the function returned the expected unzipped file path
    assert unzipped_file == os.path.join(EXTRACT_TO, 'Online Retail.xlsx')

    # Check if the unzipped file exists
    assert os.path.isfile(unzipped_file)

# Test for handling a bad zip file
def analysis_func_034(param_064, param_065):
    """
      Test for handling a bad zip file
    """
    # Create a bad zip file in the temporary directory
    with open(BAD_ZIP_FILENAME, "wb") as file:
        file.write(b"This is not a valid zip file")

    # Create a temporary directory for testing
    test_dir = param_064 / "test_dir"
    test_dir.mkdir()
    # Call the function to unzip a bad zip file
    unzip_data.data_loader_func_003(BAD_ZIP_FILENAME, test_dir)

    # Check if the function printed the appropriate error message
    captured = param_065.readouterr()
    assert "Failed to unzip" in captured.out
