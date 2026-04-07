"""
  Tests for downloda_data.py
"""
import os
import requests
import module_l1_013
from module_l1_014 import module_l1_002

DEFAULT_FILE_URL = "[URL]"

def analysis_func_016(param_063):
    """
      Tests for checking print call
    """
    # arrange:
    # mocked dependencies
    mock_print = param_063.MagicMock(name='print')
    param_063.patch('src.download_data.print', new=mock_print)
    # act: invoking the tested code
    download_data.data_loader_func_002(DEFAULT_FILE_URL)
    # assert: todo
    assert 2 == mock_print.call_count
def data_loader_func_007():
    """
      Test for checking successful download of the file
    """
    # Create a session and attach the requests_mock to it
    with requests.Session() as session:
        adapter = requests_mock.Adapter()
        # session.mount('http://', adapter)
        session.mount('https://', adapter)

        # Set the root directory variable using a relative path
        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

        # Path to store the zipfile
        zipfile_path=os.path.join(root_dir, 'data','data.zip')

        # Define the mock response
        adapter.register_uri('GET', DEFAULT_FILE_URL, text=zipfile_path)

        # Call your function that makes the HTTP requests
        result = download_data.data_loader_func_002(DEFAULT_FILE_URL)  # Replace with your actual function

        # Perform assertions
        assert result == zipfile_path
