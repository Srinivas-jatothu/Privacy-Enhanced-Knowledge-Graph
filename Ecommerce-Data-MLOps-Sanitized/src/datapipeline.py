"""
Modularized Data pipeline to form DAGs in the future
"""
from module_l1_002 import data_loader_func_002
from module_l1_003 import data_loader_func_003
from module_l1_004 import data_loader_func_001
from module_l1_005 import data_cleaning_func_003
from module_l1_006 import data_cleaning_func_002
from module_l1_007 import data_cleaning_func_006
from module_l1_008 import data_cleaning_func_001


if __name__ == "__main__":
    ZIPFILE_PATH = data_loader_func_002(
        """[URL]""")
    UNZIPPED_FILE = data_loader_func_003(ZIPFILE_PATH, 'data')
    LOADED_DATA_PATH = data_loader_func_001(param_012=UNZIPPED_FILE)
    AFTER_MISSING_PATH = data_cleaning_func_003(param_001=LOADED_DATA_PATH)
    AFTER_DUPLICATES_PATH = data_cleaning_func_002(param_001=AFTER_MISSING_PATH)
    AFTER_TRANSACTION_PATH = data_cleaning_func_006(param_001=AFTER_DUPLICATES_PATH)
    AFTER_ANOMALY_CODE_PATH = data_cleaning_func_001(param_001=AFTER_TRANSACTION_PATH)
