"""
Modular script to perform PCA on incoming data.
Scripts checks PC ranging from 2 to 8 until cumulative \
explained variance of threshold is achieved.
Input: Path string to load pickle/parquet file.
Output: Path string for parquet file.
"""
import os
import json
import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from module_l2_020 import yes_no_dialog

#Loading Config File
PAR_DIRECTORY = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(PAR_DIRECTORY,"config","feature_processing.json")
with open(config_path, "rb") as f:
    config = json.load(f).get("pca")

#Global variables
__INGESTPATH__ = os.path.join(PAR_DIRECTORY,config.get("ingest_path")) #default path in config
__OUTPUTPATH__ = os.path.join(PAR_DIRECTORY,config.get("output_path")) #default path in config
NOT_COLUMNS = config.get("columns_not_considered")#columns wrt to defaults in config
CVR_THRESHOLD = config.get("cvr_threshold")

def ml_model_func_001(param_006=__INGESTPATH__,param_007=__OUTPUTPATH__,\
    param_015=NOT_COLUMNS,param_016=CVR_THRESHOLD):
    """
    Global variables(can only be changed through Config file)
    Args:
    paths(str1,str2): str1:ingest_path, str2:output_path
    CVR_THRESHOLDold[float]: cumulative explained variance threshold for variance
    param_015: column to be ommitted for pca
    """
    #Placeholder for data
    data = None

    #File Loading
    try:
        data = pd.read_parquet(param_006)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found at {param_006}.") from None

    #Check datatype
    if not isinstance(data,pd.DataFrame):
        raise TypeError("File did not load DataFrame correctly.") from None

    #Check all required columns exist in DF
    try:
        assert all(col in data.columns for col in param_015)
    except AssertionError as exc:
        raise KeyError("Column not found in Dataframe.") from exc

    #Check columns <4, PCA not required
    if len(data.columns)<4:
        raise ValueError("Columns less than 4. Need not proceed with PCA.")

    #Value check for CVR_THRESHOLD
    if not 0<param_016<1:
        raise ValueError("CVR_THRESHOLD should lie between 0 and 1.")

    #Selecting columns in data for PCA
    data.set_index('CustomerID', inplace=True)
    data = data.drop(param_015, axis=1)

    print(data)

    n = ml_model_func_002(data,param_016)
    pca = PCA(n_components=n).fit(data)

    # Getting post-PCA data
    reduced_data = pca.transform(data)
    columns=[f'PC{i+1}' for i in range(pca.n_components_)]
    pca_transformed_data = pd.DataFrame(reduced_data, columns=columns)
    pca_transformed_data.index = data.index
    print(pca_transformed_data)

    #saving data as parquet
    try:
        p=os.path.dirname(param_007)
        if not os.path.exists(p):
            os.makedirs(p)
        pca_transformed_data.to_parquet(param_007)
        print(f"File saved successfully at Path: {param_007}.")
    except FileExistsError:
        result = yes_no_dialog(
            title='File Exists Error',
            text="Existing file in use. Please close to overwrite the file. Error:.").run()
        if result:
            pca_transformed_data.to_parquet(param_007)
            print(f"File saved successfully at Path: {param_007}.")
        else:
            print(f"Could not save File at Path: {param_007}.")

    return param_007

def ml_model_func_002(data,param_017):
    """
    Refactoring for iterative check of n_components 
    to cover threshold cumulative explained variance.
    data: input data for pca
    param_017[float]: cumulative explained variance threshold for variance
    """
    #PCA
    n_components_range=np.arange(2,9)
    n=[]
    cumulative_var_ratio=[0]
    for n_components in n_components_range:
        if cumulative_var_ratio[-1] <= param_017:
            pca_check=PCA(n_components=n_components).fit(data)
            var_ratio = pca_check.explained_variance_ratio_
            cumulative_var_ratio.append(np.cumsum(var_ratio)[n_components-1])
            n.append(n_components)
            print(n[-1],cumulative_var_ratio[-1])
        elif n_components==n_components_range[-1]:
            n=n_components
            break
        else:
            n=n_components
            break
    print(n,cumulative_var_ratio)
    return n
