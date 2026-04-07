"""
Inputs: {
    path: file saving param_038, 
    correlation threshold: threshold for determining high correlations}
Outputs: { 
    Image: png file with masked heatmap
    Matrix: Correlation Matrix as parquet}
Returns: None
"""
import os
from module_l2_020 import yes_no_dialog
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

# Reset background style
sns.set_style('whitegrid')

#Loading Config File
PAR_DIRECTORY = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#Global variables
__INGESTPATH__ = os.path.join(PAR_DIRECTORY,'data', 'processed','scaler_output.parquet')
__IMGPATH__ = os.path.join(PAR_DIRECTORY,'data', 'processed',"images",\
    "correlation_heatmap.png")
__PARQUETPATH__ = os.path.join(PAR_DIRECTORY,'data', 'processed', "correlation_matrix.parquet")
CORR_THRESH = 0.5
__OUTPUTPATH__=(__IMGPATH__,__PARQUETPATH__)

def analysis_func_001(param_006=__INGESTPATH__,param_007=__OUTPUTPATH__,\
    param_008=CORR_THRESH):
    """
    Global variables(can only be changed through Config file)
    Args:
    param_006(str): ingest_path, 
    param_007(str1,str2):
    cvr_threshold[float]: cumulative explained variance threshold for variance
    param_015: column to be ommitted for pca
    """
    #Placeholder for data
    data = None

    #Try to Load data from pickle
    try:
        data = pd.read_parquet(param_006)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found at {param_006}.") from None

    #Check datatype
    if not isinstance(data,pd.DataFrame):
        raise TypeError("File did not load DataFrame correctly.") from None

    print(data)

    #check save paths are strings
    try:
        assert isinstance(param_007[0],str)
    except AssertionError as ae:
        raise TypeError("Image Save Path should be a String!") from ae

    try:
        assert isinstance(param_007[0],str)
    except AssertionError as ae:
        raise TypeError("Parquet Save Path should be a String!") from ae

    #Value check for correlation_thresh
    if not 0<param_008<1:
        raise ValueError("param_016 should lie between 0 and 1.")

    # Calculate the correlation matrix, **Future deprecation warning**
    corr = data.corr()

    # Create a mask to only show the lower triangle of the matrix (since it's mirrored around its
    # top-left to bottom-right diagonal)
    mask = np.zeros_like(corr)
    mask[np.triu_indices_from(mask, k=1)] = True

    # Define a custom colormap
    colors = ['#ff6200', '#ffcaa8', 'white', '#ffcaa8', '#ff6200']
    my_cmap = LinearSegmentedColormap.from_list('custom_map', colors, N=256)

    # Plot the heatmap
    param_009=plt.figure(param_048=(10, 10))
    plt.title(f'Correlation Matrix, Correlation Threshold:{param_008}', fontsize=14)
    sns.heatmap(corr, mask=mask, cmap=my_cmap, annot=True, center=0, fmt='.2f', linewidths=2)

    #Save heatmap as image for reference
    data_saver_func_001(param_009,param_007[0])

    #saving corr matrix
    data_saver_func_002(corr,param_007[1])

def data_saver_func_001(param_009,path):
    """
    Saves the heatmap as png file.
    Inputs: data: data to be saved, path: file saving param_038
    Returns: None
    Executes: saving of png, except prompts the user to proceed if file open.
    Error Checks: FileNotFoundError
    """
    try:
        p=os.path.dirname(path)
        if not os.path.exists(p):
            os.makedirs(p)
        param_009.savefig(path)
        print(f"File saved successfully at Path: {path}.")
    except FileExistsError as fe:
        result = yes_no_dialog(
            title='File Exists Error',
            text=f"Existing file in use. Please close to overwrite the file. Error: {fe}.").run()
        if result:
            param_009.savefig(path)
        else:
            print(f"Could not save File at Path: {path}.")

def data_saver_func_002(data,path):
    """
    Saves the correlation matrix as parquet.
    Inputs: data: data to be saved, path: file saving param_038
    Returns: None
    Executes: raises Attribute error if data is not df as /
    to_parquet would not be executed.
    Error Checks: FileNotFoundError
    """
    try:
        p=os.path.dirname(path)
        if not os.path.exists(p):
            os.makedirs(p)
        data.to_parquet(path)
        print(f"File saved successfully at Path: {path}.")
    except AttributeError as ae:
        raise AttributeError("to_parquet could not be executed as object not DataFrame.") from ae
    except FileExistsError as fe:
        result = yes_no_dialog(
            title='File Exists Error',
            text=f"Existing file in use. Please close to overwrite the file. Error: {fe}.").run()
        if result:
            data.to_parquet(path)
        else:
            print(f"Could not save File at Path: {path}.")
