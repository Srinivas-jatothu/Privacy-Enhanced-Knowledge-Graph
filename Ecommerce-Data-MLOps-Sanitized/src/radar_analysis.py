"""
Module for Analyzing Customer Clusters
using Radar Chart
"""
import os
import pandas as pd
import plotly.express as px
from plotly.io import write_image
#import plotly.graph_objects as go
#from dash import Dash, dcc, html
from sklearn.preprocessing import StandardScaler

# Determine the absolute path of the param_035 directory
PAR_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#Global variables
__INGESTPATH__ = os.path.join(PAR_DIR,"data","processed","param_060.parquet")
__SAVEPATH__= os.path.join(PAR_DIR,"data","plots")
colors = ['#e8000b', '#1ac938', '#023eff']

def visualization_func_001(param_006=__INGESTPATH__):
    """
    Docstring
    """
    #Placeholder for data
    data = None

    #File Loading
    try:
        data = pd.read_parquet(param_006)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found at {param_006}.") from None

    # Scaling uniformly
    ml_model_func_003 = StandardScaler()
    df_customer_standardized = ml_model_func_003.fit_transform(data.drop(columns=['cluster'], axis=1))

    # Create a new dataframe with standardized values and add the cluster column back
    df_customer_standardized = pd.DataFrame(df_customer_standardized, \
        columns=data.columns[:-1], index=data.index)
    df_customer_standardized['cluster'] = data['cluster']

    cluster_centroids = df_customer_standardized.groupby('cluster').mean()
    print(cluster_centroids)

    #Plotting Figures
    clusters=cluster_centroids.index

    if not os.path.exists(__SAVEPATH__):
        os.makedirs(__SAVEPATH__)

    for i in clusters:
        param_009 = px.line_polar(cluster_centroids,r=cluster_centroids.iloc[i].tolist()\
            ,theta=cluster_centroids.columns, line_close=True)
        param_009.update_traces(fill='toself',line_color=colors[i])
        param_009.show()
        p=os.path.join(__SAVEPATH__,f"Cluster{i}.jpeg")
        write_image(param_009,file=p,format='jpeg')

#visualization_func_001()
