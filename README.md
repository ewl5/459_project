# Analyzing Rent Data in Alberta

## Datasets
Our datasets are found under the /datasets folder. In the preprocessing phase, 
the datasets required are "rentfaster.csv" and all the files under 
"osm_helper_data_Alberta", we join these tables into a "dataset_joined.csv". 
Afterward, we perform data cleaning on this file 
and create a "dataset_cleaned.csv" which we will use for the remainder of the pipeline.

## Set up environment
```
pip install -r requirements.txt
```
to install our notebook dependencies

Our main dependencies are matplotlib (3.10.7), scikit-learn (1.7.2), pandas(2.3.3), all on the latest versions

## EDA and Preprocessing
This stage of our pipeline can be found in the jupyter notebook file "EDA_Preprocessing.ipynb".
To run this notebook, run the following command
```
jupyter notebook EDA_Preprocessing.ipynb
```
## Primary Notebook
The next stage of our pipeline consisting of Classification, Clustering, Outlier Detection, can be found in the primary jupyter notebook file under "final.ipynb", which combines both the EDA and Preprocessing stage with the Classification, Clustering, Outlier Detection stage in one notebook.
To run this notebook, run the following command
```
jupyter notebook final.ipynb
```
