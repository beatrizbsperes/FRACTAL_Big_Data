# FRACTAL - Land Cover Classification using PySpark

## Project Overview
This project implements and evaluates a machine learning algorithm for **land cover classification** using **point cloud data** stored in **Parquet format**. The focus is on distributed data processing and model training using **PySpark** on **AWS EMR**.

## Objectives
- Perform classification of land cover types from point cloud data.
- Apply a machine learning model using PySpark MLlib.

## Environment file

Create a .env file that contains two variables:
```bash
ACCESS_KEY=your_key
ACCESS_SECRET=your_secret
```
## Notebook
```
|- explorer.ipynb ## contains how to go trhough the handler class function 
|- src/ ## contains the function to handle the bucket and spark dataframe
|- single_operations/ ## contain functions made during the lectures 
|- src/ ## contains the function to handle the bucket and spark dataframe 
|- src/s3handler.py ## added the functionality on read_parquet function on sparker class to read a list of s3 paths
```


## How to Run Starndar_cli_sparker.py

on the env: 
```bash
python Standard_CLI_Sparker.py  --access-key your_key --access-secret your_secret
```

