# FRACTAL - Land Cover Classification using PySpark

## Project Overview
This project implements and evaluates a machine learning algorithm for **land cover classification** using **point cloud data** stored in **Parquet format**. The focus is on distributed data processing and model training using **PySpark** on **AWS EMR**.

## Objectives
- Perform classification of land cover types from point cloud data.
- Apply a machine learning model using PySpark MLlib.
- Leverage AWS EMR for distributed data processing.

## Tools and Technologies
- **Data Storage:** AWS S3  
- **Data Preprocessing:** PySpark  
- **Machine Learning:** PySpark MLlib  
- **Processing Platform:** AWS EMR  
- **Environment Management:** uv

## Environment Setup
This project uses [uv](https://docs.astral.sh/uv/) to manage dependencies.

1. Create or sync the environment using the provided `pyproject.toml` file:
   ```bash
   uv sync
