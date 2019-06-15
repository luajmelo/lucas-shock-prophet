# Shock-Prophets

Cohort 14 Capstone Project for the Certificate of Data Science at Georgetown University School of Continuing Studies.

This project attempts to predict an individual's risk of suffering a negative income shock (i.e., a loss of 20% in inflation-adjusted income) in the next two years, utilizing data from the National Longitudinal Survey of Youth 1979 and 1997 cohorts.

Files in this repository include:

## Ingestion module

ingest_data.py - Ingests and wrangles NLSY data and saves it to a SQLite database.

nlsy.py - Importable module supporting ingestion and wrangling.

Export Data to CSV.ipynb - Saves data from the SQLite database to CSV. Most of the other Python scripts in this repository use the CSV version of the data for speedier loading.

## Data analysis and model selection

Visual Analytics.ipynb - Uses visualizations to support exploratory data analysis.

Cross-Validation.ipynb - Explores different machine learning models and exports a final model.

## Demo

Income Shock Predictor.ipynb - An interactive visualization allowing users to explore income shock risk.