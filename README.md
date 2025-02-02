# Earthquake-Data-Pipeline
This project demonstrates the implementation of a medallion architecture-based data pipeline using Microsoft Fabric, PySpark, and Power BI to process and analyze worldwide earthquake data.

## Overview

The pipeline is divided into three layers:

Bronze Layer: Ingests raw earthquake data from the USGS Earthquake API and stores it in a lakehouse.

Silver Layer: Transforms and cleans the data using PySpark, extracting key attributes like latitude, longitude, magnitude, and time.

Gold Layer: Enriches the data by adding country codes using reverse geocoding and classifies earthquake significance into Low, Moderate, and High categories.

Finally, the processed data is visualized using Power BI to analyze earthquake trends.


## Pipeline Architecture

The pipeline follows a medallion architecture, which ensures data quality and reliability as it progresses through the layers:

Bronze Layer: Raw data ingestion.

Silver Layer: Data cleaning and transformation.

Gold Layer: Data enrichment and classification.


## Technologies Used

Programming Languages: Python, PySpark

Cloud Platform: Microsoft Fabric

Data Storage: Lakehouse

Data Visualization: Power BI

Libraries: requests, json, pyspark


## Power BI Dashboard
The Power BI dashboard provides interactive visualizations of earthquake data, including trends, significance classification, and geographic distribution.

### Visualization 1
![fab_1](https://github.com/user-attachments/assets/2e2419cd-3502-428f-b7c5-2cf25167527e)




### Visualization 2
![fab_2](https://github.com/user-attachments/assets/58eea418-e100-4b14-a5a8-1a56dd2bebd8)
