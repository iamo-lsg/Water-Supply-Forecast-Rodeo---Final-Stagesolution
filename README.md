# Water-Supply-Forecast-Rodeo---Final-Stage and Explainability solution

Username: Atabek Umirbekov, Changxing Dong

## Summary

General summary of the steps for your solution:

The script is designed to handle data preprocessing, model training, prediction generation, and explainability for forecasting seasonal water volume in basins across the western US. It integrates hydrological and climate data, including SNOTEL SWE, SWANN SWE, NRCS flow data, and climate indices like SOI, PNA, and PDO. The predictions are made using Partial Least Squares (PLS) regression models, and the results are complemented with visualizations to explain the key inputs driving each forecast.

Key Features:
Self-explanatory: Each code chunk is accompanied by a clear explanation of its function.

Dynamic Data Handling: The script checks for the availability of the most recent data prior to each forecast issue date, ensuring that it works with the latest available inputs. Training and inference are conducted simultaneously, with up-to-date information, allowing for real-world operational use.

Data Integration: The model integrates multiple data sources (SWE, precipitation, monthly flows, and climate indices) and identifies the most representative inputs that have the strongest association with seasonal flow volume, ensuring relevant predictors are used.

Forecasting and Explainability: The script uses PLS regression models with Leave-One-Out Cross-Validation (LOOCV) to generate predictions for the 10th, 50th, and 90th percentiles.  It conducts variance-based variable importance analysis and  produces visual explanations of the forecasts by illustrating how different inputs (SWE, precipitation, flow) compare with historical data.

Parallel Processing: The script takes advantage of multiple CPU cores to efficiently process and forecast for all basins and issue dates.

Post-processing of the outputs: The script checks the consistency of the predictions and constrains any unrealistic outcomes. Since PLS regression tend to extrapolate linearly, extreme predictions, particularly in very dry years, may result in negative water volumes. To prevent this, the script imposes constraints, ensuring that predictions do not fall below the historical minimum values for each basin. 

Script Structure and Workflow:
This script operates in several key stages:
1.	Package Installation and Setup: It first installs and loads the necessary R packages for handling the data and conducting the forecasts.
2.	Data Preprocessing: Preprocessed inputs, such as SWE and flow data, are loaded and filtered for the relevant forecast issue dates.
3.	Inference and Model Training: The script sets up a parallel processing loop, training a PLS regression model for each basin and forecast issue date using LOOCV.
4.	Prediction and Explainability: It generates forecasts and creates visualizations to explain the role of key inputs, like SWE, precipitation, and antecedent flows.
5.	Postprocessing: The forecast results are checked for consistency, with adjustments made to ensure logical constraints are respected, such as limiting negative predictions using historical minimum thresholds.


# Setup
1. Operating system tested for our code is Ubuntu version 24.04. The following packages should be installed with root access rights (e.g. sudo) through shell script **OSpackages.sh**.
    - build-essential
    - r-base (R version 4.3)
    - git
    - python3
    - python3-dev
    - gdal-bin
    - libgdal-dev
    

2. Install the R-packages with root access rights (e.g. sudo) through R-script file (sudo Rscript **Rpackages.R**).
    - readr
    - dplyr
    - tidyr
    - lubridate
    - data.table
    - terra
    - reshape2
    - scales
    - doParallel
    - caret
    - pls
    - vip
    - ggplot2
    - knitr
    - pdp


3. Connect or download **SNOTEL** and **SWANN** data on **GitHub**
    https://github.com/iamo-lsg/Water-Supply-Forecast-Rodeo-Final-Stage-solution


# Hardware

The solution was run on Dell notebook G3 15 with 32 GB memory, CPU i7-9750H.

Training time: about 50 Min

Inference time: several seconds


# Run the Code 

To run the R script, you should first **cd** to the **solution** directory.

## Cross Validation Submission

SNOTEL and SWANN Data will be used by the code. The data are from approved sources with python code for downloading. As our solution is in R, we seperated the download steps and the main training steps. The data used is in our GitHub repository (https://github.com/iamo-lsg/Water-Supply-Forecast-Rodeo-Final-Stage-solution) 

To get the submissiong of the cross validation run in a terminal 

  Rscript FINAL_24092024_2.R

After this step several intermediate files are save in the directory **explain**, which will be used for the explainability communication.

## Explainability Communication

To get prediction for certain basins and issue dates, run in a terminal

  Rscript EXPLAINABILITY.R

This will generate the file **Rplots.pdf**, showing the results graphically.
