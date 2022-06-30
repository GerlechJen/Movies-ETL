# Movies ETL
## Overview 

Amazing prime video is a platform for streaming videos and tv shows on Amazing Prime which is the world's largest online retailer. The Amazing Prime video team wants to develop an algorithm that can predict which low budget movies being released will become popular. This will help them buy the streaming rights at a bargain.
Amazing prime has decided to sponsor a hackathon to inspire the team, have fun and connect with the local coding community. A clean dataset of movie data will be provided and the participants will be asked to predict the popular pictures. The dataset for the Hackathon are to be created from scratch from two data sources: a scraped Wikipedia for all movies released since 1990 stored as a JSON, and Kaggle data from MovieLens website that contains ratings data and megadata files stored as CSVs. 
The data will first be extracted from the two sources, transformed into one clean dataset and then, loaded into a SQL database.

In this module, python and pandas was used to perform data wranggling and then PostgreSQL used to store the finished data. The Extract, Transform, and Load (ETL)  process was used to create the data pipelines. Regular expressions (Regex) was also used to parse data and transform text into numbers.

The raw wikipedia data had 193 columns, the Kaggle metadata had 24 columns an the Kaggle ratings data had 4 columns.

The wikipedia movies JSON file was converted to a data frame and further transformed to reduce teh columns from 193 to 23 columns. The transformed Wikipedia data was then merged with the Kaggle metadata and named movies. The ratings 

The merged data was then sent to PostgreSQL 
Using the code 

The row count for movies was 6052.

The code 

provided the row count of ratings as 26,024,289

## Results 


## Summary 
The JSON file and 2 Kaggle CSV files were successfuly extracted, transformed, and loaded to a PostgreSQL database for the hackathon. 
