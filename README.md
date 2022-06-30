# Movies ETL
## Overview 

Amazing prime video is a platform for streaming videos and tv shows on Amazing Prime which is the world's largest online retailer. The Amazing Prime video team wants to develop an algorithm that can predict which low budget movies being released will become popular. This will help them buy the streaming rights at a bargain.
Amazing prime has decided to sponsor a hackathon to inspire the team, have fun and connect with the local coding community. A clean dataset of movie data will be provided and the participants will be asked to predict the popular pictures. The dataset for the Hackathon are to be created from scratch from two data sources: a scraped Wikipedia for all movies released since 1990 stored as a JSON, and Kaggle data from MovieLens website that contains ratings data and megadata files stored as CSVs. 
The data will first be extracted from the two sources, transformed into one clean dataset and then, loaded into a SQL database.

In this module, python and pandas was used to perform data wranggling and then PostgreSQL used to store the finished data. The Extract, Transform, and Load (ETL)  process was used to create the data pipelines. Regular expressions (Regex) was also used to parse data and transform text into numbers.

The raw wikipedia movie data was converted to a dataframe using ther code 

``` Python
wiki_movies_df = pd.DataFrame(wiki_movies_raw)
``` 
The dataframe had 193 columns, which is a lot. We modified it by restricting it to only entries that have a director and an IMDb link but no No. of episodes using a list comprehension.

``` python
wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]
```
The above code reduced the number of columns to 75 and number of rows to 7,076.

Further cleaning was performed on the Wikipedia movies dataframe by creating a function called clean_movie. The function combined all the different language columns for alternate titles of movies as one. There were also quite a few columns with slightly different names but the same data, such as "Directed by" and "Director." 
So a new function called clean_column_name was created under the clean_movie function to consolidate such columns into one column.

This reduced the number of columns to 40. There were duplicate rows of IMDb Ids that were also dropped which reduced the number of rows by 3. 

It was also discovered that more than half of the columns had null values. So the columns were trimmed down by using only columns that have less than 90% null values. The code used for that is found below:

``` python
wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
```

With that the number of columns was reduced to 21 useful, data-filled columns.

The release date , box office, budget and running time columns were then converted and parsed using Regular Expressions. With that cleaning of the Wikipedia file was completed.

The Kaggle data was much more structured so required lesser cleaning. The movie metadata had 24 columns with some having the wrong data types so they were converted using the code below:

``` python
kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
```
There was also some corrupted data that had the columns scrambled for 3 movies and they were removed. This brought an end to the cleaning of the Kaggle metadata.

The Kaggle ratings data had 4 columns and 26024289 rows. Using the info() method on the ratings dataframe the results below was obtained. 

``` python
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 26024289 entries, 0 to 26024288
Data columns (total 4 columns):
userId       26024289 non-null int64
movieId      26024289 non-null int64
rating       26024289 non-null float64
timestamp    26024289 non-null int64
dtypes: float64(1), int64(3)
memory usage: 794.2 MB
```



The wikipedia movies JSON file was converted to a data frame and further transformed to reduce teh columns from 193 to 23 columns. The transformed Wikipedia data was then merged with the Kaggle metadata and named movies. The ratings 

The merged data was then sent to PostgreSQL 
Using the code 

The row count for movies was 6052.

The code 

provided the row count of ratings as 26,024,289

## Results 


## Summary 
The JSON file and 2 Kaggle CSV files were successfuly extracted, transformed, and loaded to a PostgreSQL database for the hackathon. 
