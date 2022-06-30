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
The timestamp column was converted to datetime data type using the code:
``` python
pd.to_datetime(ratings['timestamp'], unit='s')
```

The statistics of the ratings data was checked for errors using a histogram of the rating distributions, and also using the describe() method to print out some stats on central tendency and spread. 

The user id column was renamed to count. The ratings data was also pivoted so that movieId is the index, columns are the rating values, and the rows are the counts for each rating value using the code:
``` python
rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                .rename({'userId':'count'}, axis=1) \
                .pivot(index='movieId',columns='rating', values='count')
```

The This also brought an end to the cleaning of theratings data.


The Wikipedia data and Kaggle metadata were then joined together as movies_df by the IMDb ID and further cleaning done where the data overlap or have redundant columns. The columns were also renamed to be more consistent. 
 The rating_counts was then left merged with the movies_df as movies_with_ratings_df with movies which did not have any rating being replaced with 0.
The movies_df was imported using this code:
``` python
movies_df.to_sql(name='movies', con=engine)
```
Because the ratings data is too large it was imported in chunks using the below code:
``` python
rows_imported = 0
# get the start_time from time.time()
start_time = time.time()
for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
    print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
    data.to_sql(name='ratings', con=engine, if_exists='append')
    rows_imported += len(data)

    # add elapsed time to final print out
    print(f'Done. {time.time() - start_time} total seconds elapsed')
```

In the continuing project, an automated pipeline that takes in new data, performs the appropriate transformations, and loads the data into existing tables was created The code already created was refactored to create one function that takes in the three files—Wikipedia data, Kaggle metadata, and the MovieLens rating data—and performs the ETL process by adding the data to the PostgreSQL database.

An ETL function was written to read in the three data files. extract and transform The Wikipedia data was extracted and transformed so it can be merged with the Kaggle metadata. A try-except block was used to catch errors while extracting the IMDb IDs using a regular expression string and dropping duplicates.

Next the Kaggle metadata and MovieLens rating data was extracted and transformed.The transformed data was converted into separate DataFrames. The Kaggle metadata DataFrame was merged with the Wikipedia movies DataFrame to create the movies_df DataFrame. Finally, the MovieLens rating data DataFrame was merged with the movies_df DataFrame to create the movies_with_ratings_df.

The movies_df DataFrame and ratings CSV was then added to a SQL database.

## Results 
It was confirmed that the movies table has 6,052 rows aas seen in the image below. 
![image1](https://github.com/GerlechJen/Movies-ETL/blob/main/Resources/movies_query.png)


It was also confirmed that the ratings table has 26,024,289 rows as seen in the image below.
![image2](https://github.com/GerlechJen/Movies-ETL/blob/main/Resources/ratings_query.png)

## Summary 
The JSON file and 2 Kaggle CSV files were successfuly extracted, transformed, and loaded to a PostgreSQL database for the hackathon. 
