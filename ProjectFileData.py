import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as datetime
import os
import json
from zipfile import ZipFile
import warnings
from kaggle.api.kaggle_api_extended import KaggleApi

warnings.filterwarnings("ignore")

# Get Weather Data

api = KaggleApi()
api.authenticate()

api.dataset_download_files('JosephW20/uk-met-office-weather-data')

file_name = "uk-met-office-weather-data.zip"

with ZipFile(file_name, 'r') as zipped:
    zipped.extractall()

weather_raw = pd.read_csv('MET Office Weather Data.csv', na_values=['NA'])

# Examine data

# print(weather_raw.columns)

# year: Year in which the measurements were taken
# month: Month in which the measurements were taken
# tmax: Mean daily maximum temperature (°C)
# tmin: Mean daily minimum temperature (°C)
# af: Days of air frost recorded that month (days)
# rain: Total rainfall (mm)
# sun: Total sunshine duration (hours)
# station: Station location where measurement was recorded

# Examine data for missing values

# print(weather_raw.isnull().sum())

# Fill in missing data with the mean averages from that year

weather_cols = ['tmax', 'tmin', 'af', 'rain', 'sun']

# weather_raw[weather_cols] = weather_raw[weather_cols].apply(pd.to_numeric, errors='coerce')

weather_raw[weather_cols] = weather_raw[weather_cols].fillna(weather_raw[weather_cols].mean())

# Examine data for missing values

# print(weather_raw.isnull().sum())
print(weather_raw.info())
# print(weather_raw[weather_cols].head())

# import uk YouTube Data - Trending
# import uk categories

ytvcategory = pd.read_excel('GB_category_id.xlsx')

# import uk youtube trending data
ytv = pd.read_csv('GBvideos.csv')

# examine youtube data

# print(ytvcategory.columns)

# print(ytv.columns)

# print(ytv.isnull().sum())

# join youtube data to identify category of trending video

youtube_raw = pd.merge(left=ytv, right=ytvcategory, how='inner', left_on='category_id', right_on='items__id')

# print(youtube_raw[
#         ['video_id', 'items__snippet__title', 'title', 'channel_title', 'publish_time', 'trending_date', 'views',
#           'likes', 'dislikes', 'comment_count', 'comments_disabled', 'ratings_disabled',
#           'video_error_or_removed']].head())

# fix dates

# print(type(youtube_raw['trending_date']))
# print(youtube_raw['trending_date'])

youtube_raw['trending_date'] = pd.to_datetime(youtube_raw['trending_date'], format='%y.%d.%m')

# print(youtube_raw['trending_date'])

# import netflix watch data - uk

netflix_raw = pd.read_csv('vodclickstream_uk_movies_03.csv')

# examine netflix data

print(netflix_raw.columns)
print(netflix_raw.isnull().sum())
print(netflix_raw.head())
print(netflix_raw.info())

netflix_raw['datetime'] = pd.to_datetime(netflix_raw['datetime'])
netflix_raw['date'] = netflix_raw['datetime'].dt.date

print(netflix_raw.info())

# aggregate netflix data
# 'Duration' shows how long it was (in sec)

netflix_data = netflix_raw.groupby(netflix_raw['date']).agg(total_watchtime=('duration', sum),
                                                            average_watchtime=('duration', 'mean'),
                                                            unique_titles=('title', 'nunique'),
                                                            unique_users=('user_id', 'nunique'))
netflix_data_watched = netflix_raw.where(netflix_raw.duration != 0).groupby(netflix_raw['datetime'].dt.date).agg(
    total_watchtime_watched=('duration', sum),
    average_watchtime_watched=('duration', 'mean'),
    unique_titles_watched=('title', 'nunique'),
    unique_users_watched=('user_id', 'nunique'))

print(netflix_data.info())
print(netflix_data.head())
print(netflix_data_watched.info())
print(netflix_data_watched.head())


# get min and max date per data set

def min_max_data(df, col):
    print(df[col].min(), df[col].max())


min_max_data(netflix_raw, 'datetime')
min_max_data(youtube_raw, 'trending_date')
min_max_data(weather_raw, 'year')

weatherdata = (weather_raw[(weather_raw['year'] >= 2016)])
start = datetime.datetime(1960, 1, 1)

end = datetime.datetime(2021, 12, 31)

datelist = pd.DataFrame(pd.date_range(start, end), columns=['date_list'])
datelist['year'] = pd.DatetimeIndex(datelist['date_list']).year
datelist['month'] = pd.DatetimeIndex(datelist['date_list']).month

print(type(datelist))

print(datelist)

print(weatherdata.head())
print(weatherdata.info())
# print(weatherdata.isnull().sum())

weatherdata_uk_avg = weatherdata.groupby(['year', 'month']).agg(average_tmax=('tmax', 'mean'),
                                                                average_tmin=('tmin', 'mean'),
                                                                average_af=('af', 'mean'),
                                                                average_rain=('rain', 'mean'),
                                                                average_sun=('sun', 'mean'))

weatherdata_uk_avg['bad_weather'] = np.where(
    (weatherdata_uk_avg['average_rain'] > 50) & (weatherdata_uk_avg['average_af'] > 5), True, False)

final_weather_data = pd.merge(left=datelist, right=weatherdata_uk_avg, how='inner', left_on=['year', 'month'],
                              right_on=['year', 'month'])

#print(final_weather_data.info())
#print(final_weather_data.head())

sns.relplot(x="date_list", y="average_tmax",
                data=final_weather_data, kind="line")

plt.show()

