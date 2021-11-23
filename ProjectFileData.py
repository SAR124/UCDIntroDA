import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt
# import os
# import json
from zipfile import ZipFile
import warnings

from clyent import color
from kaggle.api.kaggle_api_extended import KaggleApi

warnings.filterwarnings("ignore")

# Get Weather Data

api = KaggleApi()
api.authenticate()

api.dataset_download_files('JosephW20/uk-met-office-weather-data')

file_name = "uk-met-office-weather-data.zip"

with ZipFile(file_name, 'r') as zipped:
    # printing all the contents of the zip file
    zipped.printdir()

    # extracting all the files
    print('Extracting all the files now...')
    zipped.extractall()
    print('Done!')
    zipped.close()

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

# Fill in missing data with the mean averages

weather_cols = ['tmax', 'tmin', 'af', 'rain', 'sun']

weather_raw[weather_cols] = weather_raw[weather_cols].fillna(weather_raw[weather_cols].mean())

# Examine data for missing values

# print(weather_raw.isnull().sum())
# print(weather_raw.info())
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

# summarise youtube data, view counts on any trending date
youtube = youtube_raw.groupby(['trending_date']).agg(total_views=('views', sum))

youtube = youtube.reset_index()

print(youtube_raw.info())
# print(youtube_raw['trending_date'])

# import netflix watch data - uk

netflix_raw = pd.read_csv('vodclickstream_uk_movies_03.csv')

# examine netflix data

#print(netflix_raw.columns)
#print(netflix_raw.isnull().sum())
#print(netflix_raw.head())
#print(netflix_raw.info())

# convert datetime to date and keep data type date.  Add day name.

netflix_raw['datetime'] = pd.to_datetime(netflix_raw['datetime'])
netflix_raw['date'] = netflix_raw['datetime'].dt.date.astype('datetime64[ns]')
netflix_raw['day_name'] = netflix_raw['date'].dt.day_name()

print(netflix_raw.info())

# aggregate netflix data -- All data and only watched data
# 'Duration' shows how long it was (in sec)

netflix_data = netflix_raw.groupby(['date', 'genres', 'day_name']).agg(total_watchtime=('duration', sum),
                                                                       average_watchtime=('duration', 'mean'),
                                                                       unique_titles=('title', 'nunique'),
                                                                       unique_users=('user_id', 'nunique'))
netflix_data_watched = netflix_raw.where(netflix_raw.duration != 0).groupby(
    ['date', 'genres', 'day_name']).agg(
    total_watchtime_watched=('duration', sum),
    average_watchtime_watched=('duration', 'mean'),
    unique_titles_watched=('title', 'nunique'),
    unique_users_watched=('user_id', 'nunique'))

netflix_data_by_date = netflix_raw.groupby(['date', 'day_name']).agg(total_watchtime=('duration', sum),
                                                                     average_watchtime=('duration', 'mean'),
                                                                     unique_titles=('title', 'nunique'),
                                                                     unique_users=('user_id', 'nunique'))
netflix_data_watched_by_date = netflix_raw.where(netflix_raw.duration != 0).groupby(['date', 'day_name']).agg(
    total_watchtime_watched=('duration', sum),
    average_watchtime_watched=('duration', 'mean'),
    unique_titles_watched=('title', 'nunique'),
    unique_users_watched=('user_id', 'nunique'))

netflix_data_watched_by_date = netflix_data_watched_by_date.reset_index()
netflix_data_watched = netflix_data_watched.reset_index()
netflix_data_watched = netflix_data_watched.set_index('date')

#print(netflix_data.info())
#print(netflix_data.head())
#print(netflix_data_watched.info())
#print(netflix_data_watched.head())


# get min and max date per data set
# Ensure datasets overlap at least certain periods

def min_max_data(df, col):
    print(df[col].min(), df[col].max())


min_max_data(netflix_raw, 'datetime')
min_max_data(youtube_raw, 'trending_date')
min_max_data(weather_raw, 'year')

weatherdata = weather_raw

# create a list of dates to add to weather for easier plotting later
start = dt.datetime(1960, 1, 1)
end = dt.datetime(2021, 12, 31)

datelist = pd.DataFrame(pd.date_range(start, end), columns=['date_list'])
datelist['year'] = pd.DatetimeIndex(datelist['date_list']).year
datelist['month'] = pd.DatetimeIndex(datelist['date_list']).month

# Check Data
#print(type(datelist))
#print(datelist)
#print(weatherdata.head())
#print(weatherdata.info())
#print(weatherdata.isnull().sum())

# As locations are not used elsewhere, group the data by year and month - Average over all regions/locations
weatherdata_uk_avg = weatherdata.groupby(['year', 'month']).agg(average_tmax=('tmax', 'mean'),
                                                                average_tmin=('tmin', 'mean'),
                                                                average_af=('af', 'mean'),
                                                                average_rain=('rain', 'mean'),
                                                                average_sun=('sun', 'mean'))

# Identify months which we might consider bad weather - rain and frost mornings
weatherdata_uk_avg['bad_weather'] = np.where(
    (weatherdata_uk_avg['average_rain'] > 50) & (weatherdata_uk_avg['average_af'] > 5), True, False)

# Merge weather data with datelist to create the weather data records per day
final_weather_data = pd.merge(left=datelist, right=weatherdata_uk_avg, how='inner', left_on=['year', 'month'],
                              right_on=['year', 'month'])

# Create a Figure and an Axes with plt.subplots
fig, ax = plt.subplots()

# Plot average_tmax from final_weather_data against date_list
ln1 = ax.plot(final_weather_data["date_list"], final_weather_data['average_tmax'], color='r',
              label='Average Maximum Temperature')

# Plot average_tmin from final_weather_data against date_list
ln2 = ax.plot(final_weather_data["date_list"], final_weather_data['average_tmin'], color='b',
              label='Average Minimum Temperature')

# Apply some formatting
ax.grid(True)
ax.set_xlabel('Date')
ax.set_ylabel('Temperature (celsius)')
ax.set_title('Average Monthly Maximum and Minimum Temperature')

# Rotate x Ticks for readable on plot
for tick in ax.get_xticklabels():
    tick.set_rotation(90)

# create legends - this process is used to allow combining one legend for multiple y axis

lns = ln1 + ln2
labs = [l.get_label() for l in lns]
ax.legend(lns, labs, loc='upper right', shadow=True, fancybox=True)

# Call the show function
plt.show()

# clear and create a new plot
fig.clear()

# reduce the weather data

end = dt.datetime(2019, 7, 1)

final_weather_data = (final_weather_data[(final_weather_data['date_list'] < end)])

final_weather_data = (final_weather_data[(final_weather_data['year'] >= 2018)])

# match the netflix data for easier reading in plot(s)
netflix_data_watched_by_date = netflix_data_watched_by_date[netflix_data_watched_by_date['date'] >= '2018-01-01']

# print(final_weather_data.info())
# print(final_weather_data.head())

# Create a Figure and an Axes with plt.subplots
fig, ax = plt.subplots()

# Plot average_tmax from final_weather_data against date_list
ln1 = ax.plot(final_weather_data["date_list"], final_weather_data['average_tmax'], color='r',
              label='Average Maximum Temperature')

# Plot average_tmin from final_weather_data against date_list
ln2 = ax.plot(final_weather_data["date_list"], final_weather_data['average_tmin'], color='b',
              label='Average Minimum Temperature')

# plot netflix watch data

ax2 = ax.twinx()

netflix_data_watched_by_date['total_watchtime_watched'] = netflix_data_watched_by_date[
                                                              'total_watchtime_watched'] / 60 / 60 / 24
netflix_data_watched['total_watchtime_watched'] = netflix_data_watched['total_watchtime_watched'] / 60 / 60 / 24

ln3 = ax2.plot(netflix_data_watched_by_date['date'],
               netflix_data_watched_by_date['total_watchtime_watched'], color='g', label='Total Watchtime')
# Apply some formatting
ax.grid(True)
ax.set_xlabel('Date')
ax.set_ylabel('Temperature (celsius)')
ax2.set_ylabel('Total Watchtime (days)')
ax.set_title('Average Monthly Maximum and Minimum Temperature against total watchtime')
# Rotate x-tick labels
for tick in ax.get_xticklabels():
    tick.set_rotation(45)

# create legend
lns = ln1 + ln2 + ln3
labs = [l.get_label() for l in lns]
ax.legend(lns, labs, loc='upper right', shadow=True, fancybox=True)

# Call the show function
plt.show()

# clear and create a new plot
fig.clear()

# Create a Figure and an Axes with plt.subplots
fig, ax = plt.subplots()

# Plot average_tmax from final_weather_data against date_list
ln1 = ax.plot(final_weather_data["date_list"], final_weather_data['average_tmax'], color='r',
              label='Average Maximum Temperature')

# Plot average_tmin from final_weather_data against date_list
ln2 = ax.plot(final_weather_data["date_list"], final_weather_data['average_tmin'], color='b',
              label='Average Minimum Temperature')

# plot youtube watch data
ax2 = ax.twinx()
ln3 = ax2.plot(youtube['trending_date'],
               youtube['total_views'], color='g', label='Total Views')
# create legends
lns = ln1 + ln2 +ln3
labs = [l.get_label() for l in lns]
ax.legend(lns, labs, loc='upper right', shadow=True, fancybox=True)

# Apply some formatting
ax.grid(True)
ax.set_xlabel('Date')
ax.set_ylabel('Average Temperature')
ax2.set_ylabel('Total views of trending')
ax.set_title('Average Temperature against total views')

plt.show()

fig.clear()

fig, ax = plt.subplots()

# change style
sns.set_style("darkgrid")
# bar plot as looking more at categories for this data to look for other patterns
watched_days = sns.catplot(data=netflix_data_watched_by_date, x='day_name', kind='bar', y='total_watchtime_watched', ci=None)
watched_days.set_xlabels('Day of week')
watched_days.set_ylabels('Total watchtime - Days')

plt.show()

fig.clear()

# Plot average_rain from final_weather_data against date_list and Average Watchtime
fig, ax = plt.subplots()
ax2 = ax.twinx()
ln4 = ax.plot(final_weather_data["date_list"], final_weather_data["average_rain"], color='r', label='Average Rainfall (mm)')
ln5 = ax2.plot(netflix_data_watched_by_date['date'],
               netflix_data_watched_by_date['average_watchtime_watched'], color='g', label='Average Watchtime')

lns = ln4 + ln5
labs = [l.get_label() for l in lns]
ax.legend(lns, labs, loc='upper right', shadow=True, fancybox=True)

ax.grid(True)
ax.set_xlabel('Date')
ax.set_ylabel('Average Rainfall (mm)')
ax2.set_ylabel('Average Watchtime in Seconds')
ax.set_title('Average Rainfall (mm) against total watchtime (s)')

plt.show()


