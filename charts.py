import os
import pandas as pd
from pandas.plotting import register_matplotlib_converters
import matplotlib.pyplot as plt
# from matplotlib.collections import LineCollection
# from matplotlib.colors import ListedColormap, BoundaryNorm
# import matplotlib.dates as mdates
# from datetime import timedelta
# import numpy as np

if not os.path.exists('Charts'):
    os.mkdir('Charts')

# turning off the SettingWithCopyWarning: A value is trying to be set on a copy of a slice from a DataFrame
pd.set_option('mode.chained_assignment', None)

register_matplotlib_converters()

df = pd.read_csv(os.path.join(os.getcwd(), 'Results', 'Strava Data.csv'))
df.sort_values('start_datetime', inplace=True)
date_cols = ['start_datetime', 'start_datetime_utc', 'start_date']
for c in date_cols:
    df[c] = df[c].astype('datetime64[ns]')
df['start_date'] = df['start_date'].dt.date
df['year'] = pd.DatetimeIndex(df['start_datetime']).year

df_run = df[df['activity_type'] == 'Run']
year_list = df_run['year'].unique().tolist()

for y in year_list:
    df_run_y = df_run[df_run['year'] == y]
    df_run_y['total_miles'] = df_run_y['distance_miles'].cumsum()

    num_runs = df_run_y['start_date'].count()
    total_miles = round(df_run_y['distance_miles'].sum(), 2)
    total_hours = round(df_run_y['moving_time_sec'].sum() / (60 ** 2), 2)
    avg_pace = round(df_run_y['avg_speed_min_mile'].mean(), 2)
    avg_distance = round((df_run_y['distance_miles'].sum()) / (df_run_y['start_date'].count()), 2)
    year_text = f'{num_runs} runs\n{total_miles} miles\n{avg_distance} avg distance\n{total_hours} hours' \
        f'\n{avg_pace} avg pace'

    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Pace')
    ax1.set_title(f'{y} Running Data')
    c = df_run_y['avg_speed_min_mile']
    plt.scatter(df_run_y['start_date'], df_run_y['avg_speed_min_mile'], c=c, cmap='cool_r')
    ax1.set_ylim(ymin=df_run_y['avg_speed_min_mile'].max() + 0.1, ymax=df_run_y['avg_speed_min_mile'].min() - 0.1)
    ax1.set_xlim(xmin=f'{y}-01-01', xmax=f'{y}-12-31')

    ax2 = ax1.twinx()
    ax2.set_ylabel('Total Miles')
    ax2.plot(df_run_y['start_date'], df_run_y['total_miles'], color='blue')
    ax2.set_ylim(ymin=0, ymax=df_run_y['total_miles'].max())
    # ax2.text(x=f'{y}-11-01', y=1, s=year_text)
    ax2.text(0.85, 0.01, year_text, transform=ax2.transAxes)
    fig.tight_layout()
    chart_file = os.path.join('Charts', f'{y} Running Chart.png')
    if os.path.exists(chart_file):
        os.unlink(chart_file)
    fig.savefig(os.path.join('Charts', f'{y} Running Chart.png'))
    # plt.show()

df_ride = df[df['activity_type'] == 'Ride']
df_ride['total_miles'] = df_ride['distance_miles'].cumsum()

df_run_distance_year = df_run.groupby('year')['distance_miles'].sum()
df_ride_distance_year = df_ride.groupby('year')['distance_miles'].sum()
