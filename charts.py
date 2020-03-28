import os
import pandas as pd
from pandas.plotting import register_matplotlib_converters
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import date

if not os.path.exists('Charts'):
    os.mkdir('Charts')

# turning off the SettingWithCopyWarning: A value is trying to be set on a copy of a slice from a DataFrame
pd.set_option('mode.chained_assignment', None)

register_matplotlib_converters()

df = pd.read_csv(os.path.join(os.getcwd(), 'Results', 'Strava Data.csv'))
df.sort_values('start_datetime', inplace=True)
date_cols = ['start_datetime', 'start_datetime_utc', 'start_date', 'start_time']
for col in date_cols:
    df[col] = df[col].astype('datetime64[ns]')
df['start_date'] = df['start_date'].dt.date
df['year'] = pd.DatetimeIndex(df['start_datetime']).year
year_list = df['year'].unique().tolist()
df['day_of_week'] = df['start_datetime_utc'].dt.day_name()
df['hour'] = df['start_time'].dt.hour

df_run = df[df['activity_type'] == 'Run']
df_ride = df[df['activity_type'] == 'Ride']
df_ride['total_miles'] = df_ride['distance_miles'].cumsum()

df_run_distance_year = df_run.groupby('year')['distance_miles'].sum()
df_ride_distance_year = df_ride.groupby('year')['distance_miles'].sum()


def shoes():
    df_shoe_miles = df_run.groupby('gear', as_index=False)['distance_miles'].sum()
    df_shoe_runs = df_run.groupby('gear', as_index=False)['activity_id'].count()
    df_shoes = pd.merge(df_shoe_miles, df_shoe_runs, on='gear')
    df_shoes.rename(columns={'activity_id': 'runs'}, inplace=True)
    df_shoes['miles_per_run'] = df_shoes['distance_miles'] / df_shoes['runs']
    df_shoes.to_csv(os.path.join('Results', 'Shoes.csv'), index=False, encoding='utf-8-sig')


def time_of_day(show=True):
    df_run_pivot = pd.pivot_table(df_run, 'distance_miles', 'hour', 'day_of_week', 'sum', fill_value=0)
    df_run_pivot = df_run_pivot.round(2)
    df_run_pivot = df_run_pivot[['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']]
    plt.figure(figsize=(10, 10))
    heatmap = sns.heatmap(df_run_pivot.astype('int'), cmap='OrRd', annot=True, fmt='d', linewidth=0.5)
    heatmap.xaxis.tick_top()
    heatmap.xaxis.set_label_position('top')
    heatmap.tick_params(length=0)
    heatmap.set_xticklabels(heatmap.get_xticklabels(), rotation=45)

    df_run_pivot.to_csv(os.path.join(os.getcwd(), 'Results', 'Time of Day.csv'), encoding='utf-8-sig')
    fig = heatmap.get_figure()
    fig.savefig(os.path.join(os.getcwd(), 'Charts', 'When I run.png'))
    if show:
        plt.show()


def charts_by_year(show=True, *args):
    for y in args:
        df_run_y = df_run[df_run['year'] == y]
        df_run_y['total_miles'] = df_run_y['distance_miles'].cumsum()

        num_runs = df_run_y['start_date'].count()
        total_miles = round(df_run_y['distance_miles'].sum(), 2)
        total_hours = round(df_run_y['moving_time_sec'].sum() / (60 ** 2), 2)
        avg_pace = round(df_run_y['avg_speed_min_mile'].mean(), 2)
        avg_distance = round((df_run_y['distance_miles'].sum()) / (df_run_y['start_date'].count()), 2)
        current_year = date.today().year
        if y == current_year:
            current_days = (date.today() - date(current_year, 1, 1)).days
            mi_per_day = round(total_miles / current_days, 2)
            top_right_axis = round(mi_per_day * 365, 2)
            year_text = f'{num_runs} runs\n{total_miles} miles\n{avg_distance} avg distance' \
                        f'\n{mi_per_day} miles per day\n{top_right_axis} exp total mi\n{total_hours} hours' \
                        f'\n{avg_pace} avg pace'
        else:
            top_right_axis = df_run_y['total_miles'].max()
            year_text = f'{num_runs} runs\n{total_miles} miles\n{avg_distance} avg distance\n{total_hours} hours' \
                        f'\n{avg_pace} avg pace'

        fig, ax1 = plt.subplots(figsize=(10, 6))
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Pace')
        ax1.set_title(f'{y} Running Data')
        c = df_run_y['avg_speed_min_mile']
        plt.scatter(df_run_y['start_date'], df_run_y['avg_speed_min_mile'], c=c, cmap='cool_r')
        ax1.set_ylim(bottom=df_run_y['avg_speed_min_mile'].max() + 0.1, top=df_run_y['avg_speed_min_mile'].min() - 0.1)
        ax1.set_xlim(left=f'{y}-01-01', right=f'{y}-12-31')

        ax2 = ax1.twinx()
        ax2.set_ylabel('Total Miles')
        ax2.plot(df_run_y['start_date'], df_run_y['total_miles'], color='blue')
        ax2.set_ylim(bottom=0, top=top_right_axis)
        # ax2.text(x=f'{y}-11-01', y=1, s=year_text)
        ax2.text(0.85, 0.01, year_text, transform=ax2.transAxes)
        fig.tight_layout()
        chart_file = os.path.join('Charts', f'{y} Running Chart.png')
        if os.path.exists(chart_file):
            os.unlink(chart_file)
        fig.savefig(os.path.join('Charts', f'{y} Running Chart.png'))
        if show:
            plt.show()


if __name__ == '__main__':
    # all years
    # charts_by_year(False, *year_list)
    # this year only
    charts_by_year(True, date.today().year)
    time_of_day(show=False)
    shoes()
