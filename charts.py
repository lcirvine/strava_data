import os
import sys
import pandas as pd
from db_connection import connect
from strava_logging import logger
from pandas.plotting import register_matplotlib_converters
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import date

register_matplotlib_converters()


class RunningCharts:
    def __init__(self):
        self.conn = connect()
        self.df = self.get_running_data()
        self.folder = os.path.join(os.getcwd(), 'Charts')
        self.year_range = self.get_year_range()
        if not os.path.exists(self.folder):
            os.mkdir(self.folder)

    def get_running_data(self):
        df = pd.read_sql('activities', self.conn)
        df.sort_values(by='start_date', inplace=True)
        return df.loc[df['activity_type'] == 'Run']

    def get_year_range(self) -> list:
        return [yr for yr in range(self.df['start_date'].min().year, date.today().year + 1)]

    def time_of_day(self, year: int = None, show: bool = False):
        df = self.df.copy()
        if year is not None:
            df = df.loc[df['year_'] == year]
        df.rename(columns={'day_of_week': 'Day of Week', 'hour_of_day': 'Hour of Day'}, inplace=True)
        df_pivot = pd.pivot_table(df, 'distance_miles', 'Hour of Day', 'Day of Week', 'sum', fill_value=0)
        df_pivot = df_pivot.round(2)
        df_pivot = df_pivot[['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']]
        plt.figure(figsize=(10, 10))
        heatmap = sns.heatmap(df_pivot.astype('int'), cmap='OrRd', annot=True, fmt='d', linewidth=0.5)
        heatmap.xaxis.tick_top()
        heatmap.xaxis.set_label_position('top')
        heatmap.tick_params(length=0)
        heatmap.set_xticklabels(heatmap.get_xticklabels(), rotation=45)
        fig = heatmap.get_figure()
        if year is not None:
            tod_file = os.path.join(self.folder, f"{year} Time of Day Heatmap.png")
        else:
            tod_file = os.path.join(self.folder, "Time of Day Heatmap.png")
        fig.savefig(tod_file)
        if show:
            plt.show()

    def milies_per_month(self, show: bool = False):
        df = self.df.copy()
        df['Month'] = df['start_date_local'].dt.month
        df.rename(columns={'year_': 'Year'}, inplace=True)
        df_pivot = df.pivot_table(values='distance_miles', index='Year', columns='Month', aggfunc='sum', fill_value=0)
        df_pivot.rename(
            columns={1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'June', 7: 'July', 8: 'Aug',
                     9: 'Sept', 10: 'Oct', 11: 'Nov', 12: 'Dec'}, inplace=True)
        plt.figure(figsize=(10, 10))
        heatmap = sns.heatmap(df_pivot.astype('int'), cmap='YlGnBu', annot=True, fmt='d', linewidth=0.5)
        heatmap.xaxis.tick_top()
        heatmap.xaxis.set_label_position('top')
        heatmap.tick_params(length=0)
        fig = heatmap.get_figure()
        fig.savefig(os.path.join(self.folder, 'Miles per Month.png'))
        if show:
            plt.show()
        else:
            plt.close()

    def distance_and_pace(self, year: int = None, show: bool = False):
        df = self.df.copy()
        if year is not None:
            df = df.loc[df['year_'] == year]
        df['total_miles'] = df['distance_miles'].cumsum()
        num_runs = df['activity_id'].count()
        total_miles = round(df['distance_miles'].sum(), 2)
        total_hours = round((df['moving_time'].sum() / (60 ** 2)), 2)
        avg_pace = round(df['min_per_mile'].mean(), 2)
        avg_distance = round((df['distance_miles'].sum()) / (df['activity_id'].count()), 2)
        current_year = date.today().year
        if year == current_year:
            current_days = (date.today() - date(current_year, 1, 1)).days
            mi_per_day = round(total_miles / current_days, 2)
            top_right_axis = round(mi_per_day * 365, 2)
            year_text = f'{num_runs} runs\n{total_miles} miles\n{avg_distance} avg distance' \
                        f'\n{mi_per_day} miles per day\n{top_right_axis} exp total mi\n{total_hours} hours' \
                        f'\n{avg_pace} avg pace'
        else:
            top_right_axis = df['total_miles'].max()
            year_text = f'{num_runs} runs\n{total_miles} miles\n{avg_distance} avg distance\n{total_hours} hours' \
                        f'\n{avg_pace} avg pace'

        fig, ax1 = plt.subplots(figsize=(10, 6))
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Pace')
        if year is not None:
            ax1.set_title(f'{year} Running Data')
        else:
            ax1.set_title(f'All Running Data')
        c = df['min_per_mile']
        plt.scatter(df['start_date'], df['min_per_mile'], c=c, cmap='cool_r')
        ax1.set_ylim(bottom=df['min_per_mile'].max() + 0.1, top=df['min_per_mile'].min() - 0.1)
        if year is not None:
            ax1.set_xlim(left=f'{year}-01-01', right=f'{year}-12-31')
        else:
            ax1.set_xlim(left=df['start_date_local'].min().date().strftime('%Y-%m-%d'),
                         right=df['start_date_local'].max().date().strftime('%Y-%m-%d'))
        ax2 = ax1.twinx()
        ax2.set_ylabel('Total Miles')
        ax2.plot(df['start_date'], df['total_miles'], color='blue')
        ax2.set_ylim(bottom=0, top=top_right_axis)
        # ax2.text(x=f'{y}-11-01', y=1, s=year_text)
        ax2.text(0.85, 0.01, year_text, transform=ax2.transAxes)
        fig.tight_layout()
        if year is not None:
            chart_file = os.path.join(self.folder, f'{year} Running Chart.png')
        else:
            chart_file = os.path.join(self.folder, f'Running Chart.png')
        fig.savefig(chart_file)
        if show:
            plt.show()
        else:
            plt.close()

    def close(self):
        self.conn.close()


def main():
    rc = RunningCharts()
    try:
        # by year
        for yr in rc.year_range:
            rc.time_of_day(yr)
            rc.distance_and_pace(yr)
        # all time
        rc.time_of_day()
        rc.distance_and_pace()
        rc.milies_per_month()
    except Exception as e:
        logger.error(e, exc_info=sys.exc_info())
    finally:
        rc.close()


if __name__ == '__main__':
    main()
