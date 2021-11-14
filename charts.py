import os
import sys
import pandas as pd
from db_connection import connect
from strava_logging import logger
from pandas.plotting import register_matplotlib_converters
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, date
import imageio

register_matplotlib_converters()


class RunningCharts:
    def __init__(self, athlete_id: int = None, activity_type: str = 'Run', units: str = 'miles'):
        self.athlete_id = athlete_id
        self.activity_type = activity_type
        if units == 'miles':
            self.distance_unit = 'distance_miles'
            self.distance_name = 'Miles'
        elif units == 'km' or units == 'kilometers':
            self.distance_unit = 'distance_km'
            self.distance_name = 'KM'
        self.conn = connect()
        self.df = self.get_running_data()
        if self.athlete_id is not None:
            self.folder = os.path.join(os.getcwd(), 'Charts', f"{self.athlete_id}", f"{self.distance_name}")
        else:
            self.folder = os.path.join(os.getcwd(), 'Charts', f"{self.distance_name}")
        if not os.path.exists(self.folder):
            os.mkdir(self.folder)
        self.year_range = self.get_year_range()

    def get_running_data(self):
        df = pd.read_sql('activities', self.conn)
        df.sort_values(by='start_date', inplace=True)
        df = df.loc[df['activity_type'] == self.activity_type]
        if self.athlete_id is not None:
            df = df.loc[df['athlete_id'] == self.athlete_id]
        return df

    def get_year_range(self) -> list:
        return [yr for yr in range(self.df['start_date'].min().year, date.today().year + 1)]

    def time_of_day(self, year: int = None, show: bool = False):
        df = self.df.copy()
        if year is not None:
            df = df.loc[df['year_'] == year]
        df.rename(columns={'day_of_week': 'Day of Week', 'hour_of_day': 'Hour of Day'}, inplace=True)
        df_pivot = pd.pivot_table(df, self.distance_unit, 'Hour of Day', 'Day of Week', 'sum', fill_value=0)
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

    def dist_per_month(self, show: bool = False):
        df = self.df.copy()
        df['Month'] = df['start_date_local'].dt.month
        df.rename(columns={'year_': 'Year'}, inplace=True)
        df_pivot = df.pivot_table(values=self.distance_unit, index='Year', columns='Month', aggfunc='sum', fill_value=0)
        df_pivot.rename(
            columns={1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'June', 7: 'July', 8: 'Aug',
                     9: 'Sept', 10: 'Oct', 11: 'Nov', 12: 'Dec'}, inplace=True)
        plt.figure(figsize=(10, 10))
        heatmap = sns.heatmap(df_pivot.astype('int'), cmap='YlGnBu', annot=True, fmt='d', linewidth=0.5)
        heatmap.xaxis.tick_top()
        heatmap.xaxis.set_label_position('top')
        heatmap.tick_params(length=0)
        fig = heatmap.get_figure()
        fig.savefig(os.path.join(self.folder, f"{self.distance_name} per Month.png"))
        if show:
            plt.show()
        else:
            plt.close()

    def distance_and_pace(self, year: int = None, show: bool = False):
        df = self.df.copy()
        if year is not None:
            df = df.loc[df['year_'] == year]
        df['mins_per_run'] = round(df['moving_time'] / 60, 2)
        df['avg_pace'] = df['mins_per_run'] / df[self.distance_unit]
        df['total_dist'] = df[self.distance_unit].cumsum()
        num_runs = df['activity_id'].count()
        total_dist = round(df[self.distance_unit].sum(), 2)
        total_hours = round((df['moving_time'].sum() / (60 ** 2)), 2)
        avg_pace = round(df['avg_pace'].mean(), 2)
        avg_distance = round((df[self.distance_unit].sum()) / (df['activity_id'].count()), 2)
        current_year = date.today().year
        if year == current_year:
            current_days = (date.today() - date(current_year, 1, 1)).days
            dist_per_day = round(total_dist / current_days, 2)
            top_right_axis = round(dist_per_day * 365, 2)
            year_text = f"{num_runs} runs" \
                        f"\n{total_dist} {self.distance_name.lower()}" \
                        f"\n{avg_distance} avg distance" \
                        f"\n{dist_per_day} {self.distance_name.lower()} per day" \
                        f"\n{top_right_axis} est {self.distance_name.lower()}" \
                        f"\n{total_hours} hours\n{avg_pace} avg pace"
        else:
            top_right_axis = df['total_dist'].max()
            year_text = f"{num_runs} runs" \
                        f"\n{total_dist} {self.distance_name.lower()}" \
                        f"\n{avg_distance} avg distance" \
                        f"\n{total_hours} hours" \
                        f"\n{avg_pace} avg pace"

        fig, ax1 = plt.subplots(figsize=(10, 6))
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Pace')
        if year is not None:
            ax1.set_title(f'{year} Running Data')
        else:
            ax1.set_title(f'All Running Data')
        c = df['avg_pace']
        plt.scatter(df['start_date'], df['avg_pace'], c=c, cmap='cool_r')
        ax1.set_ylim(bottom=df['avg_pace'].max() + 0.1, top=df['avg_pace'].min() - 0.1)
        if year is not None:
            ax1.set_xlim(left=f'{year}-01-01', right=f'{year}-12-31')
            ax2 = ax1.twinx()
            ax2.set_ylabel(f"Total {self.distance_name.lower()}")
            ax2.plot(df['start_date'], df['total_dist'], color='blue')
            ax2.set_ylim(bottom=0, top=top_right_axis)
            ax2.text(0.85, 0.01, year_text, transform=ax2.transAxes)
        else:
            ax1.set_xlim(left=df['start_date_local'].min().date().strftime('%Y-%m-%d'),
                         right=df['start_date_local'].max().date().strftime('%Y-%m-%d'))

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

    def distance_and_pace_gif(self, year: int = None):
        temp_dir = os.path.join(os.getcwd(), 'Charts', self.distance_name, 'gif', 'temp_files')
        df = self.df.copy()
        if year is not None:
            df = df.loc[df['year_'] == year]
            chart_title = f'{year} Running Data'
            start_date = f"{year}-01-01"
            if year == date.today().year:
                end_date = date.today().strftime('%Y-%m-%d')
            else:
                end_date = f"{year}-12-31"
            s_days = pd.date_range(start=start_date, end=end_date, freq='D').to_series(name='Date')
            df = pd.merge(s_days, df, how='outer', left_on='Date', right_on='activity_date')
        else:
            chart_title = f'All Running Data'
            start_date = df['activity_date'].min().strftime('%Y-%m-%d')
            end_date = df['activity_date'].max().strftime('%Y-%m-%d')
            df['Date'] = df['activity_date']
        df = df.sort_values('Date').reset_index(drop=True)
        df['mins_per_run'] = round(df['moving_time'] / 60, 2)
        df['avg_pace'] = df['mins_per_run'] / df[self.distance_unit]
        df = df[['Date', 'avg_pace', 'activity_id', 'distance_miles', 'distance_km']]
        y_lim_bottom = df['avg_pace'].max() + 0.1
        y_lim_top = df['avg_pace'].min() - 0.1

        filenames = []
        for i in range(len(df)):
            fig, ax = plt.subplots(figsize=(8, 6))
            plt.scatter(df.loc[:i, 'Date'], df.loc[:i, 'avg_pace'], c='blue')
            fig.autofmt_xdate()
            ax.set_title(chart_title)
            ax.set_xlabel('Date')
            ax.set_ylabel('Pace')
            ax.set_ylim(bottom=y_lim_bottom, top=y_lim_top)
            ax.set_xbound(lower=datetime.strptime(start_date, '%Y-%m-%d').date(),
                          upper=datetime.strptime(end_date, '%Y-%m-%d').date())
            chart_text = f"Runs {df.loc[:i, 'activity_id'].count()}\n" \
                         f"Distance {round(df.loc[:i, self.distance_unit].sum(), 2)}"
            ax.text(0.75, 0.01, chart_text, transform=ax.transAxes, fontsize=14)
            fig.tight_layout()
            temp_file = os.path.join(temp_dir, f"gif_temp_{i}.png")
            filenames.append(temp_file)
            fig.savefig(temp_file)
            plt.close()
        gif_file = os.path.join(os.getcwd(), 'Charts', self.distance_name, 'gif', f"{chart_title}.gif")
        with imageio.get_writer(gif_file, mode='I') as writer:
            for file in filenames:
                image = imageio.imread(file)
                writer.append_data(image)
                if file == filenames[-1]:
                    for i in range(25):
                        writer.append_data(image)

        for file in filenames:
            os.remove(file)

    def close(self):
        self.conn.close()


def main(update_every_year: bool = False, update_all: bool = True):
    rc = RunningCharts()
    try:
        if update_every_year:
            for yr in rc.year_range:
                rc.time_of_day(yr)
                rc.distance_and_pace(yr)
                rc.distance_and_pace_gif(yr)
        else:
            yr = date.today().year
            rc.time_of_day(yr)
            rc.distance_and_pace(yr)
            rc.distance_and_pace_gif(yr)
        if update_all:
            rc.time_of_day()
            rc.distance_and_pace()
            rc.dist_per_month()
            rc.distance_and_pace_gif()
    except Exception as e:
        logger.error(e, exc_info=sys.exc_info())
        print('Error - see logs')
    finally:
        rc.close()


if __name__ == '__main__':
    main()
