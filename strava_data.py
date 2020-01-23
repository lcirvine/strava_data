import os
import sys
import time
import logging
import configparser
from datetime import datetime
from stravalib import Client
from stravalib import unithelper
from collections import defaultdict
import pandas as pd


if not os.path.exists('Logs'):
    os.mkdir('Logs')
handler = logging.FileHandler(os.path.join(os.getcwd(), 'Logs', 'Strava Data Logs.txt'), mode='a+',
                              encoding='UTF-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.info('-' * 100)

config = configparser.ConfigParser()
config.read('strava_api_settings.ini')


class StravaData:
    def __init__(self):
        self.client = Client()
        self.activities_dict = defaultdict(list)
        self.token_expires = None

    def refresh(self):
        refresh_response = self.client.refresh_access_token(client_id=config['Strava']['ClientID'],
                                                            client_secret=config['Strava']['ClientSecret'],
                                                            refresh_token=config['Strava']['RefreshToken'])
        self.token_expires = refresh_response['expires_at']
        logging.info(f"Refresh token will expire at "
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(refresh_response['expires_at']))}.")

    def check_expiry(self):
        if time.time() > self.token_expires:
            self.refresh()

    def get_activities(self, limit=None):
        """
        https://pythonhosted.org/stravalib/api.html#stravalib.model.Activity
        :param limit:
        :return:
        """
        if limit:
            activities = self.client.get_activities(limit=limit)
            logger.info(f'Retrieving {limit} records.')
        else:
            activities = self.client.get_activities()
        logger.info('Retrieving activity data from Strava.')
        for activity in activities:
            # Basics
            self.activities_dict['activity_id'].append(activity.id)
            self.activities_dict['activity_type'].append(activity.type)
            start_datetime = activity.start_date_local
            self.activities_dict['start_datetime'].append(start_datetime.strftime('%Y-%m-%d %H:%M:%S'))
            self.activities_dict['activity_name'].append(activity.name)
            self.activities_dict['activity_description'].append(activity.description)
            self.activities_dict['commute'].append(activity.commute)
            # Distance
            distance_miles = unithelper.miles(activity.distance).get_num()
            self.activities_dict['distance_miles'].append(distance_miles)
            self.activities_dict['distance_meters'].append(activity.distance.get_num())
            # Time
            moving_time_sec = activity.moving_time.seconds
            self.activities_dict['moving_time_sec'].append(moving_time_sec)
            self.activities_dict['elapsed_time_sec'].append(activity.elapsed_time.seconds)
            # Speed
            self.activities_dict['average_speed_ms'].append(activity.average_speed.get_num())
            if distance_miles != 0:
                self.activities_dict['avg_speed_min_mile'].append((moving_time_sec / 60) / distance_miles)
            else:
                self.activities_dict['avg_speed_min_mile'].append(None)
            self.activities_dict['max_speed_ms'].append(activity.max_speed.get_num())
            # Location
            self.activities_dict['country'].append(activity.location_country)
            self.activities_dict['state'].append(activity.location_state)
            self.activities_dict['city'].append(activity.location_city)
            self.activities_dict['start_lat'].append(activity.start_latitude)
            self.activities_dict['start_long'].append(activity.start_longitude)
            if activity.end_latlng is not None:
                self.activities_dict['end_lat'].append(activity.end_latlng.lat)
                self.activities_dict['end_long'].append(activity.end_latlng.lon)
            else:
                self.activities_dict['end_lat'].append(None)
                self.activities_dict['end_long'].append(None)
            self.activities_dict['average_temp'].append(activity.average_temp)
            # Elevation
            self.activities_dict['elev_high_m'].append(activity.elev_high)
            self.activities_dict['elev_low_m'].append(activity.elev_low)
            self.activities_dict['total_elevation_gain_m'].append(activity.total_elevation_gain.get_num())
            # Health
            self.activities_dict['average_heartrate'].append(activity.average_heartrate)
            self.activities_dict['calories'].append(activity.calories)
            # Records
            self.activities_dict['pr_count'].append(activity.pr_count)
            self.activities_dict['best_efforts'].append(activity.best_efforts)
            # Cycling specific
            self.activities_dict['average_watts'].append(activity.average_watts)
            self.activities_dict['kilojoules'].append(activity.kilojoules)
            # Additional
            self.activities_dict['device_name'].append(activity.device_name)
            self.activities_dict['upload_id'].append(activity.upload_id)
            self.activities_dict['start_datetime_utc'].append(activity.start_date.strftime('%Y-%m-%d %H:%M:%S'))
            self.activities_dict['start_date'].append(start_datetime.strftime('%Y-%m-%d'))
            self.activities_dict['start_time'].append(start_datetime.strftime('%H:%M:%S'))

            self.check_expiry()

    def check_dict(self):
        """Before creating a data frame from the dictionary,
        check that the each item in the dictionary has the same number of values"""
        first_key = next(iter(self.activities_dict))
        num_values = len(self.activities_dict[first_key])
        keys_to_del = []
        for k in self.activities_dict:
            if len(self.activities_dict[k]) != num_values:
                logger.info(f'{k} has {len(self.activities_dict[k])} values, the expected amount is {num_values}.')
                keys_to_del.append(k)
        if len(keys_to_del) > 0:
            for k in keys_to_del:
                self.activities_dict.pop(k, None)
                logger.info(f'Column {k} was removed from data')

    def create_data_frame(self):
        self.check_dict()
        df = pd.DataFrame(self.activities_dict)
        logger.info(f'{len(df)} records retrieved')
        if not os.path.exists('Results'):
            os.mkdir('Results')
        file_name = f"Strava Data {datetime.utcnow().strftime('%Y-%m-%d %H%M')}.csv"
        df.to_csv(os.path.join(os.getcwd(), 'Results', file_name), index=False, encoding='utf-8-sig')
        logger.info(f'File saved as {file_name}')


if __name__ == '__main__':
    try:
        sd = StravaData()
        sd.refresh()
        sd.get_activities()
        sd.create_data_frame()
    except Exception as e:
        logger.info(e, exc_info=sys.exc_info())
        logger.info('-' * 100)
