import os
import sys
import time
import logging
import configparser
import pandas as pd
from datetime import datetime
from stravalib import Client
from stravalib import unithelper
from collections import defaultdict


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
        self.additional_dict = defaultdict(list)
        self.token_expires = None
        self.results_file = os.path.join('Results', 'Strava Data.csv')
        if os.path.exists(self.results_file):
            self.df = pd.read_csv(self.results_file)
            self.latest_activity = self.df['start_datetime'].max()
        else:
            self.df = None
            self.latest_activity = None
        self.cols = ['activity_id', 'activity_type', 'start_datetime', 'activity_name', 'activity_description',
                     'commute', 'distance_miles', 'distance_meters', 'moving_time_sec', 'elapsed_time_sec', 'splits',
                     'fastest_mile', 'fastest_mile_time', 'average_speed_ms', 'avg_speed_min_mile', 'max_speed_ms',
                     'country', 'state', 'city', 'start_lat', 'start_long', 'end_lat', 'end_long', 'average_temp',
                     'elev_high_m', 'elev_low_m', 'total_elevation_gain_m', 'has_heartrate', 'average_heartrate',
                     'max_heartrate', 'calories', 'pr_count', 'best_efforts', 'average_watts', 'kilojoules',
                     'device_name', 'gear', 'gear_id', 'upload_id', 'external_id', 'start_datetime_utc', 'start_date',
                     'start_time']

    def refresh(self):
        """
        This is used to connect to the API initially
        The token expires time is stored in self.token_expires.
        :return:
        """
        refresh_response = self.client.refresh_access_token(client_id=config['Strava']['ClientID'],
                                                            client_secret=config['Strava']['ClientSecret'],
                                                            refresh_token=config['Strava']['RefreshToken'])
        self.token_expires = refresh_response['expires_at']
        logging.info(f"Refresh token will expire at "
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(refresh_response['expires_at']))}.")

    def check_expiry(self):
        """
        This function checks to see if the token has expired.
        If the token has expired, it refreshes the token.
        :return:
        """
        if time.time() > self.token_expires:
            self.refresh()

    def get_activities(self):
        """
        This function gets most of the activity data I'm interested in.
        This usually only results in about 4 API calls so I don't have to worry about rate limiting
        However, retrieving data through this method does not return split information, gear, calories or device name.
        To return those data points, use the additional_info function.
        :return: The data is saved to a dictionary which can be turned into a data frame
        """
        if self.latest_activity is not None:
            activities = self.client.get_activities(after=self.latest_activity)
            logger.info(f'Retrieving records after {self.latest_activity}.')
        else:
            activities = self.client.get_activities()
        logger.info('Retrieving main activity data.')
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
            assert int(distance_miles) > 0, f'Distance for activity ID {activity.id} is zero'
            self.activities_dict['distance_miles'].append(distance_miles)
            self.activities_dict['distance_meters'].append(activity.distance.get_num())
            # Time
            moving_time_sec = activity.moving_time.seconds
            self.activities_dict['moving_time_sec'].append(moving_time_sec)
            self.activities_dict['elapsed_time_sec'].append(activity.elapsed_time.seconds)
            # Speed
            # splits_dict = self.get_splits(activity.splits_standard)
            # self.activities_dict['splits'].append(splits_dict)
            # if splits_dict is not None:
            #     fastest_mile = min(splits_dict, key=splits_dict.get)
            #     self.activities_dict['fastest_mile'].append(fastest_mile)
            #     self.activities_dict['fastest_mile_time'].append(splits_dict[fastest_mile])
            # else:
            #     self.activities_dict['fastest_mile'].append(None)
            #     self.activities_dict['fastest_mile_time'].append(None)
            self.activities_dict['average_speed_ms'].append(activity.average_speed.get_num())
            self.activities_dict['avg_speed_min_mile'].append((moving_time_sec / 60) / distance_miles)
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
            self.activities_dict['has_heartrate'].append(activity.has_heartrate)
            self.activities_dict['average_heartrate'].append(activity.average_heartrate)
            self.activities_dict['max_heartrate'].append(activity.max_heartrate)
            # self.activities_dict['calories'].append(activity.calories)
            # Records
            self.activities_dict['pr_count'].append(activity.pr_count)
            self.activities_dict['best_efforts'].append(activity.best_efforts)
            # Cycling specific
            self.activities_dict['average_watts'].append(activity.average_watts)
            self.activities_dict['kilojoules'].append(activity.kilojoules)
            # Additional
            # self.activities_dict['device_name'].append(activity.device_name)
            # self.activities_dict['gear'].append(activity.gear.name)
            # self.activities_dict['gear_id'].append(activity.gear_id)
            self.activities_dict['upload_id'].append(activity.upload_id)
            self.activities_dict['external_id'].append(activity.external_id)
            self.activities_dict['start_datetime_utc'].append(activity.start_date.strftime('%Y-%m-%d %H:%M:%S'))
            self.activities_dict['start_date'].append(start_datetime.strftime('%Y-%m-%d'))
            self.activities_dict['start_time'].append(start_datetime.strftime('%H:%M:%S'))
            # Refresh if necessary
            self.check_expiry()
        logger.info('Finished retrieving main activity data.')
        logger.info(f"{len(self.activities_dict['activity_id'])} records retrieved")

    def additional_info(self):
        """
        This is used to retrieve the split information, gear, calories and device name.
        To retrieve that information, you apparently have to use a specific activity ID.
        This results in far more API calls since you are making one call per activity.
        It also takes longer to retrieve since you have to wait to avoid the rate limit.
        Rate limit = 600 calls per 15 minutes (i.e. 40 calls per minute or 2 calls every 3 seconds)
        :return:
        """
        logger.info('Retrieving additional activity data.')
        logger.setLevel(logging.ERROR)
        for activity in self.activities_dict['activity_id']:
            this_activity = self.client.get_activity(activity)
            # self.activities_dict['activity_id'].append(activity)
            splits_dict = self.get_splits(this_activity.splits_standard)
            self.activities_dict['splits'].append(splits_dict)
            if splits_dict is not None:
                fastest_mile = min(splits_dict, key=splits_dict.get)
                self.activities_dict['fastest_mile'].append(fastest_mile)
                self.activities_dict['fastest_mile_time'].append(splits_dict[fastest_mile])
            else:
                self.activities_dict['fastest_mile'].append(None)
                self.activities_dict['fastest_mile_time'].append(None)
            if this_activity.gear is not None:
                self.activities_dict['gear'].append(this_activity.gear.name)
                self.activities_dict['gear_id'].append(this_activity.gear.id)
            else:
                self.activities_dict['gear'].append(None)
                self.activities_dict['gear_id'].append(None)
            self.activities_dict['calories'].append(this_activity.calories)
            self.activities_dict['device_name'].append(this_activity.device_name)
            time.sleep(1)
            self.check_expiry()
        logger.setLevel(logging.INFO)
        logger.info('Finished retrieving additional activity data.')
        # Adding additional info to main activities dictionary
        # self.activities_dict.update(self.additional_dict)

    @staticmethod
    def check_dict(dictionary):
        """Before creating a data frame from the dictionary,
        check that the each item in the dictionary has the same number of values"""
        first_key = next(iter(dictionary))
        num_values = len(dictionary[first_key])
        keys_to_del = []
        for k in dictionary:
            if len(dictionary[k]) != num_values:
                logger.info(f'{k} has {len(dictionary[k])} values, the expected amount is {num_values}.')
                keys_to_del.append(k)
        if len(keys_to_del) > 0:
            for k in keys_to_del:
                dictionary.pop(k, None)
                logger.info(f'Column {k} was removed from data')

    @staticmethod
    def get_splits(splits_standard):
        """
        Creates a dictionary for splits in this format
        mile number : time
        If the last mile is incomplete, it will be given as a float to 4 decimal places
        :param splits_standard: the splits_standard from Strava API
        :return:
        """
        mile_count = 1
        splits = {}
        if splits_standard:
            for split in splits_standard:
                time_min = round((split.moving_time.seconds / 60), 4)
                if split.distance.get_num() < 1600:
                    mile_fraction = round((split.distance.get_num() / 1609), 2)
                    if mile_fraction > 0:
                        mile_count = (mile_count - 1) + mile_fraction
                        splits[mile_count] = round(time_min / mile_fraction, 4)
                    else:
                        pass
                else:
                    splits[mile_count] = time_min
                mile_count += 1
            return splits
        else:
            return None

    def create_data_frame(self):
        """
        Creates a data frame from the activities_dict dictionary.
        The data frame is saved as a class variable.
        :return:
        """
        self.check_dict(self.activities_dict)
        if self.df is not None:
            df_new = pd.DataFrame(self.activities_dict)
            self.df = self.df.append(df_new, sort=False)
            self.df.sort_values(by='start_datetime', ascending=False, inplace=True)
            self.df.reset_index(drop=True, inplace=True)
        else:
            self.df = pd.DataFrame(self.activities_dict)

    def save_data_frame(self, include_datetime=False):
        """
        Saves the data frame as a file with the name 'Strava Data' and the current datetime.
        :return:
        """
        if not os.path.exists('Results'):
            os.mkdir('Results')
        if include_datetime:
            file_name = f"Strava Data {datetime.utcnow().strftime('%Y-%m-%d %H%M')}.csv"
        else:
            file_name = "Strava Data.csv"
        self.df.to_csv(os.path.join(os.getcwd(), 'Results', file_name), index=False, encoding='utf-8-sig')
        logger.info(f'File saved as {file_name}')


if __name__ == '__main__':
    try:
        sd = StravaData()
        sd.refresh()
        sd.get_activities()
        sd.additional_info()
        sd.create_data_frame()
        sd.save_data_frame()
        print('Complete')
    except Exception as e:
        logger.error(e, exc_info=sys.exc_info())
        logger.info('-' * 100)
        print('Error')
