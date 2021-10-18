#!/usr/bin/env python3

import requests
import json
import pandas as pd
import numpy as np
import os
import sys
import time
import geocoder
from collections import defaultdict
from datetime import datetime, date
from strava_logging import logger
from db_connection import connect, sql


class Athlete:
    def __init__(self, **kwargs):
        self.conn = self.create_connection()
        if kwargs:
            self.cond = next(iter(kwargs.keys()))
            self.val = next(iter(kwargs.values()))
        else:
            self.cond = None
            self.val = None
        self.df = self.return_df()
        self.ath_info = self.athlete_info()

    @staticmethod
    def create_connection():
        return connect()

    def create_new_athlete(self, athlete_id: int, client_id: int, client_secret: str, refresh_token: str,
                           firstname: str, lastname: str):
        """
        Creates a new athlete in the database.

        :param athlete_id: Identifier of the athlete in Strava
        :param client_id: ID provided to access the athlete's data in the API
        :param client_secret: Secret code for this API user.
        :param refresh_token: Token used to refresh the API connection.
        :param firstname: First name of the athlete.
        :param lastname: Last name of the athlete.
        :return:
        """
        new_athlete_info = {
            'athlete_id': athlete_id,
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': refresh_token,
            'firstname': firstname,
            'lastname': lastname
        }
        df_new = pd.DataFrame(new_athlete_info)
        conn = self.create_connection()
        df_new.to_sql('athletes', conn, if_exists='append', index=False)
        conn.close()

    def return_df(self) -> pd.DataFrame:
        df = pd.read_sql(sql="""SELECT * FROM athletes""", con=self.conn)
        self.close_conn()
        if self.cond is not None and self.val is not None and self.cond in df.columns:
            df = df.loc[df[self.cond] == self.val]
        return df

    def athlete_info(self) -> dict:
        """
        Returns the athlete's data which will be used in the Activities class.

        :return:
        """
        return self.df.to_dict(orient='records')[0]

    def close_conn(self):
        self.conn.close()


class Activities:
    def __init__(self, athlete_info: dict):
        self.athlete_info = athlete_info
        assert self.athlete_info is not None, f"Please provide athlete info. " \
                                              f"Client_id, client_secret and refresh_token required."
        self.athlete_id = self.athlete_info['athlete_id']
        self.base_url = 'https://www.strava.com/api/v3'
        self.refresh_data = self.refresh_api_connection()
        self.access_token = self.refresh_data['access_token']
        self.headers = {'Authorization': f"Bearer {self.access_token}"}
        self.token_expires = self.refresh_data['expires_at']
        self.conn = connect()
        self.latest_activity = self.get_latest_activity()
        self.earliest_activity = self.get_earliest_activity()
        self.existing_locations = self.get_existing_locations()
        self.existing_gear = self.get_existing_gear()
        self.df = pd.DataFrame()
        self.df_details = pd.DataFrame()
        self.df_km = pd.DataFrame()
        self.df_miles = pd.DataFrame()

    def refresh_api_connection(self):
        """
        Retrieves a new access token from the API. The access token will be used in the headers for later API calls.

        :return:
        """
        refresh_params = {
            'client_id': self.athlete_info.get('client_id'),
            'client_secret': self.athlete_info.get('client_secret'),
            'refresh_token': self.athlete_info.get('refresh_token'),
            'grant_type': 'refresh_token',
        }
        refresh_response = requests.post(url='https://www.strava.com/oauth/token', params=refresh_params)
        assert refresh_response.ok, f"{refresh_response.status_code}, {refresh_response.text}"
        refresh_data = json.loads(refresh_response.text)
        refresh_data['expires_at_str'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(refresh_data['expires_at']))
        logger.info(f"Access token will expire at {refresh_data['expires_at_str']}.")
        return refresh_data

    def check_expiry(self):
        """
        This method checks if the token has expired. If the token has expired, it refreshes the token.

        :return:
        """
        if time.time() > self.token_expires:
            self.refresh_api_connection()

    def get_latest_activity(self):
        """
        Returns the athlete's latest activity. To be used with the after parameter of get_activities.

        :return:
        """
        query = sql.text("SELECT MAX(start_date) FROM activities WHERE athlete_id =:ath_id")
        return self.conn.execute(query, {'ath_id': self.athlete_info.get('athlete_id')}).fetchone()[0]

    def get_earliest_activity(self):
        """
        Returns the athlete's earliest activity. To be used with the before parameter of get_activities.

        :return:
        """
        query = sql.text("SELECT MIN(start_date) FROM activities WHERE athlete_id =:ath_id")
        return self.conn.execute(query, {'ath_id': self.athlete_info.get('athlete_id')}).fetchone()[0]

    def get_existing_locations(self) -> list:
        """
        Returns a list of locations that have already be saved in the database.

        :return: A list of tuples with latitude and longitude.
        """
        df_l = pd.read_sql('locations', self.conn)
        return list(zip(df_l['latitude'], df_l['longitude']))

    def get_existing_gear(self) -> list:
        """
        Returns a list of gear that is already saved in the database.

        :return: List of gear IDs
        """
        df_g = pd.read_sql('gear', self.conn)
        return df_g['gear_id'].to_list()

    def get_activities(self, save_json: bool = False, **kwargs):
        """
        Returns data for multiple activities that meet the parameters provided.
        The main use case is to retrieve all activities after the athlete's latest activity in the database
        therefore the default 'after' value will be the latest start_date. If there are no activities for the athlete
        the after value will be None.
        The results are concatenated onto the main dataframe with activities.

        :param save_json: Option to save API response data as a json file, defaults to False
        :param kwargs:
            after - return activities after this date provided as datetime, date, or str in 'yyyy-mm-dd' format,
            before - return activities before this date provided as datetime, date, or str in 'yyyy-mm-dd' format,
            per_page - number of activities per page (default and max are 200 to minimize API calls),
            page - starting page number
        :return:
        """
        after = kwargs.get('after', self.latest_activity)
        before = kwargs.get('before', None)
        if after is not None:
            if isinstance(after, str):
                after = datetime.timestamp(datetime.strptime(after, '%Y-%m-%d'))
            elif isinstance(after, datetime):
                after = datetime.timestamp(after)
            elif isinstance(after, date):
                after = datetime.timestamp(datetime.combine(after, datetime.min.time()))
        if before is not None:
            if isinstance(before, str):
                before = datetime.timestamp(datetime.strptime(before, '%Y-%m-%d'))
            elif isinstance(before, datetime):
                after = datetime.timestamp(before)
            elif isinstance(before, date):
                after = datetime.timestamp(datetime.combine(before, datetime.min.time()))
        per_page = kwargs.get('per_page', 200)
        page = kwargs.get('page', 1)
        response = requests.get(url=f"{self.base_url}/athlete/activities",
                                headers=self.headers,
                                params={'after': after, 'before': before, 'per_page': per_page, 'page': page})
        assert response.ok, f"{response.status_code}, {response.text}"
        response_data = json.loads(response.text)
        if save_json:
            data_file = os.path.join('Activities', 'activity_lists',
                                     f"activity_list {datetime.now().strftime('%Y-%m-%d %H%M%S')}.json")
            with open(data_file, 'w') as f:
                json.dump(response_data, f)
        if len(response_data) > 0:
            self.df = pd.concat([self.df, pd.json_normalize(response_data)]).reset_index(drop=True)
            time.sleep(2)
            return self.get_activities(page=(page + 1))

    def get_activity_ids(self) -> list:
        """
        Returns a list of all activity IDs to be used later in get_activity_details

        :return: list of all activity IDs
        """
        if 'id' in self.df.columns:
            return self.df['id'].to_list()
        elif 'activity_id' in self.df.columns:
            return self.df['activity_id'].to_list()

    def get_activity_detail(self, activity_id: int, relevant_fields: list = None, save_json: bool = False):
        """
        There are certain items that are only available by calling the API for each activity ID, notably the split info,
        the activity description, the perceived exertion, the device name and number of calories. Activity info will
        later be joined to main activity data. Splits are saved in separate dataframes for splits_miles and splits_km.

        :param activity_id: ID of the activity
        :param relevant_fields: List of fields that I am interested in which are not available from activity lists.
            Default items are  ['id', 'description', 'perceived_exertion', 'device_name', 'calories']
        :param save_json: Option to save API response data as a json file, defaults to False
        :return:
        """
        if relevant_fields is None:
            relevant_fields = ['id', 'description', 'perceived_exertion', 'device_name', 'calories']
        activity_response = requests.get(url=f"{self.base_url}/activities/{activity_id}", headers=self.headers)
        assert activity_response.ok, f"{activity_response.status_code}, {activity_response.text}"
        activity_data = json.loads(activity_response.text)
        if save_json:
            data_file = os.path.join('Activities', 'individual_activities', f"{activity_id}.json")
            with open(data_file, 'w') as f:
                json.dump(activity_data, f)
        relevant_data = {x: activity_data[x] for x in activity_data.keys() if x in relevant_fields}
        df_activity_details = pd.DataFrame(relevant_data, index=[0])
        df_activity_details.rename(columns={'id': 'activity_id', 'description': 'activity_description'}, inplace=True)
        self.df_details = pd.concat([self.df_details, df_activity_details]).reset_index(drop=True)

        splits_km = pd.DataFrame(activity_data['splits_metric'])
        splits_km['activity_id'] = activity_id
        self.df_km = pd.concat([self.df_km, splits_km]).reset_index(drop=True)

        splits_miles = pd.DataFrame(activity_data['splits_standard'])
        splits_miles['activity_id'] = activity_id
        self.df_miles = pd.concat([self.df_miles, splits_miles]).reset_index(drop=True)

    def update_gear(self):
        """
        Checks if there are any new gear IDs in the activities returned. New gear is added to the database by calling
        the add_gear method.

        :return:
        """
        df_g = self.df.copy()[['gear_id']]
        df_g.drop_duplicates(inplace=True)
        df_g.dropna(inplace=True)
        new_gear = [gr for gr in df_g['gear_id'].to_list() if gr not in self.existing_gear]
        for gear in new_gear:
            self.add_gear(gear)

    def add_gear(self, gear_id: str):
        """
        Adds any new gear to the database.

        :param gear_id: Gear identifier from Strava API
        :return:
        """
        gear_response = requests.get(url=f"{self.base_url}/gear/{gear_id}", headers=self.headers)
        assert gear_response.ok, f"{gear_response.status_code}, {gear_response.text}"
        gear_data = json.loads(gear_response.text)
        df_gear = pd.DataFrame(gear_data, index=[0])
        df_gear.rename(columns={'id': 'gear_id', 'name': 'gear_name'}, inplace=True)
        final_cols = [
            'gear_id',
            'gear_name',
            'nickname',
            'brand_name',
            'model_name',
            'description',
            'notification_distance',
            'retired']
        cols = [c for c in final_cols if c in df_gear.columns]
        df_gear = df_gear[cols]
        df_gear.to_sql('gear', self.conn, if_exists='append', index=False)

    def update_locations(self):
        """
        Checks if there are any new locations in the activities returned. New locations are added to the database
        by calling the add_location method.

        :return:
        """
        df_l = self.df.copy()[['start_latitude', 'start_longitude']]
        df_l.drop_duplicates(inplace=True)
        df_l.dropna(inplace=True)
        locations = list(zip(df_l['start_latitude'], df_l['start_longitude']))
        new_locations = [ll for ll in locations if ll not in self.existing_locations]
        for ll in new_locations:
            self.add_location(lat=ll[0], lon=ll[1])

    def add_location(self, lat: float, lon: float):
        """
        Adds new locations to the database.

        :param lat: Latitude of the new location.
        :param lon: Longitude of the new location.
        :return:
        """
        location = defaultdict(list)
        g = geocoder.osm(location=(lat, lon), method='reverse')
        assert g.ok, f"Error response: {g.response.text}"
        location['latitude'].append(lat)
        location['longitude'].append(lon)
        location['place_id'].append(g.place_id)
        location['osm_id'].append(g.osm_id)
        location['country_code'].append(g.country_code.upper())
        location['country'].append(g.country)
        location['region'].append(g.region)
        location['state_name'].append(g.state)
        location['city'].append(g.city)
        location['town'].append(g.town)
        location['village'].append(g.village)
        location['suburb'].append(g.suburb)
        location['quarter'].append(g.quarter)
        location['neighborhood'].append(g.neighborhood)
        location['street'].append(g.street)
        location['postal'].append(g.postal)
        location['address'].append(g.address)
        df_l = pd.DataFrame(location)
        df_l.to_sql('locations', self.conn, if_exists='append', index=False)

    def get_athlete(self) -> pd.DataFrame:
        """
        Returns a dataframe with data about the athlete. The data is not automatically added to the database because
        that athlete may already exist.

        :return: Dataframe with data about the athlete.
        """
        athlete_response = requests.get(url=f"{self.base_url}/athlete", headers=self.headers)
        assert athlete_response.ok, f"{athlete_response.status_code}, {athlete_response.text}"
        ath_data = json.loads(athlete_response.text)
        df_ath = pd.DataFrame(ath_data, index=[0])
        df_ath.rename(columns={'id': 'athlete_id', 'state': 'state_'}, inplace=True)
        df_ath.drop(columns=['resource_state', 'friend', 'follower'], inplace=True)
        for col in ['created_at', 'updated_at']:
            df_ath[col] = pd.to_datetime(df_ath[col], errors='coerce')
        return df_ath

    def format_activities_df(self):
        """
        Formatting the activity data to prepare it for the database.

        :return:
        """
        self.df['distance_miles'] = self.df['distance'] * 0.0006213712
        self.df['distance_miles'] = self.df['distance_miles'].round(2)
        self.df['distance_km'] = self.df['distance'] / 1000
        self.df['distance_km'] = self.df['distance_km'].round(2)
        self.df['min_per_mile'] = (self.df['moving_time'] / 60) / self.df['distance_miles']
        self.df['min_per_mile'] = self.df['min_per_mile'].round(4)
        self.df['end_latitude'] = self.df['end_latlng'].apply(lambda x: x[0] if len(x) == 2 else np.nan)
        self.df['end_longitude'] = self.df['end_latlng'].apply(lambda x: x[1] if len(x) == 2 else np.nan)
        self.df['start_date'] = pd.to_datetime(self.df['start_date'], errors='coerce')
        self.df['activity_date'] = self.df['start_date'].dt.date
        self.df['start_date_local'] = pd.to_datetime(self.df['start_date_local'], errors='coerce')
        self.df['end_date'] = self.df['start_date'] + pd.to_timedelta(self.df['elapsed_time'], unit='seconds')
        self.df['end_date_local'] = self.df['start_date_local'] + pd.to_timedelta(self.df['elapsed_time'],
                                                                                  unit='seconds')
        self.df['hour_of_day'] = self.df['start_date_local'].dt.hour
        self.df['day_of_week'] = self.df['start_date_local'].dt.day_name()
        self.df['year_'] = self.df['start_date_local'].dt.year
        self.df.rename(columns={
            'id': 'activity_id',
            'athlete.id': 'athlete_id',
            'type': 'activity_type',
            'name': 'activity_name',
            'distance': 'distance_meters',
            'average_speed': 'average_speed_ms',
            'max_speed': 'max_speed_ms',
            'average_heartrate': 'heartrate'
        }, inplace=True)
        self.df = self.df.merge(self.df_details, how='left', on='activity_id')
        self.df['activity_description'].replace('', np.nan, inplace=True)
        cols = [
            # basics
            'activity_id', 'athlete_id', 'activity_type', 'workout_type', 'activity_date', 'activity_name',
            'activity_description', 'perceived_exertion', 'athlete_count', 'commute',
            # distance
            'distance_miles', 'distance_meters', 'distance_km',
            # speed
            'average_speed_ms', 'min_per_mile', 'max_speed_ms',
            # time
            'start_date', 'start_date_local', 'timezone', 'moving_time', 'elapsed_time', 'end_date',
            'end_date_local', 'hour_of_day', 'day_of_week', 'year_',
            # location
            'start_latitude', 'start_longitude', 'end_latitude', 'end_longitude',
            # Records
            'pr_count', 'achievement_count',
            # Health
            'has_heartrate', 'heartrate', 'max_heartrate', 'calories',
            # Elevation
            'elev_high', 'elev_low', 'total_elevation_gain',
            # Cycling Specific
            'average_watts', 'kilojoules',
            # ID References
            'gear_id', 'device_name', 'upload_id', 'external_id'
        ]
        for col in cols:
            if col not in self.df.columns:
                self.df[col] = np.nan
        self.df = self.df[cols]

    def save_activities(self, backup_csv: str = None):
        """
        Adds new activities to the database. Optionally a CSV with the activity data can also be saved.

        :param backup_csv: (optional) the path where the CSV file will be saved
        :return:
        """
        current = pd.read_sql('activities', self.conn, params={'athlete_id': self.athlete_id}, columns=['activity_id'])
        self.df = pd.merge(self.df, current, how='left', on='activity_id', indicator=True)
        self.df = self.df.loc[self.df['_merge'] == 'left_only']
        self.df.drop(columns=['_merge'], inplace=True)
        self.df.to_sql('activities', self.conn, if_exists='append', index=False)
        if backup_csv:
            self.df.to_csv(backup_csv, index=False, encoding='utf-8-sig')

    def save_splits(self):
        """
        Adds splits data to the database.

        :return:
        """
        for df in [self.df_miles, self.df_km]:
            df.rename(columns={'average_speed': 'average_speed_ms', 'average_heartrate': 'heartrate'}, inplace=True)
            df['heartrate'] = df['heartrate'].round(2)
        self.df_miles.to_sql('splits_miles', self.conn, if_exists='append', index=False)
        self.df_km.to_sql('splits_km', self.conn, if_exists='append', index=False)

    def close(self):
        self.conn.close()


def main():
    logger.info('-' * 100)
    backup_file = os.path.join('Results', f"Strava Data {datetime.now().strftime('%Y-%m-%d %H%M')}.csv")
    me = Athlete(firstname='Lance')
    activities = Activities(getattr(me, 'ath_info'))
    try:
        activities.get_activities()
        for a_id in activities.get_activity_ids():
            activities.get_activity_detail(a_id)
            time.sleep(2)
        activities.format_activities_df()
        activities.save_activities(backup_file)
        activities.save_splits()
    except Exception as e:
        logger.error(e, exc_info=sys.exc_info())
    finally:
        activities.close()


if __name__ == '__main__':
    main()
