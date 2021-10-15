#!/usr/bin/env python3

import os
import sys
import time
from strava_logging import logger
from db_connection import connect, sql_types, sql
import geocoder
import pandas as pd
from datetime import datetime, timedelta
from stravalib import Client
from stravalib import unithelper
from collections import defaultdict


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
                           first_name: str, last_name: str):
        new_athlete_info = {
            'athlete_id': athlete_id,
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': refresh_token,
            'first_name': first_name,
            'last_name': last_name
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
        return self.df.to_dict(orient='records')[0]

    def close_conn(self):
        self.conn.close()


class Activities:
    def __init__(self, athlete_info: dict):
        self.athlete_info = athlete_info
        assert self.athlete_info is not None, f"Please provide athlete info. " \
                                              f"Client_id, client_secret and refresh_token required."
        self.client = Client(rate_limit_requests=False)
        self.activities = defaultdict(list)
        self.splits_miles_data = defaultdict(list)
        self.splits_km_data = defaultdict(list)
        self.conn = connect()
        self.latest_activity = self.get_latest_activity()
        self.earliest_activity = self.get_earliest_activity()
        self.existing_locations = self.get_existing_locations()
        self.existing_gear = self.get_existing_gear()
        self.token_expires = None
        self.refresh_connection()

    def get_latest_activity(self):
        query = sql.text("SELECT MAX(start_date_utc) FROM activities WHERE athlete_id =:ath_id")
        return self.conn.execute(query, {'ath_id': self.athlete_info.get('athlete_id')}).fetchone()[0]

    def get_earliest_activity(self):
        query = sql.text("SELECT MIN(start_date_utc) FROM activities WHERE athlete_id =:ath_id")
        return self.conn.execute(query, {'ath_id': self.athlete_info.get('athlete_id')}).fetchone()[0]

    def get_existing_locations(self) -> list:
        df_l = pd.read_sql('locations', self.conn)
        return list(zip(df_l['latitude'], df_l['longitude']))

    def get_existing_gear(self) -> list:
        df_g = pd.read_sql('gear', self.conn)
        return df_g['gear_id'].to_list()

    def refresh_connection(self):
        """
        This is used to connect to the API initially
        The token expires time is stored in self.token_expires.
        :return:
        """
        refresh_response = self.client.refresh_access_token(client_id=self.athlete_info.get('client_id'),
                                                            client_secret=self.athlete_info.get('client_secret'),
                                                            refresh_token=self.athlete_info.get('refresh_token'))
        self.token_expires = refresh_response['expires_at']
        logger.info(f"Refresh token will expire at "
                    f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(refresh_response['expires_at']))}.")

    def check_expiry(self):
        """
        This function checks to see if the token has expired.
        If the token has expired, it refreshes the token.
        :return:
        """
        if time.time() > self.token_expires:
            self.refresh_connection()

    def get_activities(self, **kwargs):
        """
        This function gets most of the activity data I'm interested in.
        This usually only results in about 4 API calls so I don't have to worry about rate limiting
        However, retrieving data through this method does not return split information, gear, calories or device name.
        To return those data points, use the additional_info function.
        :return: The data is saved to a dictionary which can be turned into a data frame
        """
        logger.info(f"Retrieving all activities for {self.athlete_info.get('athlete_id')}, {str(kwargs)}.")
        activities = self.client.get_activities(before=kwargs.get('before', None), after=kwargs.get('after', None))
        for activity in activities:
            # these will be referenced later
            start_datetime = getattr(activity, 'start_date_local', None)
            moving_time_sec = getattr(activity.moving_time, 'seconds', None)
            elapsed_time = getattr(activity.elapsed_time, 'seconds', None)
            distance_miles = unithelper.miles(activity.distance).get_num()
            # assert int(distance_miles) > 0, f'Distance for activity ID {activity.id} is zero'
            # Basics
            self.activities['activity_id'].append(activity.id)
            self.activities['athlete_id'].append(activity.athlete.id)
            self.activities['activity_type'].append(activity.type)
            self.activities['activity_date'].append(start_datetime.strftime('%Y-%m-%d'))
            self.activities['activity_name'].append(activity.name)
            self.activities['activity_description'].append(getattr(activity, 'description', None))
            self.activities['gear_id'].append(activity.gear_id)
            self.activities['commute'].append(activity.commute)
            # Distance
            self.activities['distance_miles'].append(distance_miles)
            self.activities['distance_meters'].append(activity.distance.get_num())
            # Speed
            self.activities['average_speed_ms'].append(activity.average_speed.get_num())
            self.activities['avg_speed_min_mile'].append((moving_time_sec / 60) / distance_miles)
            self.activities['max_speed_ms'].append(activity.max_speed.get_num())
            # Time
            self.activities['start_date_utc'].append(getattr(activity, 'start_date', None))
            self.activities['start_date_local'].append(start_datetime)
            self.activities['timezone'].append(getattr(activity.timezone, 'zone', None))
            self.activities['moving_time_sec'].append(moving_time_sec)
            self.activities['elapsed_time_sec'].append(elapsed_time)
            self.activities['end_date_utc'].append(activity.start_date + timedelta(seconds=elapsed_time))
            self.activities['end_date_local'].append(start_datetime + timedelta(seconds=elapsed_time))
            # Location
            self.activities['start_latitude'].append(getattr(activity, 'start_latitude', None))
            self.activities['start_longitude'].append(getattr(activity, 'start_longitude', None))
            self.activities['end_latitude'].append(getattr(activity.end_latlng, 'lat', None))
            self.activities['end_longitude'].append(getattr(activity.end_latlng, 'lon', None))
            # Records
            self.activities['pr_count'].append(activity.pr_count)
            self.activities['best_efforts'].append(activity.best_efforts)
            # Health
            self.activities['has_heartrate'].append(activity.has_heartrate)
            self.activities['average_heartrate'].append(activity.average_heartrate)
            self.activities['max_heartrate'].append(activity.max_heartrate)
            # Elevation
            self.activities['elev_high_m'].append(activity.elev_high)
            self.activities['elev_low_m'].append(activity.elev_low)
            self.activities['total_elevation_gain_m'].append(activity.total_elevation_gain.get_num())
            # Cycling specific
            self.activities['average_watts'].append(activity.average_watts)
            self.activities['kilojoules'].append(activity.kilojoules)
            # ID references
            self.activities['upload_id'].append(activity.upload_id)
            self.activities['external_id'].append(activity.external_id)
            if len(self.activities['activity_id']) >= activities.per_page:
                break
            # Refresh if necessary
            self.check_expiry()
        logger.info('Finished retrieving main activity data.')
        logger.info(f"{len(self.activities['activity_id'])} records retrieved")

    def additional_info(self):
        """
        This is used to retrieve splits, gear, calories and device name which can only be returned when searching by
        a specific activity ID. This results in far more API calls since you are making one call per activity.
        It also takes longer to retrieve since you have to wait to avoid the rate limit.
        Rate limit = 600 calls per 15 minutes (i.e. 40 calls per minute or 2 calls every 3 seconds)
        :return:
        """
        logger.info('Retrieving additional activity data.')
        for activity in self.activities['activity_id']:
            this_activity = self.client.get_activity(activity)
            self.activities['gear'].append(this_activity.gear.name)
            # self.activities['gear_id'].append(this_activity.gear.id)
            self.activities['calories'].append(this_activity.calories)
            self.activities['device_name'].append(this_activity.device_name)
            self.splits_miles(activity_id=activity, splits=this_activity.splits_standard)
            self.splits_km(activity_id=activity, splits=this_activity.splits_metric)
            time.sleep(2)
            self.check_expiry()
        logger.info('Finished retrieving additional activity data.')

    def splits_miles(self, activity_id: int, splits: list):
        for sp in splits:
            self.splits_miles_data['activity_id'].append(activity_id)
            self.splits_miles_data['split'].append(sp.split)
            self.splits_miles_data['distance'].append(unithelper.miles(sp.distance).get_num())
            self.splits_miles_data['moving_time'].append(sp.moving_time.seconds)
            self.splits_miles_data['elapsed_time'].append(sp.elapsed_time.seconds)
            self.splits_miles_data['average_speed_ms'].append(sp.average_speed.get_num())
            self.splits_miles_data['heartrate'].append(sp.average_heartrate)
            self.splits_miles_data['elevation_difference'].append(sp.elevation_difference.get_num())

    def splits_km(self, activity_id: int, splits: list):
        for sp in splits:
            self.splits_km_data['activity_id'].append(activity_id)
            self.splits_km_data['split'].append(sp.split)
            self.splits_km_data['distance'].append(sp.distance.get_num())
            self.splits_km_data['moving_time'].append(sp.moving_time.seconds)
            self.splits_km_data['elapsed_time'].append(sp.elapsed_time.seconds)
            self.splits_km_data['average_speed_ms'].append(sp.average_speed.get_num())
            self.splits_km_data['heartrate'].append(sp.average_heartrate)
            self.splits_km_data['elevation_difference'].append(sp.elevation_difference.get_num())

    def save_activities(self, backup_csv: str = None):
        """
        Creates a data frame from the activities_dict dictionary.
        :return:
        """
        df = pd.DataFrame(self.activities)
        for col in ['start_date_local', 'start_date_utc', 'end_date_local', 'end_date_utc']:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        df['hour_of_day'] = df['start_date_local'].dt.hour
        df['day_of_week'] = df['start_date_local'].dt.day_name()
        df['year_'] = df['start_date_local'].dt.year
        df.to_sql('activities', self.conn, if_exists='append', index=False)
        if backup_csv:
            df.to_csv(backup_csv, index=False, encoding='utf-8-sig')

        df_l = df.copy()[['start_latitude', 'start_longitude']]
        df_l.drop_duplicates(inplace=True)
        df_l.dropna(inplace=True)
        locations = list(zip(df_l['start_latitude'], df_l['start_longitude']))
        new_locations = [ll for ll in locations if ll not in self.existing_locations]
        for ll in new_locations:
            self.add_location(lat=ll[0], lon=ll[1])

        df_gear = df[['gear_id', 'gear']].drop_duplicates().dropna()
        df_gear = df_gear.loc[~df_gear['gear_id'].isin(self.existing_gear)]
        if len(df_gear) > 0:
            df_gear.to_sql('gear', self.conn, if_exists='append', index=False)

    def add_location(self, lat: float, lon: float):
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

    def save_splits(self):
        df_mi = pd.DataFrame(self.splits_miles_data)
        df_mi['heartrate'] = df_mi['heartrate'].round(2)
        df_mi.to_sql('splits_miles', self.conn, if_exists='append', index=False)

        df_km = pd.DataFrame(self.splits_km_data)
        df_km['heartrate'] = df_km['heartrate'].round(2)
        df_km.to_sql('splits_km', self.conn, if_exists='append', index=False)

    def close(self):
        self.conn.close()


def main():
    logger.info('-' * 100)
    backup_file = os.path.join('Results', f"Strava Data {datetime.now().strftime('%Y-%m-%d %H%M')}.csv")
    me = Athlete(first_name='Lance')
    activities = Activities(getattr(me, 'ath_info'))
    try:
        activities.get_activities()
        activities.additional_info()
        activities.save_activities(backup_csv=backup_file)
        activities.save_splits()
        print('Complete')
    except Exception as e:
        logger.error(e, exc_info=sys.exc_info())
        print('Error')
    finally:
        activities.close()


if __name__ == '__main__':
    main()
