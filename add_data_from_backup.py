import os
import json
import pandas as pd
import numpy as np
from db_connection import connect

conn = connect()

relevant_fields = ['id', 'description', 'perceived_exertion', 'device_name', 'calories']
df_details = pd.DataFrame()
df_km = pd.DataFrame()
df_miles = pd.DataFrame()

folder = os.path.join('Activities', 'individual_activities')
for file in os.listdir(folder):
    activity_id = int(os.path.splitext(file)[0])
    with open(os.path.join(os.getcwd(), folder, file), 'r') as f:
        activity_data = json.load(f)
    relevant_data = {x: activity_data[x] for x in activity_data.keys() if x in relevant_fields}
    df_activity_details = pd.DataFrame(relevant_data, index=[0])
    df_activity_details.rename(columns={'id': 'activity_id', 'description': 'activity_description'}, inplace=True)
    df_details = pd.concat([df_details, df_activity_details]).reset_index(drop=True)

    splits_km = pd.DataFrame(activity_data['splits_metric'])
    splits_km['activity_id'] = activity_id
    df_km = pd.concat([df_km, splits_km]).reset_index(drop=True)

    splits_miles = pd.DataFrame(activity_data['splits_standard'])
    splits_miles['activity_id'] = activity_id
    df_miles = pd.concat([df_miles, splits_miles]).reset_index(drop=True)


df = pd.DataFrame()
folder = os.path.join('Activities', 'activity_lists')
for file in os.listdir(folder):
    with open(os.path.join(os.getcwd(), folder, file), 'r') as f:
        json_data = json.load(f)
    df_new = pd.json_normalize(json_data)
    df = pd.concat([df, df_new]).reset_index(drop=True)

df['distance_miles'] = df['distance'] * 0.0006213712
df['distance_miles'] = df['distance_miles'].round(2)
df['distance_km'] = df['distance'] / 1000
df['distance_km'] = df['distance_km'].round(2)
df['min_per_mile'] = (df['moving_time'] / 60) / df['distance_miles']
df['min_per_mile'] = df['min_per_mile'].round(4)
df['end_latitude'] = df['end_latlng'].apply(lambda x: x[0] if len(x) == 2 else np.nan)
df['end_longitude'] = df['end_latlng'].apply(lambda x: x[1] if len(x) == 2 else np.nan)
df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
df['activity_date'] = df['start_date'].dt.date
df['start_date_local'] = pd.to_datetime(df['start_date_local'], errors='coerce')
df['end_date'] = df['start_date'] + pd.to_timedelta(df['elapsed_time'], unit='seconds')
df['end_date_local'] = df['start_date_local'] + pd.to_timedelta(df['elapsed_time'], unit='seconds')
df['hour_of_day'] = df['start_date_local'].dt.hour
df['day_of_week'] = df['start_date_local'].dt.day_name()
df['year_'] = df['start_date_local'].dt.year
df.rename(columns={
    'id': 'activity_id',
    'athlete.id': 'athlete_id',
    'type': 'activity_type',
    'name': 'activity_name',
    'distance': 'distance_meters',
    'average_speed': 'average_speed_ms',
    'max_speed': 'max_speed_ms',
    'average_heartrate': 'heartrate'
}, inplace=True)
df = df.merge(df_details, how='left', on='activity_id')
df['activity_description'].replace('', np.nan, inplace=True)
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
df = df[cols]
df.to_sql('activities', conn, if_exists='append', index=False)

for df in [df_miles, df_km]:
    df.rename(columns={'average_speed': 'average_speed_ms', 'average_heartrate': 'heartrate'}, inplace=True)
    df['heartrate'] = df['heartrate'].round(2)
df_miles.to_sql('splits_miles', conn, if_exists='append', index=False)
df_km.to_sql('splits_km', conn, if_exists='append', index=False)
