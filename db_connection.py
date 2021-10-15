import configparser
from sqlalchemy import create_engine, sql
from sqlalchemy import types as sql_types
# import psycopg2

config = configparser.ConfigParser()
config.read('config_strava_db.ini')
conn_params = {param[0]: param[1] for param in config.items('strava')}
#  conn = psycopg2.connect(**conn_params)


def connect():
    un = config.get('strava', 'user')
    pw = config.get('strava', 'password')
    host = config.get('strava', 'host')
    db = config.get('strava', 'database')
    engine = create_engine(f"postgresql+psycopg2://{un}:{pw}@{host}:5432/{db}")
    return engine.connect()
