import logging
import os

if not os.path.exists('Logs'):
    os.mkdir('Logs')
handler = logging.FileHandler(os.path.join(os.getcwd(), 'Logs', 'Strava Data Logs.txt'), mode='a+',
                              encoding='UTF-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)
