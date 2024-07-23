import snowflake.connector
import json
import random
import configparser
import time
import logging as log
from datetime import datetime

config = configparser.ConfigParser()
config.read('config.ini')

log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

