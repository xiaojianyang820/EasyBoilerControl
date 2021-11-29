import pymysql
import datetime
import time


class MySQLConnector(object):
    def __init__(self, host: str, port: int, user: str, password: str, db: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db

    def check_data(self, SQL_TEXT: str):
        conn = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                               db=self.db)
        cursor = conn.cursor()
        cursor.execute(SQL_TEXT)
        temp_data = cursor.fetchall()
        cursor.close()
        conn.close()
        return temp_data


def _check_cur_time() -> datetime.datetime:
    cur_time_ = time.localtime()
    cur_year = cur_time_.tm_year
    cur_month = cur_time_.tm_mon
    cur_day = cur_time_.tm_mday
    cur_hour = cur_time_.tm_hour
    cur_minute = cur_time_.tm_min
    cur_second = cur_time_.tm_sec

    return datetime.datetime(cur_year, cur_month, cur_day, cur_hour, cur_minute, cur_minute, cur_second)

class Logger(object):
    def __init__(self, info_path, error_path):
        self.info_path = info_path
        self.error_path = error_path

    def to_info(self, info):
        print(info)
        cur_time = _check_cur_time()
        cur_time = datetime.datetime.strftime(cur_time, '%Y-%m-%d %H:%M:%S')
        s = '[%s]: %s \n' % (cur_time, info)
        with open(self.info_path, 'a') as f:
            f.write(s)

    def to_error(self, error):
        print(error)
        cur_time = _check_cur_time()
        cur_time = datetime.datetime.strftime(cur_time, '%Y-%m-%d %H:%M:%S')
        s = '[%s]: %s \n' % (cur_time, error)
        with open(self.error_path, 'a') as f:
            f.write(s)


def parse_config_file(config_file_path: str):
    with open(config_file_path, 'r', encoding='utf-8') as f:
        text = f.readlines()

    config_map = {}
    for line in text:
        if '#' in line[:3] or line.strip() == '':
            continue
        else:
            key, value = line.split('=')
            key = key.strip()
            value = value.strip()
            config_map[key] = value

    return config_map