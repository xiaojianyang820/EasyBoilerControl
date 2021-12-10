from boilers.heatloadsuppling import StandardBoiler
from tools.tools import MySQLConnector, _check_cur_time
import datetime
import time
import numpy as np
import requests
import json


class Boiler_YSZ(StandardBoiler):

    def extra_control(self) -> float:
        # 首先获取此时的气象信息
        cur_time = _check_cur_time()
        start_time = cur_time - datetime.timedelta(0, 60*10)
        cur_time_str = datetime.datetime.strftime(cur_time, '%Y-%m-%d %H:%M:%S')
        start_time_str = datetime.datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S')
        temp, hr, lux, wind_speed = self._read_weather(start_time_str, cur_time_str)
        # 获取大棚内的温度
        temp_DP = self._get_DP_temp(cur_time_str)
        # 获取目前大棚动态阀的开度
        dtf_kd = self._check_DPDTF(start_time_str, cur_time_str)
        self.logger.to_info('当前的气象状况为：温度：%.1f  光照：%d  湿度：%.1f  风速：%d' % (temp, lux, hr, wind_speed))
        self.logger.to_info('当前大棚内的温度为：%.1f' % temp_DP)
        self.logger.to_info('当前大棚的动态阀开度为：%d' % dtf_kd)

        # 如果大棚出现严重的低温情况，无论气象和时刻，一律将阀门开大30%
        if temp_DP < int(self.config_map['DP_TEMP_MIN']):
            add_fm_kd = 5 if dtf_kd > 35 else 20
        else:
            if cur_time.hour in [9, 10, 11, 12, 13, 14, 15, 16]:
                if lux < 2000 and temp < -5 and temp_DP < 11:
                    add_fm_kd = 10
                elif lux < 10000 and temp < -7 and temp_DP < 10.5:
                    add_fm_kd = 10
                else:
                    add_fm_kd = -dtf_kd
            elif cur_time.hour in [17, 18, 19]:
                if temp_DP < 12:
                    add_fm_kd = 5
                elif temp_DP < 11:
                    add_fm_kd = 10
                elif temp_DP < 10:
                    add_fm_kd = 15
                elif temp_DP > 13:
                    add_fm_kd = -10
                else:
                    add_fm_kd = 0 if dtf_kd > 10 else 10
            else:
                if temp_DP < 8.8:
                    add_fm_kd = 0 if dtf_kd > 35 else 10
                elif temp_DP < 9.5:
                    add_fm_kd = 0 if dtf_kd > 25 else 10
                elif temp_DP > 11.8:
                    add_fm_kd = -20
                elif temp_DP > 10.0:
                    add_fm_kd = -10
                else:
                    add_fm_kd = -5
        # 设定上一轮次的大棚内温度
        if not hasattr(self, '_last_DP_temp'):
            self._last_DP_temp = 12
        if temp_DP > self._last_DP_temp + 0.5:
            add_fm_kd = -35
        # 如果大棚温度开始明显上升，那么就停止增开阀门
        elif temp_DP > self._last_DP_temp+0.1:
            add_fm_kd = min(-15, add_fm_kd)
        self._last_DP_temp = temp_DP
        new_dtf_kd = max(0, dtf_kd + add_fm_kd)
        new_dtf_kd = min(new_dtf_kd, 95)
        # 如果当前为夜间，且大棚的温度低于9.5摄氏度，那就仍保留10%的开度
        if cur_time.hour in [0, 1, 2, 3, 4, 5] and temp_DP < 9.5:
            new_dtf_kd = max(10, new_dtf_kd)
        dtf_kd_2_ratio_map = {0: 0, 5: 0.02, 10: 0.07, 15: 0.15, 20: 0.25, 25: 0.35, 30: 0.45,
                              35: 0.6, 40: 0.75, 45: 0.85, 50: 0.90, 55: 0.93, 60: 0.96, 65: 0.99,
                              70: 1, 75: 1, 80: 1, 85: 1, 90: 1, 95: 1, 100: 1}
        if new_dtf_kd != dtf_kd:
            self._control_DPDTF(new_dtf_kd)
        # 计算该控制周期的总供热量
        extra_q = dtf_kd_2_ratio_map[np.round(new_dtf_kd/5) * 5] * 140 * 12000 * 60 \
                  * int(self.config_map['Control_Interval'])
        self.logger.to_info('本控制周期大棚需要的热量为：%.2f兆焦' % (extra_q/1000000))
        return extra_q / 1000000

    def dispatch_control(self, total_q):
        self._dispatch_control_1(total_q)

    def _dispatch_control_1(self, total_q) -> None:
        """
        该指令转换方法适用于只有一台锅炉的情况

        :param total_q: float,
            本控制周期需要提供的总热量
        :return: None
        """
        self.logger.to_info('本控制周期需要的总体供热量为%.2f兆焦' % total_q)
        MAX_Q = 8200 * (int(self.config_map['Control_Interval']) / 60)
        MIN_Q = 2000 * (int(self.config_map['Control_Interval']) / 60)
        boiler_id = int(self.config_map['Cur_Boiler_ID'])
        boiler_set_value_max = 95
        boiler_set_value_min = 10
        # 锅炉设定的跳跃速度
        boiler_set_speed = 20
        while True:
            try:
                boiler_start_or_stop, boiler_state = self._check_boiler_state(boiler_id)
                break
            except:
                self.logger.to_error('查询锅炉运行状态出现异常，等待2分钟后重试')
                time.sleep(120)
        self.logger.to_info('当前的锅炉状态为：%.1f' % boiler_state)
        if total_q < MIN_Q/1.3:
            # 关闭锅炉
            if boiler_start_or_stop:
                self._start_or_stop_boiler(0, boiler_id)
                time.sleep(60)
            else:
                pass
        elif total_q <= MIN_Q:
            # 锅炉负荷设定为10%
            if not boiler_start_or_stop:
                self._start_or_stop_boiler(1, boiler_id)
                time.sleep(120)
            self._set_boiler(set_value=np.clip(boiler_set_value_min, a_min=boiler_state-boiler_set_speed,
                                               a_max=boiler_state+boiler_set_speed), boiler_id=boiler_id)
        elif total_q > MAX_Q:
            # 锅炉负荷设定为100%
            if not boiler_start_or_stop:
                self._start_or_stop_boiler(1, boiler_id)
                time.sleep(120)
            self._set_boiler(set_value=np.clip(boiler_set_value_max, a_min=boiler_state-boiler_set_speed,
                                               a_max=boiler_state+boiler_set_speed), boiler_id=boiler_id)
        else:
            set_value = ((total_q-MIN_Q) / (MAX_Q-MIN_Q)) * 90 + 10
            set_value = np.clip(set_value, boiler_set_value_min, boiler_set_value_max)
            # 锅炉负荷设定为set_value
            if not boiler_start_or_stop:
                self._start_or_stop_boiler(1, boiler_id)
                time.sleep(120)
            self._set_boiler(set_value=np.clip(set_value, a_min=boiler_state-boiler_set_speed,
                                               a_max=boiler_state+boiler_set_speed), boiler_id=boiler_id)

    def _check_boiler_state(self, boilder_id):
        cur_time = _check_cur_time()
        start_time = cur_time - datetime.timedelta(0, 60)
        cur_time_str = datetime.datetime.strftime(cur_time, '%Y-%m-%d %H:%M:%S')
        start_time_str = datetime.datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S')
        SQL_Templet = 'SELECT avg(GL%d_O_PR) FROM bd_ysz_mainstation_2021 WHERE create_time > "%s" AND create_time < "%s" ORDER BY create_time DESC LIMIT 5'
        SQL_TEXT = SQL_Templet % (boilder_id, start_time_str, cur_time_str)
        HOST = self.config_map['Read_Database_Host']
        PORT = int(self.config_map['Read_Database_Port'])
        USER = self.config_map['Read_Database_User']
        PASSWORD = self.config_map['Read_Database_Password']
        DB = self.config_map['Read_Database_Db']
        data_checker = MySQLConnector(host=HOST, port=PORT, user=USER, password=PASSWORD, db=DB)
        temp_data = data_checker.check_data(SQL_TEXT)
        boiler_state = temp_data[0][0]
        if boiler_state > 0:
            boiler_start_or_stop = 1
        else:
            boiler_start_or_stop = 0
        return boiler_start_or_stop, float(boiler_state)

    def _read_weather(self, start_time: str, end_time: str) -> tuple:
        SQL_Templet = 'SELECT avg(temp), avg(hr), avg(lux), avg(wind_speed) FROM %s WHERE create_time > "%s" and create_time <= "%s" and project_id = %s'
        SQL_TEXT = SQL_Templet % (self.config_map['WeatherStation_Table'], start_time, end_time, self.config_map['WeatherStation_ID'])
        HOST = self.config_map['Read_Database_Host']
        PORT = int(self.config_map['Read_Database_Port'])
        USER = self.config_map['Read_Database_User']
        PASSWORD = self.config_map['Read_Database_Password']
        DB = self.config_map['Read_Database_Db']
        data_checker = MySQLConnector(HOST, PORT, USER, PASSWORD, DB)
        temp_data = data_checker.check_data(SQL_TEXT)
        temp, hr, lux, wind_speed = temp_data[0]
        return float(temp), float(hr), float(lux), float(wind_speed)

    def _control_DPDTF(self, set_value):
        control_JSON = [
            {
                "id": None,
                "itemCode": 1,
                "itemValue": "%d" % set_value,
                "fromPlatform": "ML",
                "downType": "DDF_KD_SET",
                "expiryTime": 1572612098013,
                "executionTime": 1572612098013,
                "desc1": "锅炉房大棚电动阀开度设定",
                "isCtrl": 1
            }
        ]
        self.logger.to_info('将大棚控制阀门调节为：%d' % int(set_value))
        self._post_control(control_JSON)
        time.sleep(30)

    def _check_DPDTF(self, start_time, cur_time):
        HOST = self.config_map['Read_Database_Host']
        PORT = int(self.config_map['Read_Database_Port'])
        USER = self.config_map['Read_Database_User']
        PASSWORD = self.config_map['Read_Database_Password']
        DB = self.config_map['Read_Database_Db']
        data_checker = MySQLConnector(HOST, PORT, USER, PASSWORD, DB)
        SQL_Templet = 'SELECT avg(DP_DDF1) FROM bd_ysz_mainstation_2021 WHERE create_time > "%s" and create_time <"%s"'
        SQL_TEXT = SQL_Templet % (start_time, cur_time)
        temp_data = data_checker.check_data(SQL_TEXT)
        return float(temp_data[0][0])

    def _get_DP_temp(self, cur_time: str) -> float:
        """
        该函数可以返回在当前时刻的一组目标传感器的平均数值

        :param cur_time: str,
            查询温度的目标时间
        :return:
        """

        ACCESS_TOKEN = self.config_map['ACCESS_TOKEN']
        MODEL_ID = self.config_map['MODEL_ID']
        base_url = 'http://47.94.209.71/600ly-ctrl/api/v1/cmdCenter/ctrlTarget/getAll.json?model_id=%s&access_token=%s'
        target_url = base_url % (MODEL_ID, ACCESS_TOKEN)
        check_json = {'collectionTime': cur_time}
        res = requests.post(target_url, json=check_json)
        res_json = json.loads(res.content.decode('utf-8'))
        indoor_temp_list = []
        for area in res_json['data']['targetList']:
            if area['areaName'] in ['东大棚', '西大棚']:
                indoor_temp = area['target']['indoorAvgTemp']
                indoor_temp_list.append(indoor_temp)
        if len(indoor_temp_list) > 0:
            return np.mean(indoor_temp_list)
        else:
            self.logger.to_error('获取大棚内温度失败，返回一个随机温度')
            return 9.0 + np.random.rand()


if __name__ == '__main__':
    boiler = Boiler_YSZ('boilers/configs/boiler_config_200002.config')
    boiler.main()
