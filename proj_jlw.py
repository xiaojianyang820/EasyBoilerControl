from boilers.heatloadsuppling import StandardBoiler
from tools.tools import MySQLConnector, _check_cur_time
import datetime
import time
import numpy as np


class Boiler_JLW(StandardBoiler):

    def extra_control(self) -> float:
        return 0

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
        MAX_Q = 34400 * (int(self.config_map['Control_Interval']) / 60)
        MIN_Q = 8000 * (int(self.config_map['Control_Interval']) / 60)
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
        SQL_Templet = 'SELECT avg(GL%d_O_PR) FROM bd_jlw_mainstation_2021 WHERE create_time > "%s" AND create_time < "%s" ORDER BY create_time DESC LIMIT 5'
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


if __name__ == '__main__':
    boiler = Boiler_JLW('boilers/configs/boiler_config_200003.config')
    boiler.main()
