from boilers.heatloadsuppling import StandardBoiler
from tools.tools import MySQLConnector, _check_cur_time
import datetime
import time
import numpy as np


class Boiler_TLCF(StandardBoiler):

    def extra_control(self) -> float:
        return 0

    def dispatch_control(self, total_q):
        self._dispatch_control_4(total_q)

    def _dispatch_control_4(self, total_q) -> None:
        """
        该指令转换方法适用于只有一台锅炉的情况

        :param total_q: float,
            本控制周期需要提供的总热量
        :return: None
        """
        self.logger.to_info('本控制周期需要的总体供热量为%.2f兆焦' % total_q)
        adj_coef = int(self.config_map['Control_Interval']) / 60
        MAX_Q = 4900 * adj_coef
        MIN_Q = 1430 * adj_coef
        gl_num = total_q // MAX_Q + 1
        # 如果总热量需求很低，那么就关闭全部锅炉
        if total_q < MIN_Q:
            self._start_or_stop_boiler(0, 2)
            self._start_or_stop_boiler(0, 4)
            self._start_or_stop_boiler(0, 6)
            self._start_or_stop_boiler(0, 8)
        else:
            if gl_num == 1:
                self._start_or_stop_boiler(1, 2)
                self._start_or_stop_boiler(0, 4)
                self._start_or_stop_boiler(0, 6)
                self._start_or_stop_boiler(0, 8)
            elif gl_num == 2:
                self._start_or_stop_boiler(1, 2)
                self._start_or_stop_boiler(1, 4)
                self._start_or_stop_boiler(0, 6)
                self._start_or_stop_boiler(0, 8)
            elif gl_num == 3:
                self._start_or_stop_boiler(1, 2)
                self._start_or_stop_boiler(1, 4)
                self._start_or_stop_boiler(1, 6)
                self._start_or_stop_boiler(0, 8)
            else:
                self._start_or_stop_boiler(1, 2)
                self._start_or_stop_boiler(1, 4)
                self._start_or_stop_boiler(1, 6)
                self._start_or_stop_boiler(1, 8)

    def _check_boiler_state(self, boilder_id):
        cur_time = _check_cur_time()
        start_time = cur_time - datetime.timedelta(0, 60)
        cur_time_str = datetime.datetime.strftime(cur_time, '%Y-%m-%d %H:%M:%S')
        start_time_str = datetime.datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S')
        SQL_Templet = 'SELECT avg(GR_%d_GL_ZO) FROM bd_tlcf_mainstation_2021 WHERE create_time > "%s" AND create_time < "%s" ORDER BY create_time DESC LIMIT 5'
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
    boiler = Boiler_TLCF('boilers/configs/boiler_config_200005.config')
    boiler.main()
