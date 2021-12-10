from boilers.heatloadsuppling import StandardBoiler
from tools.tools import MySQLConnector, _check_cur_time
import datetime
import time
import numpy as np


class Boiler_HYXS(StandardBoiler):

    def extra_control(self) -> float:
        return 0

    def dispatch_control(self, total_q):
        self._dispatch_control_2(total_q)

    def _dispatch_control_2(self, total_q) -> None:
        """
        该指令转换方法适用于两台锅炉的情况

        :param total_q: float,
            本控制周期需要提供的总热量
        :return: None
        """
        self.logger.to_info('本控制周期需要的总体供热量为%.2f兆焦' % total_q)
        adj_coef = int(self.config_map['Control_Interval']) / 60
        zfh_2T_u = 4100 * adj_coef
        zfh_3T_u = 6200 * adj_coef
        zfh_2T_l = 2050 * adj_coef
        zfh_3T_l = 2350 * adj_coef
        fh_set_2T_u = 95
        fh_set_2T_l = 10
        fh_set_3T_u = 74
        fh_set_3T_l = 15
        max_adj_interval = 25

        state_2T_on, state_2T_value = self._check_boiler_state(boiler_id=2)
        state_3T_on, state_3T_value = self._check_boiler_state(boiler_id=1)

        # 如果实际热需求低于2T锅炉的最低负荷，就关闭两台锅炉
        if total_q <= zfh_2T_l:
            self._set_boiler(15, 1)
            if state_2T_on:
                self._start_or_stop_boiler(0, 2)
                time.sleep(60)
        # 如果3T锅炉15%的负荷够用，那么就设定3T锅炉为15%，关闭2T锅炉
        elif total_q < zfh_3T_l:
            if not state_3T_on:
                self._start_or_stop_boiler(1, 1)
                time.sleep(60)
            self._set_boiler(15, 1)
            if state_2T_on:
                self._start_or_stop_boiler(0, 2)
                time.sleep(60)
        # 如果实际热负荷低于2T锅炉的110%的总负荷，那么就只运行2T锅炉
        elif total_q < zfh_2T_u * 1.1:
            if not state_3T_on:
                self._start_or_stop_boiler(1, 1)
                time.sleep(60)
                self._set_boiler(15, 1)
            else:
                self._set_boiler(15, 1)
            if not state_2T_on:
                self._start_or_stop_boiler(1, 2)
                time.sleep(60)
            set_value_2T = max(0, (total_q-zfh_3T_l - zfh_2T_l))/(zfh_2T_u - zfh_2T_l) * 90 + fh_set_2T_l
            set_value_2T = min(fh_set_2T_u, set_value_2T)
            set_value_2T = max(set_value_2T, state_2T_value-max_adj_interval)
            set_value_2T = min(set_value_2T, state_2T_value+max_adj_interval)
            self._set_boiler(set_value_2T, boiler_id=2)
        # 如果实际热负荷超过2T锅炉的110%的总负荷，又低于2T锅炉的50%负荷和3T锅炉50%负荷之和，
        # 就先让2T锅炉运行50%的负荷，剩余的由3T锅炉来完成
        elif total_q < (zfh_2T_u+zfh_2T_l)*0.5 + (zfh_3T_u+zfh_3T_l)*0.5:
            if not state_2T_on:
                self._start_or_stop_boiler(1, 2)
                time.sleep(60)
            if not state_3T_on:
                self._start_or_stop_boiler(1, 1)
                time.sleep(60)
            base_q = (zfh_2T_u+zfh_2T_l)*0.5
            remain_q = total_q - base_q
            set_value_2T = 55
            set_value_3T = max(0, (remain_q - zfh_3T_l))/(zfh_3T_u - zfh_3T_l) * 85 + fh_set_3T_l
            set_value_3T = min(set_value_3T, fh_set_3T_u)
            set_value_2T = max(set_value_2T, state_2T_value - max_adj_interval)
            set_value_2T = min(set_value_2T, state_2T_value + max_adj_interval)
            self._set_boiler(set_value_2T, boiler_id=2)
            assert set_value_3T < 75
            set_value_3T = max(set_value_3T, state_3T_value - max_adj_interval)
            set_value_3T = min(set_value_3T, state_3T_value + max_adj_interval)
            self._set_boiler(set_value_3T, boiler_id=1)
        # 否则的话，2T锅炉满负荷运行，剩余的由3T锅炉来完成
        else:
            if not state_2T_on:
                self._start_or_stop_boiler(1, 2)
                time.sleep(60)
            if not state_3T_on:
                self._start_or_stop_boiler(1, 1)
                time.sleep(60)
            base_q = zfh_2T_u
            remain_q = total_q - base_q
            set_value_2T = fh_set_2T_u
            set_value_3T = (remain_q - zfh_3T_l) / (zfh_3T_u - zfh_3T_l) * 85 + fh_set_3T_l
            set_value_3T = min(set_value_3T, fh_set_3T_u)
            set_value_2T = max(set_value_2T, state_2T_value - max_adj_interval)
            set_value_2T = min(set_value_2T, state_2T_value + max_adj_interval)
            self._set_boiler(set_value_2T, boiler_id=2)
            assert set_value_3T < 75
            set_value_3T = max(set_value_3T, state_3T_value - max_adj_interval)
            set_value_3T = min(set_value_3T, state_3T_value + max_adj_interval)
            self._set_boiler(set_value_3T, boiler_id=1)

    def _check_boiler_state(self, boiler_id):
        cur_time = _check_cur_time()
        start_time = cur_time - datetime.timedelta(0, 60*3)
        cur_time_str = datetime.datetime.strftime(cur_time, '%Y-%m-%d %H:%M:%S')
        start_time_str = datetime.datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S')
        SQL_Templet = 'SELECT avg(GL%d_O_PR) FROM bd_hyxs_mainstation_2021 WHERE create_time > "%s" AND create_time < "%s" ORDER BY create_time DESC LIMIT 5'
        SQL_TEXT = SQL_Templet % (boiler_id, start_time_str, cur_time_str)
        HOST = self.config_map['Read_Database_Host']
        PORT = int(self.config_map['Read_Database_Port'])
        USER = self.config_map['Read_Database_User']
        PASSWORD = self.config_map['Read_Database_Password']
        DB = self.config_map['Read_Database_Db']
        data_checker = MySQLConnector(host=HOST, port=PORT, user=USER, password=PASSWORD, db=DB)
        for _ in range(4):
            try:
                temp_data = data_checker.check_data(SQL_TEXT)
                boiler_state = temp_data[0][0]
                temp = boiler_state >= 0
                break
            except Exception as e:
                self.logger.to_error('无法查询锅炉当前的状态，等待60秒之后重试！')
                time.sleep(60)
        else:
            boiler_state = 20

        if boiler_state > 0:
            boiler_start_or_stop = 1
        else:
            boiler_start_or_stop = 0
        return boiler_start_or_stop, float(boiler_state)


if __name__ == '__main__':
    boiler = Boiler_HYXS('boilers/configs/boiler_config_200004.config')
    boiler.main()
