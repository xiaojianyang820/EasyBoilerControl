from boilers.heatloadsuppling import StandardBoiler
from tools.tools import MySQLConnector, _check_cur_time
import datetime
import time
import numpy as np


class Boiler_XHLY(StandardBoiler):

    def extra_control(self) -> float:
        return 0

    def dispatch_control(self, total_q):
        self._dispatch_control_3(total_q)

    def _dispatch_control_3(self, total_q) -> None:
        """
        该指令转换方法适用于只有一台锅炉的情况

        :param total_q: float,
            本控制周期需要提供的总热量
        :return: None
        """
        self.logger.to_info('本控制周期需要的总体供热量为%.2f兆焦' % total_q)
        adj_coef = int(self.config_map['Control_Interval']) / 60
        # 锅炉最高设定温度
        MAX_GS_T = 85
        # 锅炉最低设定温度
        MIN_GS_T = 36
        # 查询当前的回水温度
        while True:
            try:
                cur_zg_hs_t = self._check_hs_t()
                temp_v = cur_zg_hs_t + 1
                break
            except:
                self.logger.to_error('查询回水温度出现异常，60秒后重试')
                time.sleep(60)
        # 查询当前的锅炉状态
        cur_gl_1_on, _ = self._check_boiler_state(1)
        cur_gl_2_on, _ = self._check_boiler_state(2)
        cur_gl_3_on, _ = self._check_boiler_state(3)
        # 查询当前的一次网流量
        fir_net_flew = float(self.config_map['Fir_Net_Flew'])
        # 目前需要的一次网温差为：
        fir_net_temp_up = (total_q * 1000000) / (fir_net_flew * adj_coef * 1000 * 4200)
        self.logger.to_info('目前一次网需要升温 %.1f 摄氏度' % fir_net_temp_up)
        if fir_net_temp_up < 1.3 and np.random.rand() > 0.4:
            # 这种情况下关闭全部锅炉
            self._start_or_stop_boiler(0, 3)
            self._start_or_stop_boiler(0, 1)
            self._start_or_stop_boiler(0, 2)
        elif fir_net_temp_up < 2.25:
            # 在这种情况下只运行一台锅炉
            self._start_or_stop_boiler(1, 3)
            self._start_or_stop_boiler(0, 1)
            self._start_or_stop_boiler(0, 2)
            time.sleep(60)
            target_gs_temp = cur_zg_hs_t + fir_net_temp_up * 3
            target_gs_temp = min(MAX_GS_T, max(MIN_GS_T, target_gs_temp))
            self._set_gl_gs_temp(target_gs_temp, 3)
        elif fir_net_temp_up < 4.5:
            # 在这种情况下只运行二台锅炉
            self._start_or_stop_boiler(1, 3)
            self._start_or_stop_boiler(1, 1)
            self._start_or_stop_boiler(0, 2)
            time.sleep(60)
            target_gs_temp = cur_zg_hs_t + fir_net_temp_up * 3/2
            target_gs_temp = min(MAX_GS_T, max(MIN_GS_T, target_gs_temp))
            self._set_gl_gs_temp(target_gs_temp, 3)
            self._set_gl_gs_temp(target_gs_temp, 1)
        else:
            # 如果需要的温度更高，那么就需要开启三台锅炉
            self._start_or_stop_boiler(1, 3)
            self._start_or_stop_boiler(1, 1)
            self._start_or_stop_boiler(1, 2)
            time.sleep(60)
            target_gs_temp = cur_zg_hs_t + fir_net_temp_up * 3 / 3
            target_gs_temp = min(MAX_GS_T, max(MIN_GS_T, target_gs_temp))
            self._set_gl_gs_temp(target_gs_temp, 3)
            self._set_gl_gs_temp(target_gs_temp, 1)
            self._set_gl_gs_temp(target_gs_temp, 2)

    def _check_hs_t(self):
        cur_time = _check_cur_time()
        start_time = cur_time - datetime.timedelta(0, 60)
        cur_time_str = datetime.datetime.strftime(cur_time, '%Y-%m-%d %H:%M:%S')
        start_time_str = datetime.datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S')
        SQL_Templet = 'SELECT avg(GL1_HS_T) FROM bd_zjxhly_mainstation_2021 WHERE create_time > "%s" AND create_time < "%s" ORDER BY create_time DESC LIMIT 5'
        SQL_TEXT = SQL_Templet % (start_time_str, cur_time_str)
        HOST = self.config_map['Read_Database_Host']
        PORT = int(self.config_map['Read_Database_Port'])
        USER = self.config_map['Read_Database_User']
        PASSWORD = self.config_map['Read_Database_Password']
        DB = self.config_map['Read_Database_Db']
        data_checker = MySQLConnector(host=HOST, port=PORT, user=USER, password=PASSWORD, db=DB)
        temp_data = data_checker.check_data(SQL_TEXT)
        zg_hs_t = temp_data[0][0]
        return zg_hs_t

    def _check_boiler_state(self, boilder_id):
        cur_time = _check_cur_time()
        start_time = cur_time - datetime.timedelta(0, 60)
        cur_time_str = datetime.datetime.strftime(cur_time, '%Y-%m-%d %H:%M:%S')
        start_time_str = datetime.datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S')
        SQL_Templet = 'SELECT avg(GL%d_FH) FROM bd_zjxhly_mainstation_2021 WHERE create_time > "%s" AND create_time < "%s" ORDER BY create_time DESC LIMIT 5'
        SQL_TEXT = SQL_Templet % (boilder_id, start_time_str, cur_time_str)
        HOST = self.config_map['Read_Database_Host']
        PORT = int(self.config_map['Read_Database_Port'])
        USER = self.config_map['Read_Database_User']
        PASSWORD = self.config_map['Read_Database_Password']
        DB = self.config_map['Read_Database_Db']
        data_checker = MySQLConnector(host=HOST, port=PORT, user=USER, password=PASSWORD, db=DB)
        temp_data = data_checker.check_data(SQL_TEXT)
        boiler_state = temp_data[0][0]
        try:
            if boiler_state > 0:
                boiler_start_or_stop = 1
            else:
                boiler_start_or_stop = 0
        except:
            self.logger.to_error('查询锅炉状态出现异常，设置锅炉为关机状态')
            boiler_start_or_stop = 0
            boiler_state = 0
        return boiler_start_or_stop, float(boiler_state)

    def _set_gl_gs_temp(self, set_value, boiler_id):
        control_JSON = [
            {
                'id': None,
                'itemCode': boiler_id,
                'itemValue': set_value,
                'fromPlatform': 'ML',
                'downType': 'GL_GS_T_SET',
                'expiryTime': 1572612098013,
                'executionTime': 1572612098013,
                'desc1': '%d#锅炉供温设定' % boiler_id,
                'isCtrl': 1
            }
        ]
        info = '设定%d#锅炉的温度为：%.1f' % (boiler_id, set_value)
        self.logger.to_info(info)
        self._post_control(control_JSON)


if __name__ == '__main__':
    boiler = Boiler_XHLY('boilers/configs/boiler_config_200007.config')
    boiler.main()
