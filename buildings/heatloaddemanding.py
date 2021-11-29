from abc import ABCMeta, abstractmethod
from tools.tools import MySQLConnector, parse_config_file, Logger
import os
import time
import datetime
import numpy as np
import pandas as pd
import requests
import json


class AbsBuilding(object):
    __meta_class__ = ABCMeta
    """
    该类是一个抽象类，规定了建筑物群需要实现的标准接口，以及建筑物实时热负荷需求的计算主逻辑
    """
    def __init__(self, config_file_path: str, connector_generator: callable, logger: Logger):
        # 该建筑物群相关参数的配置文件
        self.config_file_path = config_file_path
        # 该建筑物群相关参数的配置字典
        self.config_map = parse_config_file(self.config_file_path)
        # 该项目的ID编号
        self.proj_id = int(self.config_map['Proj_ID'])
        # 调用后返回数据库链接对象conn和cursor
        HOST = self.config_map['Read_Database_Host']
        PORT = int(self.config_map['Read_Database_Port'])
        USER = self.config_map['Read_Database_User']
        PASSWORD = self.config_map['Read_Database_Password']
        DB = self.config_map['Read_Database_Db']
        self.data_checker = connector_generator(HOST, PORT, USER, PASSWORD, DB)
        # 输出日志的对象
        self.logger = logger

    def main(self):
        info = '+++++++++ 建筑群ID: %d ++++++++++++' % self.proj_id
        # 该建筑物群相关参数的配置字典
        self.config_map = parse_config_file(self.config_file_path)
        self.logger.to_info(info)
        # 计算出当前的实时设计热负荷（瓦）
        design_realtime_heatload = self.design_realtime_heatload()
        # 计算出上一个控制周期实际运行热负荷（瓦）
        run_realtime_heatload = self.run_realtime_heatload()
        # 计算出当前的设计误差热负荷（瓦）
        need_add_p_p, need_add_p_i = self.design_error_evaluate()
        design_error = need_add_p_p + need_add_p_i
        info = '当前的实时设计热负荷为：%.2f' % design_realtime_heatload
        self.logger.to_info(info)
        info = '上一个控制周期的实际运行热负荷为：%.2f' % run_realtime_heatload
        self.logger.to_info(info)
        info = '当前的设计热负荷误差为：%.2f' % design_error
        self.logger.to_info(info)
        run_error = design_realtime_heatload - run_realtime_heatload
        info = '当前的运行热负荷误差为：%.2f' % run_error
        self.logger.to_info(info)
        run_error_ = np.clip(run_error, -int(self.config_map['Run_HeatLoad_Upper']),
                             int(self.config_map['Run_HeatLoad_Upper']))
        design_error_ = np.clip(design_error, -int(self.config_map['Design_HeatLoad_Upper']),
                             int(self.config_map['Design_HeatLoad_Upper']))
        cur_heatload = design_realtime_heatload + run_error_ + design_error_
        info = '当前的实际热负荷需求为：%.2f' % cur_heatload
        self.logger.to_info(info)
        cur_p = np.clip(cur_heatload, 0, 65)
        cur_q = cur_p * int(self.config_map['Area']) * 60 * int(self.config_map['Control_Interval'])
        info = '本控制周期需要总热量（兆焦）为：%d' % (cur_q/1000000)
        self.logger.to_info(info)
        return cur_p, cur_q / 1000000

    @abstractmethod
    def design_realtime_heatload(self) -> float:
        """
        该函数需要返回实时设定热负荷

        :return:
        """

    @abstractmethod
    def run_realtime_heatload(self) -> float:
        """
        该函数需要返回上一控制周期的实际运行供热功率

        :return:
        """

    @abstractmethod
    def design_error_evaluate(self) -> float:
        """
        该函数需要返回基于当前室内温度的调整供热功率

        :return:
        """

    @abstractmethod
    def _read_weather(self, start_time: str, end_time: str) -> tuple:
        """
        该方法需要返回该建筑物对应气象站下的室外温度，室外湿度，光照强度，风速

        :param start_time:
        :param end_time:
        :return: tuple,
            (室外温度，室外湿度，光照强度，风速)
        """


class StandardBuilding(AbsBuilding):
    def __init__(self, config_file_path: str, logger: Logger):
        super(StandardBuilding, self).__init__(config_file_path=config_file_path,
                                               connector_generator=MySQLConnector,
                                               logger=logger
                                               )

    def design_realtime_heatload(self) -> float:
        # 获取当前的时间
        cur_clock = self._check_cur_time()
        start_clock = cur_clock - datetime.timedelta(0, 60*10)
        cur_clock_str = datetime.datetime.strftime(cur_clock, '%Y-%m-%d %H:%M:%S')
        start_clock_str = datetime.datetime.strftime(start_clock, '%Y-%m-%d %H:%M:%S')
        # 设置的标准基础热负荷
        stand_base_heatload = float(self.config_map['Standard_Base_HeatLoad'])
        # 获取气象信息
        temp, hr, lux, wind_speed = self._read_weather(start_clock_str, cur_clock_str)
        # 计算基础热负荷-气象调整
        adj_weather_base_heatload = self._adj_heatload_of_weather(stand_base_heatload, temp, lux,
                                                                  wind_speed, cur_clock.hour)
        # 计算基础热负荷-时刻调整
        adj_clock_base_heatload = self._adj_heatload_of_clock(adj_weather_base_heatload, cur_clock.hour)
        # 计算实时热负荷
        realtime_heatload = adj_clock_base_heatload * (18 - temp) / 26
        return realtime_heatload

    def run_realtime_heatload(self) -> float:
        control_interval = int(self.config_map['Control_Interval'])
        # 获取当前的时间
        cur_clock = self._check_cur_time()
        start_clock = cur_clock - datetime.timedelta(0, 60 * control_interval)
        cur_clock_str = datetime.datetime.strftime(cur_clock, '%Y-%m-%d %H:%M:%S')
        start_clock_str = datetime.datetime.strftime(start_clock, '%Y-%m-%d %H:%M:%S')
        # 二次网数据存储表名
        secnet_table = self.config_map['SecNet_Table']
        # 二次网供水温度
        secnet_gst_name = self.config_map['SecNet_GST']
        # 二次网回水温度
        secnet_hst_name = self.config_map['SecNet_HST']
        # 二次网流量
        secnet_flow = float(self.config_map['SecNet_Flow'])

        while True:
            try:
                SQL_Templet = 'SELECT avg(%s), avg(%s) FROM %s WHERE create_time > "%s" and create_time <= "%s"'
                SQL_TEXT = SQL_Templet % (secnet_gst_name, secnet_hst_name, secnet_table, start_clock_str, cur_clock_str)
                temp_data = self.data_checker.check_data(SQL_TEXT)
                secnet_gst, secnet_hst = temp_data[0]
                run_q = (secnet_gst - secnet_hst) * secnet_flow * 1000 * 4200 * (int(self.config_map['Control_Interval'])/60)
                break
            except Exception as e:
                print(e)
                self.logger.to_error('无法正常查询系统的供回水温度，等待20秒后重新查询')
                time.sleep(20)

        run_p = run_q / (int(self.config_map['Area']) * 60 * int(self.config_map['Control_Interval']))
        # 如果二次网供水温度到达上限位置，则降低10%的供热量
        if secnet_gst > float(self.config_map['Sec_Net_Temp_Upper_Bound']):
            self.logger.to_info('当前的二次网温度为%.1f，已经超过上限，故降低百分之20的供热负荷' % secnet_gst)
            run_p_s = run_p * 1.2
            self.logger.to_info('将运行负荷从 %.1f 调整为 %.1f' % (run_p, run_p_s))
            run_p = run_p_s
        return run_p

    def design_error_evaluate(self) -> tuple:
        # 获取当前的时间
        cur_clock = self._check_cur_time()
        cur_hour = cur_clock.hour
        indoor_distribution = pd.read_csv('buildings/tables/indoor_distribution_%d.csv' % self.proj_id,
                                          encoding='gbk')
        mean_add_temp = np.mean(indoor_distribution['相对温度'].values)
        _, adj_temp = indoor_distribution[indoor_distribution['时刻'] == cur_hour].values[0]
        min_indoor_temp = float(self.config_map['Min_Indoor_Temp'])
        # 当前的目标室内温度
        tar_indoor_temp = min_indoor_temp + adj_temp
        # 过去48个小时的室内温度记录
        history_clock_list = []
        for i in range(48):
            history_clock_list.append(cur_clock - datetime.timedelta(0, 60*60*i))
        history_clock_list = [datetime.datetime.strftime(i, '%Y-%m-%d %H:%M:%S')
                              for i in history_clock_list]
        history_indoor_temp_list = [self._read_indoor_temp(i) for i in history_clock_list]
        # 当前的观测室内温度
        cur_indoor_temp = history_indoor_temp_list[0]
        # 累计的温度误差
        accu_indoor_temp_error = np.mean(history_indoor_temp_list) - min_indoor_temp
        self.logger.to_info('当前阶段的即刻室内温度为：%.1f  平均室内温度为：%.1f' % (cur_indoor_temp,
                                                                 np.mean(history_indoor_temp_list)))
        # 计算需要增加的比例量（单位瓦）
        need_add_p_p = (tar_indoor_temp - cur_indoor_temp) * float(self.config_map['PID_P_coef'])
        # 计算需要增加的积分量（单位瓦）
        need_add_p_i = (mean_add_temp - accu_indoor_temp_error) * float(self.config_map['PID_I_coef'])
        self.logger.to_info('需要增加的比例量为：%.2f, 需要增加的积分量为：%.2f' % (need_add_p_p, need_add_p_i))
        return need_add_p_p, need_add_p_i

    def _read_weather(self, start_time: str, end_time: str) -> tuple:
        try:
            SQL_Templet = 'SELECT avg(temp), avg(hr), avg(lux), avg(wind_speed) FROM %s WHERE create_time > "%s" and create_time <= "%s" and project_id = %s'
            SQL_TEXT = SQL_Templet % (self.config_map['WeatherStation_Table'], start_time, end_time, self.config_map['WeatherStation_ID'])
            temp_data = self.data_checker.check_data(SQL_TEXT)
            temp, hr, lux, wind_speed = temp_data[0]
        except:
            self.logger.to_error('当前气象站数据无法查询')
            temp = 0.0
            hr = 20
            lux = 0.0
            wind_speed = 1.0
        return min(float(temp), 17.5), float(hr), float(lux), float(wind_speed)

    def _check_cur_time(self) -> datetime.datetime:
        cur_time_ = time.localtime()
        cur_year = cur_time_.tm_year
        cur_month = cur_time_.tm_mon
        cur_day = cur_time_.tm_mday
        cur_hour = cur_time_.tm_hour
        cur_minute = cur_time_.tm_min
        cur_second = cur_time_.tm_sec
        return datetime.datetime(cur_year, cur_month, cur_day, cur_hour, cur_minute, cur_minute, cur_second)

    def _adj_heatload_of_weather(self, base_heatload, temp, lux, wind_speed, hour):
        # 室外温度调整
        temp_base_heatload = base_heatload / np.exp(26*0.03)
        adj_base_heatload = temp_base_heatload * np.exp((18-temp) * 0.03)
        # 光照调整
        #   读取光照调整系数表
        lux_adj_table = pd.read_csv('buildings/tables/lux_adj_table_%d.csv' % self.proj_id, encoding='gbk', index_col='时刻')
        lux_1, lux_2, lux_3, lux_4 = lux_adj_table[lux_adj_table.index == hour].values[0]
        #   确定调整系数
        if lux < lux_3:
            coef_lux = 1.2
        elif lux < lux_2:
            coef_lux = 1.1
        elif lux < lux_4:
            coef_lux = 1.0
        else:
            coef_lux = 0.9
        # 风速调整
        #   读取风速调整系数表
        wind_adj_table = pd.read_csv('buildings/tables/wind_adj_table_%d.csv' % self.proj_id, encoding='gbk')
        for i, j in wind_adj_table.values:
            if wind_speed < i:
                coef_wind = j
                break
        else:
            coef_wind = 1.30

        return adj_base_heatload * coef_wind * coef_lux

    def _adj_heatload_of_clock(self, base_heatload, hour):
        clock_adj_table = pd.read_csv('buildings/tables/clock_adj_table_%d.csv' % self.proj_id, encoding='gbk')
        _, coef_clock = clock_adj_table[clock_adj_table['时刻'] == hour].values[0]
        return base_heatload * coef_clock

    def _read_indoor_temp(self, cur_time: str) -> float:
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
        #print(res_json)
        for area in res_json['data']['targetList']:
            if area['areaName'] == self.config_map['Area_Name']:
                indoor_temp = area['target']['indoorAvgTemp']
                break
        else:
            indoor_temp = float(self.config_map['Random_Set_Indoor_Temp'])
            self.logger.to_error('查询 %s 平均温度失败，将平均温度暂时设定为%.1f度' % (self.config_map['Area_Name'],
                                                                   indoor_temp))

        return indoor_temp


if __name__ == '__main__':
    logger = Logger('../log/info_test.txt', '../log/error_test.txt')
    standard_building = StandardBuilding(config_file_path='configs/building_config_100002.config',
                                         logger=logger)
    print('Here')
    standard_building._read_indoor_temp('2021-11-01 18:15:00')
