import numpy as np
import os, time
from abc import ABCMeta, abstractmethod
from tools.tools import parse_config_file, Logger
from buildings.heatloaddemanding import StandardBuilding
import requests
import json


class AbsBoiler(object):
    __meta_class__ = ABCMeta

    def __init__(self, config_file_path):
        self.config_file_path = config_file_path
        # 解析配置文件
        self.config_map = parse_config_file(self.config_file_path)
        # 热源组编号
        self.boilers_id = int(self.config_map['Boilers_ID'])
        # 关联的建筑物群编号
        self.building_ids = [i for i in self.config_map['Building_IDs'].split(',') if i.strip() != '']
        # 运行日志文件和错误日志文件地址
        info_path = os.path.join('log', 'info_%d.txt' % self.boilers_id)
        error_path = os.path.join('log', 'error_%d.txt' % self.boilers_id)
        self.logger = Logger(info_path, error_path)
        # 初始化建筑物群
        self.building_group = []
        for building_id in self.building_ids:
            building_config_path = 'buildings/configs/building_config_%s.config' % building_id
            self.building_group.append(StandardBuilding(building_config_path, self.logger))
            self.logger.to_info('成功创建建筑物模型-%s ' % building_id)

    @abstractmethod
    def extra_control(self) -> float:
        """
        该方法完成本控制周期内的辅助控制，同时返回该辅助控制所伴随的对热量的需求

        :return:
        """

    @abstractmethod
    def dispatch_control(self, total_q) -> None:
        """
        该方法需要将本控制周期需要的总热量，转换为控制指令，分发到各个热源设备上

        :return:
        """

    def episode(self):
        # 解析配置文件
        self.config_map = parse_config_file(self.config_file_path)
        # 第一步计算出该控制周期常规建筑物群对热量的需求总和
        building_demand_q = sum([building.main()[1] for building in self.building_group])
        # 第二步执行该项目的辅助控制操作
        extra_demand_q = self.extra_control()
        # 第三步将总体热量分发至不同的热源设备进行执行
        total_demand_q = building_demand_q + extra_demand_q
        self.dispatch_control(total_demand_q)

    def main(self):
        while True:
            self.logger.to_info('开始本轮控制')
            self.episode()
            time.sleep(int(self.config_map['Control_Interval']) * 60)


class StandardBoiler(AbsBoiler):
    def extra_control(self) -> float:
        return 0

    def dispatch_control(self, total_q):
        pass

    def _post_control(self, control_json):
        access_token = '17_-z7lTsRwNjyjaUYlgE-NQCgDJIMWMVe3K7rogtG_9M' + \
                       'yhdXI8hj0pYNZnvoNJdGACUVv28vRpTw0FM_WGNXLaQw8kFf0tryaMV0XeF0KS' + \
                       'hgg0DsyyhwEo96mCtlaWHe2DY4QbyUFUE169oU7IGCKdAAAJFN'
        targetURL = 'http://47.94.209.71/600ly-ctrl/cmdCenter/sendBatchCmd/v3.json?access_token=%s&model_id=%s'
        targetURL = targetURL % (access_token, self.config_map['MODEL_ID'])
        res = requests.post(targetURL, json=control_json)
        self._validateRES(res)

    def _validateRES(self, res):
        try:
            res_j = json.loads(res.content.decode('utf-8'))
            errcode = res_j['errcode']
            if errcode == 0:
                return
            else:
                self.logger.to_error('控制指令下发失败')
        except Exception as e:
            print(e)
            self.logger.to_error('控制指令下发失败')

    def _start_or_stop_boiler(self, start_or_stop, boiler_id):
        control_JSON = [
            {
                'id': None,
                'itemCode': boiler_id,
                'itemValue': '%d' % start_or_stop,
                'fromPlatform': 'ML',
                'downType': 'GL_SS_SET',
                'expiryTime': 1572612098013,
                'executionTime': 1572612098013,
                'desc1': '%d#锅炉启停设定' % boiler_id,
                'isCtrl': 1
            }
        ]
        if start_or_stop:
            info = '启动%d#锅炉' % boiler_id
        else:
            info = '关闭%d#锅炉' % boiler_id
        self.logger.to_info(info)
        self._post_control(control_JSON)
        time.sleep(3)

    def _set_boiler(self, set_value, boiler_id):
        control_JSON = [
            {
                'id': None,
                'itemCode': boiler_id,
                'itemValue': '%d' % set_value,
                'fromPlatform': 'ML',
                'downType': 'GL_O_PR_SET',
                'expiryTime': 1572612098013,
                'executionTime': 1572612098013,
                'desc1': '%d#锅炉启停设定' % boiler_id,
                'isCtrl': 1
            }
        ]
        info = '设定%d#锅炉的负荷为：%d' % (boiler_id, set_value)
        self.logger.to_info(info)
        self._post_control(control_JSON)

    def _check_boiler_state(self, boiler_id):
        return True, 45


if __name__ == '__main__':
    boiler = StandardBoiler('configs/boiler_config_200001.config')
    boiler.main()
    print('over')
