#!/usr/bin/env python
#该类是一个通用的执行map任务的类
class Map_worker:
    def __init__(self,mapper_class,input_data):
        self.mapper=mapper_class()
        self.mapper.worker=self
        if isinstance(input_data,list):
            self.input_data=input_data
        else:
            print "Please input list"
        self.output=[]
    def RunMapper(self):
        self.output=self.mapper.Map(self.input_data)
#该类用于让用户继承，继承后编写某个特定功能的map代码
class Mapper:
    def __init__(self):
        self.worker=None
    def Map(self,input_data):
        pass

