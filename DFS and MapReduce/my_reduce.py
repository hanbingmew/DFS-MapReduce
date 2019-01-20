#!/usr/bin/env python
#该类是一个通用的执行reduce任务的类，最终结果写在thumm01中的finalresult.txt中
class Reduce_worker:
    def __init__(self,reducer_class,input_data):
        self.reducer=reducer_class()
        self.reducer.worker=self
        if isinstance(input_data,dict):
            self.input_data=input_data
        else:
            print("Please input dictionary")
        self.output=[]
    def RunReducer(self):
        self.output=self.reducer.Reduce(self.input_data)
        with open('finalresult.txt','a+') as fw:
            fw.write(str(self.output))
#该类用于让用户继承，继承后编写某个特定功能的reduce代码
class Reducer:
    def __init__(self):
        self.worker=None
    def Reduce(self,input_data):
        pass
