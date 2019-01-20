#!/usr/bin/env python
#该文件在远程节点上通过远程命令运行，运行后执行任务节点上的map任务
import my_map
import os
import re
import uuid
from uuid import UUID
from multiprocessing import Pool
from functools import reduce

#该类实现map任务，即求出每个文件块中所有行的行数，和，平方和
class CalmeanvarMapper(my_map.Mapper):
    def Map(self,input_data):
        output=[]
        output.append(len(input_data))
        output.append(reduce((lambda x,y:x+y),input_data))
        square=lambda x:x ** 2
        output.append(reduce((lambda x,y:x+y),map(square,input_data)))
        return output
#该函数把字符串转换成浮点数，如果某一项无法转成浮点数，用None填充   
def convert_float(l):
    res=[]
    for i in l:
        try:
            res.append(float(i))
        except ValueError:
            res.append(None)
    return res
    
##该函数对从文件中读取的文件块标识符进行预处理，返回一个包含文件块标识符的列表    
def pre_strlist(l):
    s1=l.replace('[','').replace(']','').replace(' ','').replace('(','').replace(')','').replace('\'','').replace('UUID','')
    list1=s1.split(',')
    return list1
#对从文件块中读取的内容进行预处理，返回一个浮点数列表输入给map
def pre_data(s):
    s1=s.replace('[','').replace(']','').replace('\'','').replace('\\n','').replace(' ','')
    list1=s1.split(',')
    list2=convert_float(list1)
    for i in list2:
        try:
            list2.remove(None)
        except ValueError:
            break
    return list2
    
#对每一个文件块执行map任务，包含容错机制。如果发现数据块丢失，到相邻的两个节点中复制过来
def do_map(chunk_uuid):
    try:
        with open('%s' % chunk_uuid,'r') as fr:
            input_data=fr.read()
    except IOError:
        os.system('scp 2018210904@thumm0%s:/data/dsjxtjc/2018210904/%s /data/dsjxtjc/2018210904' % ((int(my_id)%4+1),(chunk_uuid)))
        os.system('scp 2018210904@thumm0%s:/data/dsjxtjc/2018210904/%s /data/dsjxtjc/2018210904' % ((int(my_id)%4+2),(chunk_uuid)))
        with open('%s' % chunk_uuid,'r') as fr:
            input_data=fr.read()
    input_data=pre_data(input_data)
    worker=my_map.Map_worker(CalmeanvarMapper,input_data)
    worker.RunMapper()
    return worker.output


if __name__=='__main__':
#找到thumm01传来的process+节点编号文件，获得自己的id，并根据文件建立要处理的文件块标识符列表
    for root,dirs,files in os.walk(".", topdown=False): 
        for file in files: 
            if re.match('process',file): 
                fname = file
    my_id=str(re.findall(r'\d+$',fname)).replace('\'','').replace('[','').replace(']','')
    with open(fname,'r') as fr:
        data=fr.read()
    process_list=pre_strlist(data)
    processor=len(process_list)
    res=[]
    p = Pool(processes=5)#最大子进程数为5个，一次最多并行处理5个文件块
    for i in range(processor):
        res.append(p.apply_async(do_map, args=(process_list[i],)))
    p.close()
    p.join()#并行处理需要处理的所有文件块，处理结束后再继续执行后面的程序
#获取处理结果，写入resultprocess+节点编号文件,传回thumm01
    result={}
    for i in range(len(res)):
        result[i]=res[i].get()
    with open('result'+fname,'w') as fw:
        fw.write(str(result))
    os.system('scp %s 2018210904@thumm01:/home/dsjxtjc/2018210904' % ('result'+fname))
