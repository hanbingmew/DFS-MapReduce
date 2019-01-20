#!/usr/bin/env python
#该文件实现对所有流程的总体控制，运行时在thumm01上运行python3 control_all.py 文件名 即可完成整个mapreduce过程
import os
import re
import sys
import subprocess
import uuid
from uuid import UUID
from functools import reduce
import my_dfs
import my_reduce
#该类为计算均值和方差的reducer类
class CalmeanvarReducer(my_reduce.Reducer):
#该方法输入一个字典，格式为{节点编号：[总数字个数，所有该节点处理的文件块中数字总和，总平方和]},输出[均值，方差]
    def Reduce(self,input_data):
        count=0
        sum_1=0
        sum_2=0
        mean=0
        output=[]
        for (key,value) in input_data.items():
            count=count+value[0]
            sum_1=sum_1+value[1]
            sum_2=sum_2+value[2]
            mean=sum_1*1.0/count
        output.append(mean)
        output.append(sum_2*1.0/count-mean**2)
        return output
#该函数对从文件中读取的文件块标识符进行预处理，返回一个包含文件块标识符的列表
def pre_strlist(l):
    s1=l.replace('[','').replace(']','').replace(' ','').replace('(','').replace(')','').replace('\'','').replace('UUID','')
    list1=s1.split(',')
    return list1
#该函数对从thumm02-05传回的结果文件进行预处理，处理后的结果进行reduce。输入的每个结果文件里包含一个字典，格式为{文件块序号：[数字个数，数字的和，数字的平方和]},处理后输出一个列表，包含该节点处理的所有文件块的总数字个数，和，平方和，格式为[总数字个数，所有该节点处理的文件块中数字总和，总平方和]
def pre_dict(s):
    s1=s.replace('{','').replace('}','').replace(' ','')
    pattern=re.compile(r'\[\S*?\]')
    list1=pattern.findall(s1)
    list2=[]
    for i in list1:
        list2.append(i.replace('[','').replace(']',''))
    list3=[i.split(',') for i in list2]
    list4=[[float(i[j]) for i in list3] for j in range(3)]
    list5=[reduce((lambda x,y:x+y),i) for i in list4]
    return list5

    
if __name__ == "__main__":
#先把thumm02-05工作文件夹清空
    for i in range(2,6):
        str1='ssh 2018210904@thumm0%s "cd /data/dsjxtjc/2018210904;rm -f *"' % (i)
        p1=subprocess.Popen(str1,shell=True)
        p1.wait()
    nd=my_dfs.Namenode() #实例化一个DFS
    client=my_dfs.Client(nd) #实例化一个DFS客户端
    file_open=sys.argv[1]
    with open('%s' % (file_open),'r') as fr:
        data1=fr.readlines()
    client.write('%s' % (file_open),data1)#把需要统计均值和方差的文件写入DFS系统中
#以下这段代码是把存放在thumm02上的文件块删掉，用于测试容错机制
#    rm_list=pre_strlist(str([k for (k,v) in client.namenode.chunktable.items() if v==1]))
#    for i in rm_list:
#        str3='ssh 2018210904@thumm02 "cd /data/dsjxtjc/2018210904;rm %s"' % (i)
#        p3=subprocess.Popen(str3,shell=True)
#        p3.wait()
    process_content={} #该字典存放每个任务节点需要处理的文件块标识符列表 格式为{节点号：[文件块标识符]}
    p=[]#该列表用于存放4个节点上运行的子进程
#对于4个任务节点，首先从DFS的文件块映射表中读取该节点上存储的文件块标识符，把标识符列表写入process+节点编号文件中，然后把这个文件和my_map.py,map_master.py三个文件传给对应的节点，远程运行 python map_master.py在任务节点上执行map任务，并把4个子进程加入列表p中
    for i in range(1,5):
        process_content[i]=[k for (k,v) in client.namenode.chunktable.items() if v==i]
        with open('process%s' % (i+1),'w') as fw:
            fw.write(str(process_content[i]))
        os.system('scp process%s 2018210904@thumm0%s:/data/dsjxtjc/2018210904' % ((i+1),(i+1)))
        os.system('scp my_map.py 2018210904@thumm0%s:/data/dsjxtjc/2018210904' % (i+1))
        os.system('scp map_master.py 2018210904@thumm0%s:/data/dsjxtjc/2018210904' % (i+1))
        str2='ssh 2018210904@thumm0%s "cd /data/dsjxtjc/2018210904;python map_master.py"' % (i+1)
        p.append(subprocess.Popen(str2,shell=True))
#等4个子进程都运行完，即4个任务节点都执行完map任务后再继续执行后面的程序
    for i in p:
        i.wait()    
    after_map={}
#读取4个任务节点传回的结果文件并预处理，处理后的结果送给reduce，最终结果在thumm01的finalresult.txt中
    for i in range(1,5):
        with open('resultprocess%s' % (i+1),'r') as fr:
            data2=fr.read()
        after_map[i]=pre_dict(data2)
    worker=my_reduce.Reduce_worker(CalmeanvarReducer,after_map)
    worker.RunReducer()
