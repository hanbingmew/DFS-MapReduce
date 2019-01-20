#!/usr/bin/env python
#该文件用于建立一个DFS
import os
import uuid
import math

#用户通过实例化一个Client类与DFS交互
class Client:
    def __init__(self,namenode):
        self.namenode=namenode
#该方法把data写入到DFS中，文件名为filename
    def write(self,filename,data):
        chunk=[]
        chunk_loc=1
        num_chunks=self.get_num_chunks(data)#求文件分块数
        for i in range(0,num_chunks-1):
            chunk.append(data[i*self.namenode.chunksize:(i+1)*self.namenode.chunksize])
        chunk.append(data[(num_chunks-1)*self.namenode.chunksize:])#把data分块
        chunk_uuid=self.namenode.alloc(filename,num_chunks)#让元节点根据文件名和分块数建立逻辑映射，确定每个分块存储在哪个节点上，返回一个包含所有文件块标识符的列表
        for i in range(0,len(chunk_uuid)):
            chunk_loc=i%self.namenode.num_datanode+1#把每个文件块写到对应的数据节点上
            self.namenode.datanodes[chunk_loc].write(chunk_uuid[i],chunk[i])
            chunk_loc=chunk_loc%self.namenode.num_datanode+1
            self.namenode.datanodes[chunk_loc].write(chunk_uuid[i],chunk[i])#在该节点后面的两个节点分别复制一份，即每个文件存三份
            chunk_loc=chunk_loc%self.namenode.num_datanode+1
            self.namenode.datanodes[chunk_loc].write(chunk_uuid[i],chunk[i])
#该方法从DFS中读取名为filename的文件
    def read(self,filename):
        if self.namenode.exist(filename)==True:#先判断是否存在
            data=''
            chunk_uuid=self.namenode.filetable[filename]#从filetable中找到该文件对应的文件块的标识符列表
            for i in chunk_uuid:#对于每个文件块，从chunktable中找到其存放的位置，把它读取出来，然后拼到一起
                datanode=self.namenode.datanodes[self.namenode.chunktable[i]]
                data=data+datanode.read(i)
            return data
        else:
            print("The file "+filename+" does not exist")
#该方法从DFS中删除名为filename的文件
    def delete(self,filename):
        if self.namenode.exist(filename)==True:
            chunk_uuid=self.namenode.filetable[filename]
            for i in chunk_uuid:#对所有文件块进行物理删除
                chunk_loc=self.namenode.chunktable[i]
                datanode=self.namenode.datanodes[chunk_loc]
                datanode.delete(i)
                chunk_loc=chunk_loc%self.namenode.num_datanode+1
                datanode=self.namenode.datanodes[chunk_loc]
                datanode.delete(i)
                chunk_loc=chunk_loc%self.namenode.num_datanode+1
                datanode=self.namenode.datanodes[chunk_loc]
                datanode.delete(i)
            self.namenode.delete(filename)#对文件进行逻辑删除
        else:
            print("The file "+filename+" does not exist")
#该方法求出数据分块数       
    def get_num_chunks(self,data):
        return math.ceil(len(data)/self.namenode.chunksize)
#文件系统的元节点，保存文件、文件块与数据节点的映射关系    
class Namenode:
    def __init__(self):
        self.num_datanode=4 #有4个数据节点
        self.chunksize=10800000 #每个文件块大小为108MB
        self.filetable={}
        self.chunktable={}
        self.datanodes={}
        self.init_datanodes()
    def init_datanodes(self):
        for i in range(1,self.num_datanode+1):
            self.datanodes[i]=Datanode(i)
    def alloc(self,filename,num_chunks):#给文件块分配存储节点，在chunktable中建立文件块和存储节点的映射关系，再在filetable中建立文件名和文件块的映射关系。chunktable的格式为{文件块标识符：存储节点}，filetable格式为{文件名：[文件块标识符列表]}
        chunk_uuids=[]
        chunkloc=1
        for i in range(0,num_chunks):
            chunk_uuid=uuid.uuid1()
            chunk_uuids.append(chunk_uuid)
            self.chunktable[chunk_uuid]=chunkloc
            chunkloc=chunkloc%self.num_datanode+1
        self.filetable[filename]=chunk_uuids
        print(self.filetable)
        print(self.chunktable)
        return chunk_uuids
    def delete(self,filename):
        chunk_uuids=self.filetable[filename]
        for i in chunk_uuids:
            self.chunktable.pop(i)
        self.filetable.pop(filename)
    def exist(self,filename):
        if filename in self.filetable:
            return True
        else:
            return False
#数据节点，与系统进行交互执行各种任务       
class Datanode:
    def __init__(self,chunkloc):
        self.chunkloc=chunkloc
    def write(self,chunk_uuid,chunk): #把文件块写入远程数据节点，先把对应的内容写到本地的tmp.txt中，再用scp命令传给数据节点
        with open('tmp.txt','w') as fw:
            fw.write(str(chunk))
        os.system('scp ./tmp.txt 2018210904@thumm0%s:/data/dsjxtjc/2018210904/%s' % ((self.chunkloc+1),chunk_uuid))
    def read(self,chunk_uuid): #从远程数据节点读取文件块
        os.system('scp 2018210904@thumm0%s:/data/dsjxtjc/2018210904/%s ./tmp.txt' % ((self.chunkloc+1),chunk_uuid))
        with open('tmp.txt','r') as fr:
            data=fr.read()
        return data
    def delete(self,chunk_uuid): #在远程数据节点上删除文件块
        os.popen('ssh 2018210904@thumm0%s rm /data/dsjxtjc/2018210904/%s' % ((self.chunkloc+1),chunk_uuid))
#命令行，如果直接运行 python3 my_dfs.py会实例化一个Command类，让用户通过Command类与DFS进行交互，执行各种任务           
class Command:
    def __init__(self,client):
        self.client=client
    def upload_file(self): #上传文件到DFS
        filename=input('Input the filename you want to upload to dfs:')
        with open(filename,'r') as fr:
            data=fr.readlines()
        self.client.write(filename,data)
    def download_file(self): #从DFS上下载文件到本地
        filename=input('Input the filename you want to download from dfs:')
        data=self.client.read(filename)
        with open(filename,'w') as fw:
            fw.write(data)
    def command_line(self):
        while True:
            cmd=input('Input your command:')
            if cmd=='upload_file':
                self.upload_file()
            elif cmd=='download_file':
                self.download_file()
            elif cmd=='ls':
                for k in self.client.namenode.filetable:
                    print(k)
            elif cmd=='delete':
                filename=input('Input the filename you want to delete from dfs:')
                self.client.delete(filename)    
            elif cmd=='exit':
                break
            else:
                print('wrong command')

                
def main():
    nmd=Namenode()
    client=Client(nmd)
    command=Command(client)
    command.command_line()
    
if __name__=='__main__':
    main()
