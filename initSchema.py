#!/usr/bin/env python
# coding: utf-8
# desc  : 初始化表结构

from module.oplogs import Oplogs
from config import db_host,db_name,db_cols

import time
import os


# 导出数据
class outCollection(Oplogs):
    def __get_all_data(self,  col):
        cols = getattr(getattr(self.client, self.db), col)  # 获得集合
        return cols.find()


    def outPut(self):
        self.timestamp = str(time.time()).split()[0]


        for col in self.collections:
            for doc in self.__get_all_data(col):
                if col == "users":
                    self.putfile( self.users(doc))
                if col == "works":
                    self.putfile(self.works(doc))
                if col == 'news':
                    self.putfile(self.news(doc))
                if col == 'user_followings':
                    self.putfile(self.follow(doc))


        self.load()

    def putfile(self, data):
        fileObj = open('data/goldendata.rdf','a+')
        fileObj.write(data)
        fileObj.flush()
        fileObj.close()


    def load(self):
        os.system("gzip data/goldendata.rdf")
        '''
        pwd = os.path.curdir
        client = docker.from_env()
        client.containers.run(
            image='docker.io/dgraph/dgraph',
            remove=True,
            volumes="{'%s': {'bind': '/data',}" %(pwd+"/data"),
            command="dgraph live -r /data/goldendata.rdf.gz -s /data/goldendata.schema  -d localhost:9080 -z localhost:5080"
        )
        '''
        #os.system("")
        #os.system("docker run  -v %s:/data docker.io/dgraph/dgraph dgraph live -r /data/goldendata.rdf.gz -s /data/goldendata.schema  -d 172.30.1.50:9080 -z 172.30.1.50:5080 " %(os.curdir))
        #os.system("curl %s -H \"X-Dgraph-CommitNow: true\" -XPOST --data-binary @%s > %s_result" %(url, 'logs/'+self.timestamp, self.timestamp))



    def users(self, raw):
        data = '''
        _:user_%s <xid> "%s" .
        _:user_%s <type.user> "" .
        '''
        return data %( raw['_id'],raw['_id'],raw['_id'])

    def follow(self, raw):
        flow = '''
        _:user_%s <friend> _:user_%s (since="%s") .
        '''

        data = ""
        for item in raw['following_details']:
            data += flow %(
                raw['user_id'],
                item['user_id'],
                time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(item['follow_at']))
            )
        return data

    def works(self, raw):
        work = '''
        _:work_%s <xid> "%s" .
        _:work_%s <type.work> "" .
        _:work_%s <work.created_at> "%s" .
        _:work_%s <work.status> "%d" .
        _:user_%s <work> _:work_%s .
        '''

        if raw['status'] != 1 :
            return ''

        return work %(
            raw['_id'],
            raw['_id'],
            raw['_id'],
            raw['_id'],
            "T".join(raw['created_at'].split(" ")),
            raw['_id'],
            raw['status'],
            raw['user_id'],
            raw['_id']
        )


    def news(self, raw):
        news = '''
        _:news_%s <xid> "%s" .
        _:news_%s <type.news> "%s" .
        _:news_%s <news.created_at> "%s" .
        _:user_%s <news> _:news_%s .
        '''
        if raw['status'] != 1 :
            return ''

        return news %(
            raw['_id'],
            raw['_id'],
            raw['_id'],
            raw['type'],
            raw['_id'],
            "T".join(raw['created_at'].split(" ")),
            raw['user_id'],
            raw['_id'],
        )



if __name__ == '__main__':
    outCollection(db=db_name,
                  host=db_host,
                  collections=db_cols).outPut()

