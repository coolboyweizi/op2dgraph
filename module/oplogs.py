#!/usr/bin/env python
# encoding:utf-8

import pymongo
import time
import os
from bson import timestamp
import datetime
from cols import users
from cols import news
from cols import works
from cols import follow

class Oplogs():

    def __init__(self, host, db, collections=None):
        self.client = pymongo.MongoClient(host=host)
        self.collections = collections
        self.db = db

        self.cols = []
        for col in collections :
            self.cols.append('.'.join((self.db, col)))

    @staticmethod
    def __get_id(op):
        '''
        获取主键ID
        :return:
        '''
        id = None
        o2 = op.get('o2')
        if o2 is not None:  id = o2.get('_id')
        if id is None:      id = op['o'].get('_id')
        return id

    def __get_data(self, id, col):
        cols = getattr(getattr(self.client,self.db), col) # 获得集合
        return cols.find_one({"_id":id})


    def __logger(self,data, logfile):

        obj = open(logfile, "a+")
        obj.write(data)
        obj.flush()
        obj.close()

    def __log_error(self, data):

        strDate = time.strftime('%Y-%m-%d', time.localtime(time.time()))  # 这里格式是‘%Y-%m-%d’，可有其他格式，也可只求年和月。
        logfile = os.sep.join(("logs",strDate+"_err"))
        self.__logger(data,logfile)

    def __log_data(self, data):
        strDate = time.strftime('%Y-%m-%d', time.localtime(time.time()))  # 这里格式是‘%Y-%m-%d’，可有其他格式，也可只求年和月。
        logfile = os.sep.join(("logs", strDate + "_out"))
        self.__logger(data,logfile)

    def out(self):
        '''
        输出打印oplog
        :return:
        '''
        oplog_rs = self.client.local.oplog.rs
        first = oplog_rs.find().sort("$natural", pymongo.ASCENDING).limit(-1).next()
        ts = first['ts']

        #cur = datetime.datetime.now()
        #ts = timestamp.Timestamp(datetime.datetime(cur.year, cur.month, cur.day), 0)
        while True:

            cursor = oplog_rs.find(
                {
                    "ts": {"$gt": ts},
                    "ns": {"$in": self.cols}
                },
                cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                oplog_replay=True
            )

            while cursor.alive:
                try:
                    for op in cursor:
                        ts = op['ts']

                        if op['op'] not in ("i","u","d"):   continue
                        col = op['ns'].split(".")[1]

                        if col not in self.collections: continue

                        id = self.__get_id(op)
                        self.run(col=col, id=id, raw=op)

                except BaseException as e:
                    print "currer_error:",e
                    #self.__log_error( "error cursor: %s \r" %(e) )
                    time.sleep(1)

    def run(self, id, col, raw):
        print col

        print raw['op']
        return
        if col == "users":
            self.users(id, col, raw)

        if col in ("works","news"):
            self.worksORnews(id, col, raw)

        if col == "user_followings":
            self.follow(id=id, col=col, raw=raw)

        if col == "user_unfollowed":
            self.unfollow(id=id, col=col, raw=raw)

    def users(self,id ,col, raw):
        '''
        添加用户
        :param id:
        :param col:
        :param raw:
        :return:
        '''
        if raw['op'] == 'i':

            timestamp = "%s add %s for %s " %(raw['ts'].as_datetime(), col, id)
            try:
                obj = users.Users(user_id=id)
                rs = obj.get_uid()
                if rs['status'] < 0 :
                    self.__log_error(
                        "\n".join((timestamp, rs['data'],  "\r"))
                    )

                else:
                    self.__log_data(
                        "\n".join((timestamp,  "\r"))
                    )
            except BaseException as e:
                self.__log_error(
                    "\n".join((timestamp, e, "\r"))
                )
            finally:
                del obj
        else:
            pass

    def worksORnews(self,id ,col, raw):
        '''
        work 或 news 节点添加。该节点适用于单对单
        :param id:      主键ID
        :param col:     集合
        :param raw:     改变的参数
        :return:
        '''

        try:
            data = raw['o']['$set']
        except:
            data = raw['o']

        if data.has_key("status") == False:
            return


        try:
            data = self.__get_data(id,col)

            if col == 'news':
                obj = news.News(
                        news_id= id,
                        user_id= data['user_id'],
                        created= data['created_at'],
                        type   = data['type']
                )

            elif col == 'works':
                obj = works.Works(
                        user_id=data['user_id'],
                        work_id=id,
                        created= data['created_at'],
                        status = data['status']
            )
            else:
                return

            timestamp = "%s %s %s add %s %s for %s " %(raw['ts'].as_datetime(), col, raw['op'],col, id, data['user_id'])
            if data.get('status', 0) == 1:
                rs = obj.create_node()
            else:
                rs = obj.delete_node()

            if rs['status'] < 0:
                self.__log_error(
                    "\n".join((timestamp, rs['data'],  "\r"))
                )
            else:

                self.__log_data(
                    "\n".join((timestamp, "\r"))
                )

        except BaseException as e:
            self.__log_error(
                "\n".join((timestamp,e))
            )
        finally:
            del obj


    def follow(self, id, col, raw):

        if raw['op'] in ('i','u'):
            try:
                data = raw['o']['$set']
            except:
                data = raw['o']
        else:
            return

        item = {}
        for keys in data.keys():
            if 'following_details' in keys:
                item = data.get(keys) # 更新的数据
                break

        if len(item) < 1:
            return # no data

        data = self.__get_data(id, col)

        timestamp = "%s add %s for %s" %(raw['ts'].as_datetime(), col,data['user_id'])

        try:

            obj = follow.Follow(data['user_id'], data['following_details'])
            rs = obj.create_node()
            if rs['status'] < 0:
                self.__log_error(
                    "\n".join((timestamp, rs['data'], "\r"))
                )

            else:
                self.__log_data(
                    "\n".join((timestamp,   "\r"))
                )
        except BaseException as e :
            self.__log_error(
                "\n".join((timestamp, e))
            )
        finally:
            del obj



    def unfollow(self,id, col, raw):
        '''
        取消关注
        :param id:
        :param col:
        :param raw:
        :return:
        '''

        if raw['op'] in ('i','u'):
            try:
                data = raw['o']['$set']
            except:
                data = raw['o']
        else:
            return

        item = []
        for keys in data.keys():
            if 'unfollowed_details' in keys:
                if isinstance(data.get(keys), dict):
                    item.append(data.get(keys))
                else:
                    item = data.get(keys)

        if len(item) < 1 : return
        user = self.__get_data(id, col)
        print raw
        timestamp = "%s delete %s for %s" % ( raw['ts'].as_datetime() ,col, user['user_id'])
        try:

            obj = follow.Follow(user['user_id'], item)
            rs  =  obj.delete_node()

            if rs['status'] < 0:
                self.__log_error(
                    "\n".join((timestamp, rs['data'], "\r"))
                )
            else:
                self.__log_data(
                    "\n".join((timestamp,   "\r"))
                )
        except BaseException as e :
            self.__log_error(
                "\n".join((timestamp, e))
            )
        finally:
            del obj




