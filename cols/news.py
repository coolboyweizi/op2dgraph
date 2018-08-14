#!/usr/bin/env python
# encoding:utf-8

from module.dgraph import Dgraph
from users import Users
class News():

    def __init__(self, news_id, created, type,  user_id):

        self.user = Users(user_id)  # 对应的user的xid
        self.news = Dgraph(news_id)  # 对应的xid

        if self.news.get_uid()['status'] < 0 : # 不存在则创建
            data = {
                "xid": news_id,
                "news.type": type,
                "news.created_at": "T".join(created.split(" "))
            }
            self.news.add(data)
        self.uid = self.news.get_uid()['data']


    def create_node(self):
        '''
        创建work的节点
        :return:
        '''
        data = {
            "uid": self.user.get_uid()['data'],
            'news': {
                'uid': self.uid
            }
        }

        return self.news.add(data)

    def delete_node(self):
        '''
        从dgraph 删除节点

        先删除uid的关联  <uid(user)> <work> <uid(work)> .
        再删除work的关联 <uid(work)> * *

        :return:
        '''
        user_news = {
            'uid': self.user.get_uid()['data'],
            'news' : {
                'uid' : self.news.get_uid()['data']
            }
        }

        # 先删除user与news的关联。在删除news节点
        if self.news.delete(user_news)['status'] >= 0 :
            return self.news.delete(
                {
                'uid' : self.news.get_uid()['data']
                }
            )
        else:
            return {
                "status": -1,
                "data"  : " remove news-users bad"
            }

    def __del__(self):
        del self.user
        self.news.close()
