#!/usr/bin/env python
# encoding:utf-8

from module.dgraph import Dgraph
from users import Users
class Works():

    def __init__(self, work_id, status, created,  user_id):


        self.work_id = work_id
        self.user_id = user_id
        self.user = Users(user_id)  # 对应的user的xid
        self.work = Dgraph(work_id)  # 对应的xid

        if self.work.get_uid()['status'] < 0 : # 不存在则创建
            data = {
                "xid": work_id,
                "type.work": "",
                "work.status": status,
                "work.created_at": "T".join(created.split(" "))
            }
            self.work.add(data)

        self.uid = self.work.get_uid()['data']



    def create_node(self):
        '''
        创建work的节点
        :return:
        '''
        data = {
            "uid": self.user.get_uid()['data'],
            'work': {
                'uid': self.work.get_uid()['data']
            }
        }

        return self.work.add(data)



    def delete_node(self):
        '''
        从dgraph 删除节点

        先删除uid的关联  <uid(user)> <work> <uid(work)> .
        再删除work的关联 <uid(work)> * *

        :return:
        '''
        user_work = {
            'uid': self.user.get_uid()['data'],
            'news' : {
                'uid' : self.work.get_uid()['data']
            }
        }

        if self.work.delete(user_work)['status'] >= 0 :

            return self.work.delete(
                {
                'uid' : self.work.get_uid()['data']
                }
            )

        else:
            return {
                "status": -1,
                "data"  : " remove work-users bad"
            }



    def __del__(self):
        del self.user
        self.work.close()

