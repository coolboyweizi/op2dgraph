#!/usr/bin/env python
# encoding:utf-8

import time

from users import Users


class Follow(Users):

    def __init__(self, user_id, follow_details):

        Users.__init__(self,user_id)
        self.flow = follow_details  #



    def create_node(self):
        '''
        创建friend的节点. 删除该所有的friend数据，再重新建立
        :return:
        '''
        self.delete_node()
        items = []

        for item in self.flow:

            obj = Users(user_id=item['user_id'])
            items.append(
                {
                    'uid': obj.get_uid()['data'],
                    "friend|since": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(item['follow_at']))
                }
            )
            del obj


        data = {
            "uid": self.get_uid()['data'],
            'friend': items
        }

        return self.user.add(data)


    def delete_node(self):

        p = {
            'uid':self.get_uid()['data'],
            'friend':  None
        }

        return self.user.delete(p)

