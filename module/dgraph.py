#!/usr/bin/env python
# encoding:utf-8
# author: MasterKing
# desc: dgraph CURD

import pydgraph
import json
from config import dgraph_zero

pdgraph = dgraph_zero

class Dgraph():
    def __init__(self, xid):
        '''
        :param xid:  节点的xid
        '''
        self.client_stub = pydgraph.DgraphClientStub(pdgraph)
        self.client = pydgraph.DgraphClient(self.client_stub)
        self.xid    = xid


    def rebuild(self):
        '''
        重新建立表结构
        :return:
        '''
        schema = '''xid: string @index(exact) .
        type.user: string @index(exact) .
        type.work: string @index(exact) .
        friend: uid @count @reverse .
        work: uid @count @reverse .
        work.created_at: dateTime @index(hour) .
        work.status: int @index(int) .
        news: uid @count @reverse .
        news.type: string @index(exact) .
        news.created_at: dateTime @index(hour) .'''

        self.client.alter(pydgraph.Operation(drop_all=True))
        op = pydgraph.Operation(schema=schema)
        self.client.alter(op)

    def close(self):
        '''
        关闭连接
        :return:
        '''
        self.client_stub.close()



    def get_uid(self):
        '''
        获取xid对应的uid
        :return:
        '''
        result = {'status': 0}
        try:
            query = """query all($a: string) {
                            all(func: eq(xid, $a)) {
                                uid
                            }
                    }"""
            variables = {'$a': self.xid}
            res = self.client.query(query, variables=variables)
            ppl = json.loads(res.json)
            result['data'] = ppl['all'][0]['uid']
        except  BaseException as e:
            result = {"status": -1, "data": e}
        return result


    def add(self, p):
        '''
        添加节点
        :param p:   json格式，节点
        :return:
        '''
        txn = self.client.txn()
        result = {"status": 0}
        try:
            assign = txn.mutate(set_obj=p)
            txn.commit()
            result['data'] = assign.uids['blank-0']
        except BaseException as e:
            result = {"status": -1, "data": e}
        finally:
            txn.discard()
        return result



    def delete(self,p):
        '''
        删出节点
        :param p:  json格式
        :return:
        '''
        txn = self.client.txn()
        result = {"status": 0}
        try:
            assign = txn.mutate(del_obj=p)
            txn.commit()
            result['data'] = assign.uids['blank-0']
        except BaseException as e:
            result = {"status": -1, "data": e}
        finally:
            txn.discard()
        return result
