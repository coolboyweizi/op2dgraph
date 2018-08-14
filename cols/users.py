from module.dgraph import Dgraph
class Users():

    def __init__(self, user_id):

        self.user = Dgraph(user_id)
        self.user_id = user_id

    def get_uid(self):
        rs = self.user.get_uid()

        if rs['status'] < 0 :
            p = {
                'xid':self.user_id,
                'type.user':''
            }
            return self.user.add(p)
        return rs


    def __del__(self):
        self.user.close()
