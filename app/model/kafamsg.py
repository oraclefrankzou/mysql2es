

#用来封装kafka消息
class KafaMsg():

    def __init__(self,database,table,type,ts,xid,position,data,old=None):
        self.database=database
        self.table =table
        self.type=type
        self.ts =ts
        self.xid = xid
        self.position = position
        self.data = data
        self.old = old

    def to_json(self):
        data={}
        data["database"] = self.database
        data["table"] = self.table
        data["type"] = self.type
        data["ts"] = self.ts
        data["xid"] = self.xid
        data["position"] = self.position
        data["data"] = self.data
        data["old"] = self.old

        return  data