from elasticsearch import Elasticsearch
import json
import app

#用来实现对es基本的操作

class ServiceES():
    def __init__(self,esip):
        self.esip=esip
        try:

           self.es=Elasticsearch(esip)
           app.logger.info(
               "elasticsearch服务器:" + str(self.esip) + " 连接成功")
        except Exception as e:
            app.logger.error(
                "elasticsearch服务器:" +str(self.esip)+" 连接失败"+" "+e.args.__str__())


    #数据保存
    def saveEs(self,index,index_type,data):
        try:
           self.es.index(index=index, doc_type=index_type, body=data)
           app.logger.info("保存成功, elasticsearch:"+self.esip+" index:"+index+" index_type:"+index_type+"body:"+data.__str__())
           return "1"
        except Exception as e:
           app.logger.error(
                "失败, elasticsearch:" + self.esip + " index:" + index + " index_type:" + index_type + "body:" + data.__str__()+" "+e.args.__str__())
           return  "0"

    #根据id来删除他
    def deleteEs(self,index,index_type,id):
        try:
           self.es.delete(index=index, doc_type=index_type, id=id)
           app.logger.info(
               "删除成功, elasticsearch:" + self.esip + " index:" + index + " index_type:" + index_type + "id:" + id)
           return  "1"
        except Exception as e:
            app.logger.error(
                "删除成功, elasticsearch:" + self.esip + " index:" + index + " index_type:" + index_type + "id:" + id+" "+e.args.__str__())
            return  "0"

    #每个表都有主键，根据主键来查询数据
    def getDataById(self,index,index_type,id):
        body={
                 "query": {
                 "match": {"id":id}
                }
        }
        try:
            result={}
            result=self.es.search(index=index, doc_type=index_type,body=json.dumps(body))
            return (result["hits"]["hits"][0]["_id"])
        except Exception as e:
            app.logger.error(
                "查询成功, elasticsearch:" + self.esip + " index:" + index + " index_type:" + index_type + "body"+json.dumps(body)+" "+e.args.__str__())
            return "0"
