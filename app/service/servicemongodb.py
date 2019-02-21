import app
import  pymongo
import datetime


class ServiceMongodb():
    def __init__(self,host,port,dbname):
        self.host=host
        self.port=port
        self.dbname=dbname
        try:
            pymongoClient=pymongo.MongoClient(host=self.host,port=self.port)
            self.mongoDb=pymongoClient[self.dbname]
            self.collection = self.mongoDb["topics_offset"]
            app.logger.info("mongodb服务器:"+host+" port:"+str(port)+" dbname:"+dbname+" 连接成功")

        except Exception as e:
            app.logger.error("mongodb服务器 连接失败" + host + " port:" + str(port) + " dbname:" + dbname+" 异常信息:"+e.args.__str__())

    def getDataByTopics(self,topics):
        try:
          results = self.collection.find_one({"topics":topics})
          app.logger.debug("mongodb查询 topics:" + topics.__str__())
          return results
        except Exception as e:
            app.logger.error( "mongodb查询失败 topics:"+topics.__str__()+" 异常信息:"+e.args.__str__())

    def saveData(self,topics,offset):
        try:
            record={
                "topics":topics,
                "offset":offset,
                "opdate":datetime.datetime.now()
            }
            results=self.collection.save(record)
            app.logger.info("mongodb保存成功 :" + record.__str__())
            return results
        except Exception as e:
            app.logger.error("mongodb保存失败 : topics" +topics.__str__() )


    def updateData(self, topics, offset):
        try:
            records=self.getDataByTopics(topics)
            records["topics"]=topics
            records["offset"] = offset
            records["opdate"] = datetime.datetime.now()
            results=self.collection.update({"topics":topics},document=records)
            return results
            app.logger.debug("mongodb更新成功 :" + records.__str__())
        except Exception as e:
            app.logger.error("mongodb更新失败 : topics" +topics.__str__()+" offset:"+str(offset) )

