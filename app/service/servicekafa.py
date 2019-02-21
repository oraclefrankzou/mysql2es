
from kafka import KafkaConsumer
from app.model.kafamsg import KafaMsg
import json
import app

#提供kafka，侦听消息，进行处理后写ES
class ServiceKafa():
   def __init__(self,topics,bootstrap_servers):
       self.topics=topics
       self.bootstrap_servers=bootstrap_servers
       #建议连接
       try:
           self.kc= KafkaConsumer(topics, bootstrap_servers=self.bootstrap_servers)
           app.logger.info("kafka服务器:" + bootstrap_servers + ":连接成功"+" topics:"+self.topics)
       except Exception as e:
           app.logger.error("kafka服务器:"+bootstrap_servers+":连接失败"+" topics:"+self.topics+" "+e.args.__str__())

   def listenKafka(self,serviceES):
        for message in self.kc:
            value=json.loads(message.value)
            app.logger.info("kafka服务器:"+self.bootstrap_servers+" topics:"+self.topics+" 消息:"+value.__str__())
            kafaMsg=KafaMsg(database=value["database"],table=value["table"],type=value["type"], \
                            ts=value["ts"],xid=value["xid"],position=value["position"], \
                            data = value["data"])

            #如果是insert的话， 直接保存就可以了
            if value["type"]=="insert":
                serviceES.saveEs(index=value["database"] + value["table"], index_type=value["table"],
                                 data=value["data"])

            #如果是update,就要先删除，再更新
            if value["type"]=="update":
                #根据表id,得到document id
                esResultsId = serviceES.getDataById(index=value["database"] + value["table"], index_type=value["table"],
                                          id=value["data"]["id"])
                if (esResultsId!="0"):
                       #根据document id删除原来的document
                       serviceES.deleteEs(index=value["database"] + value["table"], index_type=value["table"], id=esResultsId)
                       #保存新数据
                       serviceES.saveEs(index=value["database"] + value["table"], index_type=value["table"],
                                        data=value["data"])
                else:
                    serviceES.saveEs(index=value["database"] + value["table"], index_type=value["table"],
                                 data=value["data"])


            if value["type"] == "delete":
                # 根据表id,得到document id
                esResultsId = serviceES.getDataById(index=value["database"] + value["table"], index_type=value["table"],
                                                    id=value["data"]["id"])
                # 根据document id删除原来的document
                serviceES.deleteEs(index=value["database"] + value["table"], index_type=value["table"], id=esResultsId)