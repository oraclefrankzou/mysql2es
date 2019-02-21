from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import logging
from settings import Settings
from app.service.servicees import ServiceES
from app.service.servicekafa import ServiceKafa


#为应用定义日志配置
logging.basicConfig(level=Settings.LOGGING_LEVEL,format="%(asctime)s-%(levelname)s-%(module)s-%(funcName)s- %(lineno)d:%(message)s  ",filename=Settings.LOGGING_FILE)

logger=logging.getLogger()



def mysql2es_run():
    serviceES = ServiceES(esip=Settings.ESIP)
    serviceKafa=ServiceKafa(topics=Settings.TOPICS,bootstrap_servers=Settings.BOOTSTRAP_SERVERS)
    serviceKafa.listenKafka(serviceES=serviceES)


