功能:

   实现同步mysql数据到elasticsearch
   
流程:
  1,maxwell软件从mysql中读取binlog日志到kafka软件中;
  
  2,mysql2es读取对应的kafka中的topics(和写入的maxwell一致);
  
  3,数据经过软件写入到elasticsearch中;
  

依赖软件:
maxwell,
kafka,
python 3.6以上版本,python包elasticsearch,kafka

配置:
 在settings.py中
 
 # 定义日志级别和日志文件路径
 LOGGING_LEVEL = logging.INFO
 LOGGING_FILE = "app/logs/mysql2es.lo
 #定义kafka信息,ip:port
 BOOTSTRAP_SERVERS="10.0.0.3:9092"
 TOPICS="tes
 #定义elasticearch服务器信息,ip:port
 ESIP="10.0.0.3:9202"     
 
 
 
 版权所有: oraclefrankzou, 联系方式: 327356330