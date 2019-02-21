
import logging

class Settings():
    # 定义日志级别和日志文件路径
    LOGGING_LEVEL = logging.INFO
    LOGGING_FILE = "app/logs/mysql2es.log"

    #定义kafka信息,ip:port
    BOOTSTRAP_SERVERS="10.0.0.3:9092"
    TOPICS="test"

    #定义elasticearch服务器信息,ip:port
    ESIP="10.0.0.3:9202"



