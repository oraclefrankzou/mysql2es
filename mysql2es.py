from app import mysql2es_run
from app import logger

#主函数运行
if __name__ == '__main__':
    logger.info("mysql2es开始启动...")
    mysql2es_run()