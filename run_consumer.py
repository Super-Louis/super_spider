import logging
import os
from logging.handlers import TimedRotatingFileHandler
import multiprocessing
from modules import Url_Producer, Url_Consumer
from fetch_proxy import CrawlerProxy

class Run():

    def __init__(self,crawler):
        self.crawler = crawler
        self.logger = self.init_logger()


    def init_logger(self):
        logger = logging.getLogger('run')  # 创建一个logger
        logger.setLevel(logging.INFO)  # logger等级总开关
        # 创建一个handler,用于写入日志文件(timed rotating file handler)
        logfile = os.path.abspath('./modules/logs/run.log')
        fh = TimedRotatingFileHandler(
            filename=logfile,
            when='midnight',
            interval=1,
            encoding='utf-8'
        )
        fh.setLevel(logging.INFO)  # 输出到file的等级总开关
        # 创建一个handler,用于输出到控制台
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        # 定义handler的输出格式
        formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # 将logger添加到handler里面
        logger.addHandler(fh)
        logger.addHandler(ch)
        return logger

    def run(self):
        task_list = [Url_Consumer().run]
        ps = []
        for t in task_list:
            p = multiprocessing.Process(target=t,args=())
            ps.append(p)
            p.start()
        for p in ps:
            p.join()


if __name__ == '__main__':
    run = Run('weibo')
    run.run()