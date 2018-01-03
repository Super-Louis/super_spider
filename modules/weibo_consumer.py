import asyncio
import aiohttp
import traceback
import logging
import re
import urllib
import motor.motor_asyncio
import time
import json
from spider_queue import *
import multiprocessing


basic_info_url = 'https://m.weibo.cn/api/container/getIndex'

headers = {
        'Accept':'application/json, text/plain, */*',
        'Accept-Encoding':'gzip, deflate, br',
        'Cache-Control':'no-cache',
        'Connection':'keep-alive',
        'Host':'m.weibo.cn',
        'Pragma':'no-cache',
        'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.75 Safari/537.36',
        }
l = logging.getLogger('run')

class Url_Consumer:
    def __init__(self):
        self._session = None
        self.client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://liuchao:liuchao@39.106.110.169:27017')
        self.collection = self.client.weibo.user_detail_info

    def __enter__(self):
        return self

    @property
    def session(self):
        if self._session is None:
            conn = aiohttp.TCPConnector(verify_ssl=False,
                                        limit=100,  # 连接池不能太大
                                        use_dns_cache=True)
            self._session = aiohttp.ClientSession(connector=conn)
        return self._session

    def check_exists(self,res,key):
        try:
            return res[key]
        except:
            return ''

    async def request_page(self,url,data=None,params=None,headers=None,proxy=False):
        retry = 0
        while True:
            retry += 1
            if retry == 31:
                return ''
            tag, ip = await self.proxy_mq.get('proxy_queue')
            try:
                if data:
                    async with self.session.post(url, data=data, headers=headers,proxy=ip.decode('utf-8'),timeout=3) as response:
                        # logging.info("proxy:{} is valid".format(ip))
                        await self.proxy_mq.put('proxy_queue',ip)
                        print(await response.text())
                        return await response.json()
                elif params:
                    async with self.session.get(url, params=params, headers=headers,proxy=ip.decode('utf-8'),timeout=3) as response:
                        # logging.info("proxy:{} is valid".format(ip))
                        await self.proxy_mq.put('proxy_queue',ip)
                        print(await response.text())

                        return await response.json()
                else:
                    async with self.session.get(url,headers=headers,proxy=ip.decode('utf-8'),timeout=3) as response:
                        # logging.info("proxy:{} is valid".format(ip))
                        await self.proxy_mq.put('proxy_queue',ip)
                        print(await response.text())

                        return await response.json()
            except Exception as e:
                l.info(e)
                # l.info("proxy:{} is not valid".format(ip))
                continue


    async def get_details(self):#初始从Task_Gernator中随机选取一个种子用户
        tag, url = await self.mq.get('info_url')
        url = url.decode('utf-8')
        l.info("get url {}".format(url))
        res = await self.request_page(url=url,headers=headers)
        res = await self.build_profile(res)
        additional_info = {
            'id': self.check_exists(res, 'id'),
            'tag': self.check_exists(res, '标签'),
            'lacation': self.check_exists(res,'所在地'),
            'company': self.check_exists(res,'公司'),
            'email': self.check_exists(res,'邮箱'),
            "blog": self.check_exists(res,'博客'),
            'level': self.check_exists(res,'等级'),
            'credit': self.check_exists(res,'阳光信用'),
            'register_time':self.check_exists(res,'注册时间')
        }
        try:
            await self.collection.find_one_and_update({"id":res['id']},
                                                      {"$set":{'tag':additional_info['tag'],
                                                      'lacation':additional_info['lacation'],
                                                      'company':additional_info['company'],
                                                      'email':additional_info['email'],
                                                      "blog":additional_info['blog'],
                                                      'level':additional_info['level'],
                                                      'credit': additional_info['credit'],
                                                      'register_time':additional_info['register_time']}})
            l.info("update information of url:{} to mongo".format(url))
        except Exception as e:
            l.info(e)
            pass

    async def build_profile(self,p):
        profile = dict()
        try:
            cards = p['data']['cards']
        except:
            pass
        else:
            for card in cards:
                if 'card_group' in card.keys():
                    for item in card['card_group']:
                        try:
                            profile[item['item_name']] = item['item_content']
                        except:
                            continue
            try:
                profile['id'] = int(re.findall(r'uid:(\d+)',json.dumps(p,ensure_ascii=False))[0])#int
            except Exception as e:
                pass
            return profile

    async def tasks(self):
        self.mq = await AsyncMqSession()
        self.proxy_mq = await AsyncMqSession()
        while True:
            tasks = [self.get_details() for _ in range(1000)]
            print(len(tasks))
            await asyncio.gather(*tasks)

    def worker(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.tasks())
        loop.close()

    def run(self):
        ps = list()
        for i in range(3):
            p = multiprocessing.Process(target=self.worker, args=())
            ps.append(p)
            p.start()
        for p in ps:
            p.join()

    def __exit__(self, exc_type, exc_val, exc_tb):

        if exc_tb:
            msg = f'exc type: {exc_type}, val: {exc_val}'
            l.info(msg)
            tb_list = traceback.format_exception(exc_type, exc_val, exc_tb)
            tb_str = ''.join(tb_list)
            l.error(tb_str)
            return False
        else:
            l.info("No exception")
            return True


if __name__ == '__main__':
    with Url_Consumer() as uc:
        uc.run()

