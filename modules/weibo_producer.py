import asyncio
import aiohttp
import traceback
import logging
import re
import urllib
import motor.motor_asyncio
import time
import aioredis
from spider_filter import BloomFilter
from logging.handlers import TimedRotatingFileHandler
import multiprocessing
from spider_queue import *
from db_config import DB

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


class Url_Producer:
    def __init__(self):
        self._session = None
        self.client = motor.motor_asyncio.AsyncIOMotorClient(f"mongodb://{DB['mongo']['user']}:{DB['mongo']['password']}@{DB['mongo']['host']}:{DB['mongo']['port']}")
        self.collection = self.client.weibo.user_detail_info
        self.bf = BloomFilter()

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

    async def request_page(self,url,data=None,params=None,headers=None,proxy=False):
        if not proxy:
            retry = 0
            while retry < 3:
                try:
                    if data:
                        async with self.session.post(url,data=data,headers=headers,timeout=3) as response:
                            return await response.json()
                    if params:
                        async with self.session.get(url,params=params,headers=headers,timeout=3) as response:
                            return await response.json()
                except Exception as e:
                    l.info(e)
                    retry += 1
                    continue
        retry = 0
        while True:
            retry += 1
            if retry == 30:
                return None
            tag, ip = await self.proxy_mq.get('proxy_queue')
            try:
                if data:
                    async with self.session.post(url, data=data, headers=headers,proxy=ip.decode('utf-8'),timeout=10) as response:
                        # l.info("proxy:{} is valid".format(ip))
                        await self.proxy_mq.put('proxy_queue',ip)
                        return await response.json()
                if params:
                    async with self.session.get(url, params=params, headers=headers,proxy=ip.decode('utf-8'),timeout=10) as response:
                        # l.info("proxy:{} is valid".format(ip))
                        await self.proxy_mq.put('proxy_queue',ip)
                        return await response.json()
            except Exception as e:
                l.info(e)
                await asyncio.sleep(3)
                # l.info("proxy:{} is not valid".format(ip))
                continue


    async def crawler_entry(self):#初始从Task_Gernator中随机选取一个种子用户
        id = await self.redis.spop('user_id')
        if id:
            l.info("get id:{}".format(id))
            info_params = {
                    'type':'uid',
                    'value':id,
                    }
            basic_res = await self.request_page(url=basic_info_url,params=info_params,headers=headers,proxy=True)
            # print(basic_res)

            try:
                l.info("process inserting id:{} into mongodb".format(id))
                await self.collection.insert_one(basic_res['data']['userInfo'])
                l.info("inserted id:{} into mongodb".format(id))
            # TODO : save basic_res to mongo_db basic infomation ,add field:refer
                info_url = basic_res['data']['follow_scheme']
                follow_count = basic_res['data']['userInfo']['follow_count']
            except Exception as e:
                l.info(e)
                pass
            else:
                containerid_follow = re.findall(r'containerid=(.+?)&',info_url)[0].replace('_followersrecomm_','_followers_')
                luicode = re.findall(r'luicode=(.+?)&',info_url)[0]
                lfid = re.findall(r'lfid=(\d+)',info_url)[0]
                containerid_detail = basic_res['data']['tabsInfo']['tabs'][0]['containerid'] + '_-_INFO'
                detail_params = {
                    'containerid':containerid_detail,
                    'title':'%E5%9F%BA%E6%9C%AC%E4%BF%A1%E6%81%AF',
                    'luicode':luicode,
                    'lfid':lfid,
                    'type':'uid',
                    'value':id
                }
                detail_url = basic_info_url + "?" + urllib.parse.urlencode(detail_params)
                print(detail_url)
                await self.mq.put('info_url',detail_url)

                follow_params = {
                    'containerid': containerid_follow,
                    'luicode': luicode,
                    'lfid': lfid,
                    'type': 'uid',
                    'value': id
                }
                # await asyncio.sleep(random.random())
                max_page = int(int(follow_count) / 20) + 1#最多显示10页内容
                if max_page > 10:
                    max_page = 10
                for page in range(1,max_page):
                    await self.get_user_list(params=follow_params,page=page)

    async def get_user_list(self,params,page):#不能同时搜索多个page!

        params['page'] = str(page)
        follow_res = await self.request_page(url=basic_info_url, params=params, headers=headers,proxy=True)
        try:
            cards = follow_res['data']['cards']
        except Exception as e:
            l.info(e)
            pass
        else:
            if cards:
                if page != 1:
                    follow_ids = cards[0]['card_group']
                    # id_list = [str(i['user']['id']) for i in follow_ids]
                    for i in follow_ids:
                        await self.insert_redis(str(i['user']['id']))
                else:
                    for card in cards:
                        if 'title' in card.keys():
                            # id_list = [str(i['user']['id']) for i in card['card_group']]
                            for i in card['card_group']:
                                await self.insert_redis(str(i['user']['id']))
                            break


    async def insert_redis(self, id):
        l.info("process check id: {}".format(id))
        redis_len = await self.redis.scard('user_id')
        print("length of id is:{}".format(redis_len))
        if redis_len <= 10000:  # 限制id数量，防止内存占满
            # todo:去重
            if await self.bf.isContains(id):  # 判断字符串是否存在
                l.info('{} exists!'.format(id))
            else:
                l.info('{} not exists, insert into redis!'.format(id))
                await self.bf.insert(id)
                await self.redis.sadd('user_id', str(id))

    async def tasks(self):
        self.mq = await AsyncMqSession()
        self.proxy_mq = await AsyncMqSession()
        self.redis = await aioredis.create_redis((DB['redis']['host'], DB['redis']['port']),password=DB['redis']['password'],encoding='utf-8')
        while True:

            tasks = [self.crawler_entry() for _ in range(500)]
            try:
                await asyncio.gather(*tasks)
            except Exception as e:
                l.info(e)

    def worker(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.tasks())
        loop.close()

    def run(self):
        ps = list()
        for i in range(5):
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
    with Url_Producer() as up:
        up.run()

