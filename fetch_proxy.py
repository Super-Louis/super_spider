from spider_queue import *
import logging
import requests
import aiohttp
import asyncio
import time

l = logging.getLogger('run')

class CrawlerProxy():

    def __init__(self,debug=False):
        self._session = None
        self.debug = debug
        self.count = 0


    @property
    def session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def get_proxies(self):
        self.mq = await AsyncMqSession()
        while True:
            size = await self.queue_size()
            if size < 1000:
                l.info("Proxies are not enough. Get and check proxies...")
                proxy_list = []
                try:
                    res = requests.get("http://80161325451205093.standard.hutoudaili.com/"
                                   "?num=8000&area_type=1&scheme=1&anonymity=0&order=1")
                    for p in res.text.split("\n"):
                        proxy_ip = "http://" + p.strip()
                        # proxy_ips = "https://" + p.strip()
                        proxy = proxy_ip
                        if self.debug:
                            l.info("get proxy:{}".format(proxy))
                        proxy_list.append(proxy)
                    tasks = [self.check_proxy(p) for p in proxy_list]
                    await asyncio.gather(*tasks)
                except Exception as e:
                    l.info(e)
                    pass
            else:
                l.info("Proxies are enough. Get proxy directly...")
                await asyncio.sleep(5)
                pass

    async def check_proxy(self,ip):
        url = 'http://myip.ipip.net/'
        try:
            res = await self.session.get(url, proxy=ip, timeout=1)
            response = await res.text()
            if '阿里云' not in response:
                # self.l.info(response)
                self.count += 1
                l.info("{} is ok".format(ip))
                l.info("count {}".format(self.count))
                await self.mq.put(queue='proxy_queue', body=ip)

        except Exception as e:
            l.info("{} is not valid: {}".format(ip,e))

    async def queue_size(self):
        try:
            queue = self.mq.session.channel.queue_declare(queue='proxy_queue', durable=True, passive=True)
            queue_size = queue.method.message_count
            l.info("queue size is {}".format(queue_size))
            return queue_size
        except Exception as e:
            l.info(e)
            return 0

    def run(self):
        # 如果代理数量小于200,则重新获取代理
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.get_proxies())
        loop.close()



if __name__ == '__main__':
    # mq = MqSession()
    cp = CrawlerProxy(debug=False)
    cp.run()