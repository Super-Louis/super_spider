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
            if size < 3000:
                l.info("Proxies are not enough. Get and check proxies...")
                proxy_list = []
                try:
                    res = requests.get("http://eindex:proxy@proxy.eindex.me")
                    for p in res.text.split("\n"):
                        proxy_ip = "http://" + p.strip()
                        # proxy_ips = "https://" + p.strip()
                        proxy = proxy_ip

                        await self.mq.put(queue='proxy_queue', body=proxy)
                except Exception as e:
                    l.info(e)
                    pass
            else:
                l.info("Proxies are enough. Get proxy directly...")
                await asyncio.sleep(5)
                pass

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

