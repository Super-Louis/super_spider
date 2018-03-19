import json
import logging
from pprint import pprint
import asyncio
from concurrent.futures import ThreadPoolExecutor
import aioredis
import pika
from db_config import DB


class MqSession(object):
    def __init__(self):
        self.channel = self._init_session()

    def _init_session(self):
        tries = 0
        max_tries = 3
        while tries < max_tries:
            tries += 1
            try:
                self.credentials = pika.PlainCredentials(username=DB['rabbit']['user'], password=DB['rabbit']['password'])
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=DB['rabbit']['host'],port=DB['rabbit']['port'],credentials=self.credentials, socket_timeout=20, heartbeat_interval=0))
                self.channel = self.connection.channel()
                self.channel.basic_qos(prefetch_count=10)
                # 如果不设置，会一次释放所有message,但是只有正常ack的会被删除
                # 如果设为1，则在consumer ack之前不会再向该consumer发送message；
                # 设为n，则一次释放n条message，同样只有正常ack的会被删除
                return self.channel
            except Exception as e:
                logging.warning('', exc_info=True, stack_info=True)
                continue

    def put(self, queue, body):
        try:
            self.channel.basic_publish(exchange='', routing_key=queue, body=body,
                                       properties=pika.BasicProperties(delivery_mode=2,))
        except Exception as e:
            logging.warning('', exc_info=True, stack_info=True)
            return -1

    def get(self, queue):
        try:
            for method_frame, properties, body in self.channel.consume(queue):
                # self.ack(method_frame.delivery_tag)
                return method_frame.delivery_tag, body
        except Exception as e:
            logging.warning('', exc_info=True, stack_info=True)
            return None

    def ack(self, delivery_tag):
        self.channel.basic_ack(delivery_tag)

    def nack(self, delivery_tag, multiple=False, requeue=True):
        self.channel.basic_nack(delivery_tag, multiple=multiple, requeue=requeue)

    def close(self):
        self.connection.close()


class AsyncMqSession:
    """
    This class is NOT thread-safe
    # init
    session = await AsyncMqSession()
    # get
    tag, body = await session.get('name')
    # ack
    await session.ack(tag)
    # put
    await session.put('name', 'body')
    # close
    await session.close()

    # with
    async with (await AsyncMqSession()) as session:
        await session.put('name', 'body')
    """
    async def __new__(cls):
        self = super().__new__(cls)
        await self.__init__()
        return self

    async def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=1)  # pika is NOT thread-safe
        self.loop = asyncio.get_event_loop()
        self.session = await self.loop.run_in_executor(self.executor, self._init_session)  # type: MqSession

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self.close()
        except:
            logging.warning('', exc_info=True, stack_info=True)

    @staticmethod
    def _init_session():
        session = MqSession()
        return session

    async def put(self, queue, body):
        return await self.loop.run_in_executor(self.executor, self.session.put, queue, body)

    async def get(self, queue):
        return await self.loop.run_in_executor(self.executor, self.session.get, queue)

    async def ack(self, tag):
        return await self.loop.run_in_executor(self.executor, self.session.ack, tag)

    async def nack(self, tag, multiple=False, requeue=True):
        return await self.loop.run_in_executor(self.executor, self.session.nack, tag, multiple, requeue)

    async def close(self):
        await self.loop.run_in_executor(self.executor, self.session.close)

class Aio_Redis():

    async def __aenter__(self):
        self.r = await aioredis.create_redis((DB['redis']['host'], DB['redis']['port']),password=DB['redis']['password'],encoding='utf-8')
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.r.close()
        await self.r.wait_closed()


class testQueue():

    async def get_message(self):
        t, m = await self.mq.get('hello')
        m = m.decode('utf-8')
        print("get message:{},tag:{}".format(m, t))
        await self.doSomething(m, t)

    async def doSomething(self,message, tag):
        await asyncio.sleep(5)
        await self.mq.nack(tag)
        print('nack:{}, message:{}'.format(tag, message))


    async def tasks(self):
        self.mq = await AsyncMqSession()
        tasks = [self.get_message() for _ in range(5)]
        await asyncio.gather(*tasks)

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.tasks())
        loop.close()

tq = testQueue()
tq.run()


def test_async():
    async def main():
        body = '1pe0sa12'
        async with (await AsyncMqSession()) as session:
            await session.put('subscribe-queue', body)
            print('put')
            _tag, _body = await session.get('subscribe-queue')
            print('get', _body)
            await session.ack(_tag)
            print('ack')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())