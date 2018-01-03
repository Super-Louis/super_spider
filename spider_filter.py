# encoding=utf-8

from spider_queue import Aio_Redis
from hashlib import md5
import asyncio

class SimpleHash(object):
    def __init__(self, cap, seed):
        self.cap = cap
        self.seed = seed

    def hash(self, value):
        ret = 0
        for i in range(len(value)):
            ret += self.seed * ret + ord(value[i])
        return (self.cap - 1) & ret


class BloomFilter(object):
    def __init__(self, blockNum=1, key='bloomfilter'):
        """
        :param host: the host of Redis
        :param port: the port of Redis
        :param db: witch db in Redis
        :param blockNum: one blockNum for about 90,000,000; if you have more strings for filtering, increase it.
        :param key: the key's name in Redis
        """
        self.bit_size = 1 << 31  # Redis的String类型最大容量为512M，现使用256M
        self.seeds = [5, 7, 11, 13, 31, 37, 61]
        self.key = key
        self.blockNum = blockNum
        self.hashfunc = []
        for seed in self.seeds:
            self.hashfunc.append(SimpleHash(self.bit_size, seed))

    async def isContains(self, str_input):
        if not str_input:
            return False
        m5 = md5()
        m5.update((str_input).encode('utf-8'))
        str_input = m5.hexdigest()
        ret = True
        name = self.key + str(int(str_input[0:2], 16) % self.blockNum)
        async with Aio_Redis() as ar:
            for f in self.hashfunc:
                loc = f.hash(str_input)
                ret = ret & await ar.r.getbit(name, loc)
            return ret

    async def insert(self, str_input):
        m5 = md5()
        m5.update((str_input).encode('utf-8'))
        str_input = m5.hexdigest()
        name = self.key + str(int(str_input[0:2], 16) % self.blockNum)
        async with Aio_Redis() as ar:
            for f in self.hashfunc:
                loc = f.hash(str_input)
                await ar.r.setbit(name, loc, 1)

    def run(self,string):
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(self.isContains(string))
        loop.close()
        print(result)


if __name__ == '__main__':

    """ 第一次运行时会显示 not exists!，之后再运行会显示 exists! """

    bf = BloomFilter()
    bf.run('2247657845')
    # if bf.isContains('2247657845'):   # 判断字符串是否存在
    #     print('exists!')
    # else:
    #     print('not exists!')
    #     bf.insert('2247657845')
