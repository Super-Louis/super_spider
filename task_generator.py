from spider_queue import *
import asyncio
import aioredis

init_id_list = ['1195354434','1801055875','1197369013','1842357963'
    ,'1234872834','1752164320','2812335943','1838993640','2825986082','2818965582']

async def gen_tasks():
    async with Aio_Redis() as ar:
        for i in init_id_list:
            # await ar.r.sadd('user_id', i)
            await ar.r.sadd('test', i)



if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(gen_tasks())
    loop.close()


