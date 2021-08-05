import asyncio
import aiopg
import logging
import os


CONNECTION_DATA = {
    'dbname': os.environ.get('POSTGRES_DB'),
    'user': os.environ.get('POSTGRES_USER'),
    'password': os.environ.get('POSTGRES_PASSWORD'),
    'host': 'postgres',
}
dsn = ' '.join(map(lambda i: f'{i[0]}={i[1]}', CONNECTION_DATA.items()))


async def get_data(cur):
    await cur.execute('SELECT * FROM test;')
    ret = []
    async for row in cur:
        ret.append(row)
    logging.warning(ret)
    await cur.execute('UPDATE test SET done = true WHERE done = false;')
    logging.warning(cur.rowcount)


async def go():
    async with aiopg.create_pool(dsn) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                while True:
                    await get_data(cur)
                    await asyncio.sleep(1)


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
