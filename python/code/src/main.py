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


async def handle_message(cur, id):
    logging.warning(f'Get mesage with id = {id}')
    await cur.execute(
        f'''
        UPDATE test_message
        SET sending = true
        WHERE id = {id};
        '''
    )
    if cur.rowcount > 0:
        logging.warning(f'Sending mesage with id = {id}')
        await asyncio.sleep(30)
        await cur.execute(
            f'''
            UPDATE test_message
            SET (sending, done) = (false, true)
            WHERE id = {id};
            '''
        )


async def get_data(cur):
    await cur.execute(
        '''
        SELECT id, done, sending
        FROM test_message
        WHERE done = false AND sending = false;
        '''
    )
    logging.warning(f'cur.rowcount {cur.rowcount}')
    if cur.rowcount == 0:
        return
    data = await cur.fetchall()
    for id, done, sending in data:
        await handle_message(cur, id)


async def go(timeout):
    await asyncio.sleep(timeout)
    async with aiopg.create_pool(dsn) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await get_data(cur)


async def main():
    await asyncio.gather(*(go(i) for i in range(100)))


if __name__ == '__main__':
    asyncio.run(main())
