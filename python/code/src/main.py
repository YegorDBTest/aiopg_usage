import asyncio
import aiopg
import logging
import os


class MessageHandler:

    def __init__(self, app, id):
        self._app = app
        self._id = id
        self._cur = None

    async def start(self):
        await asyncio.sleep(self._id / self._app.threads_count)
        async with aiopg.create_pool(self._app.dsn) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    self._cur = cur
                    while True:
                        await self.get_data()
                        await asyncio.sleep(1)

    async def get_data(self):
        await self._cur.execute(
            '''
            SELECT id, done, sending
            FROM test_message
            WHERE done = false AND sending = false
            ORDER BY id ASC
            LIMIT 1;
            '''
        )
        logging.warning(f'{self._id} rowcount {self._cur.rowcount}')
        if self._cur.rowcount == 0:
            return
        id, done, sending = await self._cur.fetchone()
        await self.handle_message(id)

    async def handle_message(self, id):
        await self._reserve_message(id)
        if self._cur.rowcount > 0:
            await self._send_message(id)
            await self._complete_message(id)

    async def _reserve_message(self, id):
        logging.warning(f'{self._id} Reserve mesage with id = {id}')
        await self._cur.execute(
            f'''
            UPDATE test_message
            SET sending = true
            WHERE id = {id};
            '''
        )

    async def _send_message(self, id):
        logging.warning(f'{self._id} Sending mesage with id = {id}')
        await asyncio.sleep(30)

    async def _complete_message(self, id):
        logging.warning(f'{self._id} Complete mesage with id = {id}')
        await self._cur.execute(
            f'''
            DELETE
            FROM test_message
            WHERE id = {id};
            '''
        )


class App:

    def __init__(self, connection_data, threads_count=5):
        self.dsn = ' '.join((f'{k}={v}' for k, v in connection_data.items()))
        self.threads_count = threads_count
        self.data = {}

    async def main(self):
        await asyncio.gather(
            *(MessageHandler(self, i).start() for i in range(self.threads_count))
        )


if __name__ == '__main__':
    asyncio.run(App({
        'dbname': os.environ.get('POSTGRES_DB'),
        'user': os.environ.get('POSTGRES_USER'),
        'password': os.environ.get('POSTGRES_PASSWORD'),
        'host': 'postgres',
    }).main())
