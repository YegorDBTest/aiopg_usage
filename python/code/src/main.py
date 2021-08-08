import asyncio
import aiopg
import logging
import os


class MessageHandler:

    def __init__(self, app):
        self._app = app
        self._cur = None

    async def start(self):
        async with aiopg.create_pool(self._app.dsn) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    self._cur = cur
                    while True:
                        await self._handle()
                        await asyncio.sleep(1)


class MessageGetHandler(MessageHandler):
    async def _handle(self):
        await self._cur.execute(
            '''
            SELECT id, done, sending
            FROM test_message
            WHERE done = false AND sending = false;
            '''
        )
        logging.warning(f'GETTER rowcount {self._cur.rowcount}')
        if self._cur.rowcount == 0:
            return
        for id, done, sending in await self._cur.fetchall():
            self._app.data[id] = {
                'done': done,
                'sending': sending,
            }


class MessageSendHandler(MessageHandler):

    def __init__(self, app, id):
        super().__init__(app)
        self._id = id

    async def start(self):
        await asyncio.sleep(self._id / self._app.threads_count)
        await super().start()

    async def _handle(self):
        if not self._app.data:
            return
        id, values = self._app.data.popitem()
        await self._reserve_message(id)
        if self._cur.rowcount == 0:
            return
        await self._send_message(id)
        await self._complete_message(id)

    async def _reserve_message(self, id):
        logging.warning(f'SENDER {self._id} Reserve mesage with id = {id}')
        await self._cur.execute(
            f'''
            UPDATE test_message
            SET sending = true
            WHERE id = {id};
            '''
        )

    async def _send_message(self, id):
        logging.warning(f'SENDER {self._id} Sending mesage with id = {id}')
        await asyncio.sleep(30)

    async def _complete_message(self, id):
        logging.warning(f'SENDER {self._id} Complete mesage with id = {id}')
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
            MessageGetHandler(self).start(),
            *(
                MessageSendHandler(self, i).start()
                for i in range(self.threads_count
            ))
        )


if __name__ == '__main__':
    asyncio.run(App({
        'dbname': os.environ.get('POSTGRES_DB'),
        'user': os.environ.get('POSTGRES_USER'),
        'password': os.environ.get('POSTGRES_PASSWORD'),
        'host': 'postgres',
    }).main())
