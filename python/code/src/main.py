import asyncio
import aiopg
import logging
import os

from functools import wraps


CONNECTION_DATA = {
    'dbname': os.environ.get('POSTGRES_DB'),
    'user': os.environ.get('POSTGRES_USER'),
    'password': os.environ.get('POSTGRES_PASSWORD'),
    'host': 'postgres',
}
DSN = ' '.join((f'{k}={v}' for k, v in CONNECTION_DATA.items()))


def with_cursor(method):
    @wraps(method)
    async def wrapper(self, *args, **kwargs):
        async with aiopg.create_pool(DSN) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    self._cur = cur
                    await method(self, *args, **kwargs)
    return wrapper


class MessageHandler:

    def __init__(self, id, message_id):
        self._id = id
        self._message_id = message_id
        self._cur = None

    @with_cursor
    async def start(self):
        await self._reserve_message()
        if self._cur.rowcount == 0:
            return
        await self._send_message()
        await self._complete_message()

    async def _reserve_message(self):
        logging.warning(f'SENDER {self._id} Reserve mesage with id = {self._message_id}')
        await self._cur.execute(
            f'''
            UPDATE test_message
            SET sending = true
            WHERE id = {self._message_id};
            '''
        )

    async def _send_message(self):
        logging.warning(f'SENDER {self._id} Sending mesage with id = {self._message_id}')
        await asyncio.sleep(30)

    async def _complete_message(self):
        logging.warning(f'SENDER {self._id} Complete mesage with id = {self._message_id}')
        await self._cur.execute(
            f'''
            DELETE
            FROM test_message
            WHERE id = {self._message_id};
            '''
        )


class MessagesHandler:

    def __init__(self, app, id):
        self._app = app
        self._id = id
        self._cur = None

    async def start(self):
        while True:
            await self._handle()
            await asyncio.sleep(1)

    async def _handle(self):
        if not self._app.data:
            return
        handlers = (
            MessageHandler(self._id, id).start()
            for id, done, sending in self._app.data
        )
        self._app.data = None
        await asyncio.gather(*handlers)


class MessagesGetter:

    def __init__(self, app):
        self._app = app
        self._cur = None

    @with_cursor
    async def start(self):
        while True:
            await self._handle()
            await asyncio.sleep(1)

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
        self._app.data = await self._cur.fetchall()


class App:

    def __init__(self, handlers_count=5):
        self._handlers_count = handlers_count
        self.data = None

    async def main(self):
        await asyncio.gather(
            MessagesGetter(self).start(),
            *(MessagesHandler(self, i).start() for i in range(self._handlers_count)),
        )


if __name__ == '__main__':
    asyncio.run(App().main())
