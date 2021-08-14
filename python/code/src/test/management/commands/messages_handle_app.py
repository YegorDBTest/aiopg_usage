import aiohttp
import aiopg
import asyncio
import logging

from functools import wraps

from django.conf import settings
from django.core.management.base import BaseCommand


def with_cursor(method):
    @wraps(method)
    async def wrapper(self, *args, **kwargs):
        async with aiopg.create_pool(settings.DSN) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    self._cur = cur
                    await method(self, *args, **kwargs)
    return wrapper


class MessageHandler:

    def __init__(self, app, id, message_id):
        self._app = app
        self._id = id
        self._message_id = message_id
        self._cur = None

    @with_cursor
    async def start(self):
        await self._send_message()

    async def _send_message(self):
        url = f'http://0.0.0.0:8000/send_message/{self._message_id}/'
        logging.warning(f'SENDER {self._id} Sending mesage with id = {self._message_id}')
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                logging.warning(
                    f'SENDER {self._id} Response status'
                    f'mesage with id = {self._message_id} is {resp.status}')
                if resp.status == 200:
                    await self._complete_message()

    async def _complete_message(self):
        logging.warning(f'SENDER {self._id} Completing mesage with id = {self._message_id}')
        await self._cur.execute(
            f'''
            DELETE
            FROM test_message
            WHERE id = {self._message_id};
            '''
        )
        self._app.done_message(self._message_id)
        logging.warning(f'SENDER {self._id} Complete mesage with id = {self._message_id}')


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
        logging.warning(f'HANDLER {self._id} Handle {self._app.data}')
        handlers = (
            MessageHandler(self._app, self._id, d[0]).start()
            for d in self._app.data
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
        logging.warning(f'data {self._app.data}, fetching {self._app.fetching}, processing_ids {self._app.processing_ids}')
        if self._app.fetching or self._app.data:
            return
        await self._cur.execute(
            f"""
            SELECT id
            FROM test_message
            WHERE NOT (id = ANY ('{self._app.processing_ids or '{}'}'));
            """
        )
        logging.warning(f'GETTER rowcount {self._cur.rowcount}')
        if self._cur.rowcount == 0:
            return
        self._app.fetching = True
        self._app.set_data(await self._cur.fetchall())
        self._app.fetching = False


class App:

    def __init__(self, handlers_count=None):
        self._handlers_count = handlers_count or settings.MESSAGE_HANDLERS_COUNT
        self.data = None
        self.fetching = False
        self.processing_ids = set()

    async def main(self):
        await asyncio.gather(
            MessagesGetter(self).start(),
            *(MessagesHandler(self, i).start() for i in range(self._handlers_count)),
        )

    def set_data(self, data):
        self.data = data
        self.processing_ids |= {d[0] for d in data}

    def done_message(self, id):
        self.processing_ids -= {id}


class Command(BaseCommand):

    def handle(self, *args, **options):
        asyncio.run(App().main())
