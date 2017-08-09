# coding: utf-8
import json
import os
from collections import defaultdict
from typing import Dict, List

import aiohttp
import aiopg


class BaseBot(object):
    dsn = f'dbname={os.getenv("TG_DBNAME")} ' \
          f'user={os.getenv("TG_DBUSER")} ' \
          f'host={os.getenv("TG_DBHOST")} ' \
          f'password={os.getenv("TG_DBPASSWORD")}'
    TG_SEND_MESSAGE = 'sendMessage'
    TG_SEND_STICKER = 'sendSticker'
    API_TOKEN = os.getenv('TG_API_TOKEN')
    BOT_URL = f'https://api.telegram.org/bot{API_TOKEN}'

    def __init__(self, session: aiohttp.ClientSession):
        self.history = {}
        self.session = session

    async def process_message(self, message: dict):
        raise NotImplemented

    async def send_message(self, chat_id: str, text: str) -> None:
        data = {
            'text': text,
            'chat_id': chat_id,
        }
        await self.post(self.TG_SEND_MESSAGE, data=data)

    async def get(self, action: str, data: Dict[str, str]) -> dict:
        async with self.session.get(
                os.path.join(self.BOT_URL, action),
                params=data,
        ) as resp:
            res = await resp.json()
        return res

    async def post(self, action: str, data: dict) -> None:
        async with self.session.post(
                os.path.join(self.BOT_URL, action),
                json=data,
                headers={'Content-Type': 'application/json'},
        ) as resp:
            await resp.json()


class DialogsMiner(BaseBot):
    """
    Dialogs fetcher.
    """
    def __init__(self, session):
        super(DialogsMiner, self).__init__(session)
        self.history = defaultdict(list)
        self.table = 'dialogs_data_set'
        self.keyboard_text = 'От души, надави на /commit ' \
                             'или продолжи присылать сопливые диаложики'
        self.keyboard = [['/commit']]

    async def _process_text(self, text: str):
        dialog = text.split('\n')
        need_to_process = len(dialog) == 1
        return dialog, need_to_process

    async def _start_convesation(self, chat_id: str) -> None:
        await self.send_message(
            chat_id,
            text='От души братиш, подзалей мне самых сопливых диаложиков.'
                 '\nЯ туповат, поэтому могу принять диалоги в формате:\n'
                 '— Я не очень хороша, правда?\n'
                 '— Ты идеальна!\n'
                 'Ну, то есть каждая реплика на новой строке.'
        )

    async def process_message(self, message: dict) -> None:
        msg_data = message['message']
        chat_id = msg_data['chat']['id']
        sender_id = msg_data['from']['id']
        sender_name = msg_data['from']['username']
        text = msg_data.get('text')
        sender_history = self.history.get(sender_id, {})

        if text.startswith('/start'):
            await self._start_convesation(chat_id)

        elif text.startswith('/commit'):
            data = [i for i in sender_history if i.get('dialog')]
            if data:
                await self.write_to_db(data)
                self.history[sender_id] = []
            else:
                await self.send_message(
                    chat_id,
                    text='Надо бы сначала диалогов подкинуть сопливых.',
                )
        elif text.startswith('/stat'):
            num_rows_query = f'SELECT COUNT(*) FROM {self.table};'
            users_query = 'SELECT sender_name, COUNT(dialog) ' \
                          f'FROM {self.table} ' \
                          f'GROUP BY sender_name;'
            num_rows = await self.read_from_db(num_rows_query)
            users = await self.read_from_db(users_query)
            user_stat = '\n'.join(
                f'{user.strip()}: {num_dialogs}'
                for user, num_dialogs in users
            )
            text = f'Всего диалгов: {num_rows[0]}\n' \
                   f'Вклад:\n' \
                   f'{user_stat}'

            await self.send_message(chat_id, text)

        elif text:
            dialog, need_to_process = await self._process_text(text)
            self.history[sender_id].append({
                'sender_name': sender_name,
                'sender_id': sender_id,
                'chat_id': chat_id,
                'dialog': dialog,
                'need_to_process': need_to_process,
            })
            await self.send_keyboard_reply(
                chat_id,
                text=self.keyboard_text,
                keyboard=self.keyboard,
            )

    async def send_keyboard_reply(self,
                                  chat_id: str,
                                  text: str,
                                  keyboard: List[List[str]]
                                  ) -> None:
        data = {
            'text': text,
            'chat_id': chat_id,
            'reply_markup': {
                'keyboard': keyboard,
                'resize_keyboard': True,
                'one_time_keyboard': True,
            },
        }
        await self.post(self.TG_SEND_MESSAGE, data=data)

    async def write_to_db(self,
                          data: list,
                          extra: dict = None) -> None:

        extra = extra or {}
        values = ','.join(
            f"({i['sender_id']},"
            f" \'{i['sender_name']}\',"
            f" \'{json.dumps(i['dialog'])}\',"
            f" {1 if i['need_to_process'] else 0},"
            f" \'{extra}\')"
            for i in data
        )
        async with aiopg.create_pool(self.dsn) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO "
                        f"{self.table} "
                        "(sender_id, sender_name, dialog, need_to_process, extra) "
                        "VALUES "
                        f"{values};"
                    )

    async def read_from_db(self, query: str) -> list:
        async with aiopg.create_pool(self.dsn) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query)
                    ret = []
                    [ret.append(row) async for row in cur]
        return ret
