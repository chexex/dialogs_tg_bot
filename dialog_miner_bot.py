# coding: utf-8
import asyncio
import logging
import os
import time

import aiohttp
import uvloop

from bot_models import DialogsMiner

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
log = logging.getLogger('DialogsMiner')


class TokenError(Exception):
    pass


class PostgresDSNError(Exception):
    pass


async def main():
    async with aiohttp.ClientSession() as session:
        bot = DialogsMiner(session)
        update_id = 0
        while True:
            try:
                time.sleep(2)
                log.info("Get updates from server")
                async with session.get(
                        os.path.join(bot.BOT_URL, 'getUpdates'),
                        params={'offset': update_id and update_id + 1},
                ) as resp:
                    res = await resp.json()

                res = res.get('result')
                log.info(f'Got {len(res)} new messages')
                for m in res:
                    update_id = m['update_id']
                    await bot.process_message(m)
            except Exception:
                log.exception(f'<{"-=-=-"*3}>')


if __name__ == '__main__':
    if not os.getenv('TG_API_TOKEN'):
        raise TokenError('Telegram token didn\'t provide. '
                         'You should specify it in environment: '
                         'export TG_API_TOKEN=___your_tg_token___')
    if not (os.getenv("TG_DBNAME") and
            os.getenv("TG_DBUSER") and
            os.getenv("TG_DBHOST") and
            os.getenv("TG_DBPASSWORD") is not None):
        raise PostgresDSNError('Postgres variables didn\'t provide. '
                               'You should specify it in environment: '
                               'export TG_DBNAME=___dbname___'
                               'export TG_DBUSER=___dbuser___'
                               'export TG_DBHOST=___dbhost___'
                               'export TG_DBPASSWORD=___dbpassword___')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
