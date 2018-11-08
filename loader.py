#!/usr/bin/env python3

import collections.abc
import json
import logging
import logging.config
import os
import sqlite3
from datetime import datetime


def parse_args():
    """
        Парсинг аргументов, переданных в командной строке

        :return: argparse.Namespace
    """
    import argparse

    parser = argparse.ArgumentParser(
        description=''
    )

    parser.add_argument('--source', type=str, dest='source',
                        default='./data/three_minutes_tweets.json.txt',
                        help='Path to file with tweets-data')
    parser.add_argument('--dbfile', type=str, dest='dbfile',
                        default='./db.sqlite',
                        help='Path to sqlite database')
    parser.add_argument('--to_table', type=str, dest='to_table',
                        default='Tweet', help='Table name for save')

    return parser.parse_args()


def setup_logging(base_severity=logging.DEBUG):
    """
        Настройка логгирования
    """
    logging_config = {
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'base': {
                'format': '%(asctime)s %(levelname)s %(filename)s %(message)s',
            },
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'base',
            },
        },
        'loggers': {
            '': {
                'level': 'DEBUG',
                'handlers': ['console'],
                'propagate': False,
            }
        },
        'root': {
            'level': 'ERROR',
            'handlers': ['console'],
        }
    }
    logging.config.dictConfig(logging_config)

    my_logger = logging.getLogger(__name__)
    my_logger.setLevel(base_severity)

    return my_logger


logger = setup_logging(logging.DEBUG)


class JSONSource(collections.abc.Iterator):
    """
        Данный класс является источником сообщений
          на базе списка сообщений в формате JSON
    """
    def __init__(self, filepath):
        """
            Конструктор класса

            :param filepath: путь к файлу с загружаемыми данными
            :return: None
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(filepath)
        self._filepath = filepath

    def __iter__(self):
        self._data = open(self._filepath, 'r')
        return self

    def __next__(self):
        for line in self._data:
            if line is not None:
                return json.loads(line)
        raise StopIteration


class Loader:
    """
        Данный класс осуществляет загрузку
        данных в указанное хранилище
    """
    def __init__(self, store):
        """
            Конструктор класса загрузчика

            :param object store: объект
        """
        self._store = store

    def load(self, source, to_table):
        """
            Метод загружает из источника в хранилище

            :param object source: источник данных
            :param str to_table: имя таблицы для сохранения

            :return: None
        """
        logger.info('Start loading tweets')

        for item in source:
            logger.debug('Prepare item: %s', item)
            prepared_item = self._prepare_item(item)
            if prepared_item:
                logger.info('Save tweet to db')
                self._store.save(to_table, prepared_item)

    @staticmethod
    def _prepare_item(item):
        """
            Подготавливает данные для загрузки

            :param dict item:
            :return dict prepared_item:
        """
        if 'user' not in item or 'text' not in item:
            logger.debug('Invalid tweet format. Skip')
            return

        display_url = 'http://twitter.com/{0}/status/{1}'.format(
            item['user']['id'], item['id'])

        created_at = datetime.strptime(item['created_at'],
                                       '%a %b %d %H:%M:%S %z %Y')

        country_code = item['place'].get('country_code') \
            if isinstance(item['place'], dict) else None

        prepared_item = {
            'tweet_text': item['text'],
            'name': item['user'].get('name'),
            'country_code': country_code,
            'display_url': display_url,
            'lang': item['user'].get('lang'),
            'location': item['user'].get('location'),
            'created_at': created_at
        }
        return prepared_item


class SQLiteStore:
    """
        Данный класс представляет собой хранилище-обертку
          вокруг SQLite для сохранения сообщений
    """
    CREATE_TWEET_TABLE_SQL = """CREATE TABLE IF NOT EXISTS Tweet (
                                  name TEXT,
                                  tweet_text TEXT,
                                  country_code TEXT DEFAULT NULL,
                                  display_url TEXT,
                                  lang TEXT DEFAULT NULL,
                                  location TEXT DEFAULT NULL,
                                  created_at TIMESTAMP
                            );"""

    def __init__(self, filepath):
        """
            Конструктор класса. Инициализирует соединение с БД

            :param str filepath: Путь к файлу с БД
        """
        self._conn = sqlite3.connect(filepath,
                                     detect_types=sqlite3.PARSE_DECLTYPES)
        if self._conn is not None:
            self._create_table(self.CREATE_TWEET_TABLE_SQL)

    def _create_table(self, create_table_sql):
        """
            Метод создает таблицу по заданному SQL-выражению

            :param str create_table_sql:
            :return: None
        """
        try:
            c = self._conn.cursor()
            c.execute(create_table_sql)
        except sqlite3.Error as e:
            logger.error('Create table error: %s', e)

    def save(self, table, row):
        """
            Метод сохраняет данные в заданную таблицу

            :param str table: имя таблицы
            :param dict row: данные для сохранения

            :return: None
        """
        logger.debug('Data for save: %s', row)

        insert_sql = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(
            table,
            ', '.join('"' + k + '"' for k in row.keys()),
            ','.join('?'*len(row))
        )
        try:
            logger.debug('Insert tweet %s in db', row)

            c = self._conn.cursor()
            c.execute(insert_sql, list(row.values()))
            self._conn.commit()
        except Exception as e:
            logger.error('Insert tweet error: %s', e)


if __name__ == '__main__':
    args = parse_args()

    source = JSONSource(filepath=args.source)
    store = SQLiteStore(filepath=args.dbfile)

    loader = Loader(store)
    loader.load(source, to_table=args.to_table)
