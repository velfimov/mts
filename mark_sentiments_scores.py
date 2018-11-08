#!/usr/bin/env python3

import math
import sqlite3


class SentimentAnalyzer:
    """
        Данный класс реализует расчет значения
          эмоциональной окраски сообщения
    """
    DEFAULT_DICTIONARY = 'AFINN-111.txt'

    def __init__(self, dictionary=None):
        """
            Конструктор класса

            :param str dictionary: путь к файлу загружаемого словаря
            :return: None
        """
        if dictionary is None:
            dictionary = self.DEFAULT_DICTIONARY
        self._dict = self._load_dictionary(dictionary)

    @staticmethod
    def _load_dictionary(dictionary):
        """
            Метод осуществляет парсинг файла со словарем
              и возвращает словарь слов

            :param str dictionary: путь к файлу загружаемого словаря
            :return dict dct: словарь слов
        """
        dct = {}
        for f in open(dictionary):
            word, score = f.strip().split('\t')
            try:
                dct[word] = int(score)
            except ValueError:
                pass
        return dct

    def score(self, text):
        """
            Метод рассчитывает sentiment score

            :param str text: текст для расчета
            :return int, float total_score: значение sentiment анализа
        """
        words = self._prepare_words(text)
        scores = list(map(lambda word: self._dict.get(word, 0), words))

        if scores:
            total_score = float(sum(scores)) / math.sqrt(len(scores))
        else:
            total_score = 0

        return total_score

    @staticmethod
    def _prepare_words(text):
        """
            Метод разбивает текст и очищает слова от незначащих

            :param str text: текст для разбиения
            :return list words: список слов
        """
        text = text.lower()

        words = []
        for w in text.split():
            if w.startswith('@') or w.startswith('http'):
                continue
            words.append(w)
        return words


def parse_args():
    """
        Парсинг аргументов, переданных в командной строке

        :return: argparse.Namespace
    """
    import argparse

    parser = argparse.ArgumentParser(
        description='Calculate sentiment score for string'
    )

    parser.add_argument('--dbfile', type=str, dest='dbfile',
                        default='./db.sqlite',
                        help='Path to sqlite database')

    return parser.parse_args()


def update_sentiments(db_file):
    """
        Реализует обновление значения tweet_sentiment в указаной БД

        :param str db_file: путь к файлу БД
        :return: None
    """
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    analyzer = SentimentAnalyzer(dictionary='./data/AFINN-111.txt')

    select_all_rows_sql = 'SELECT id, text FROM Tweet_text'
    cursor.execute(select_all_rows_sql)
    for row in cursor.fetchall():
        score = analyzer.score(row[1])

        update_sql = 'UPDATE Tweet_text SET tweet_sentiment = ? WHERE id = ?'
        cursor.execute(update_sql, (score, row[0]))

    conn.commit()
    conn.close()


if __name__ == '__main__':
    args = parse_args()
    update_sentiments(db_file=args.dbfile)
