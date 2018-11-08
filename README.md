# Тестовое задание:


#### 1. Написать скрипт на Python, который загружает в БД (sqlite3) данные по каждому твитту из файла “three_minutes_tweets.json.txt”: Таблица Tweet name, tweet_text, country_code, display_url, lang, created_at, location. Все необходимые файлы находятся во вложенном файле Documrnts.zip

- Код загрузчика находится в loader.py. По-умолчанию использует источник твитов по пути ./data/three_minutes_tweets.json.txt и создает базу по пути ./db.sqlite. Логгирование настроено на DEBUG-уровень.
- Запустить можно так: ```python3 ./loader.py```


#### 2. Добавить новую колонку tweet_sentiment

- Добавляем колонку, выполняя запрос "ALTER TABLE Tweet ADD COLUMN tweet_sentiment REAL DEFAULT NULL;"
Выполнить его можно следующим образом: ```echo 'ALTER TABLE Tweet ADD COLUMN tweet_sentiment REAL DEFAULT NULL;' | sqlite3 db.sqlite```

P.S. Поле tweet_sentiment перенесено в таблицу Tweet_text. Один расчет значения sentiment из-за отсутствия дублей в данной таблице. 


#### 3. Подумать как можно провести нормализацию данной таблицы, создать и применить SQL скрипт для нормализации

- Все инструкции для нормализации БД содержаться в файлу normalize.sql (Выносим поля tweet_text, country_code, country_code, location в отдельные таблицы. Оставляем ссылки в таблице Tweet на вынесенные значения в таблицах. Удаляем более ненужные дублирующие колонки в Tweet). Применить изменения можно сл. образом: ```cat normalize.sql | sqlite3 db.sqlite```

#### 4. Написать скрипт на Python для подсчета среднего sentiment (Эмоциональной окраски сообщения) на основе AFINN-111.txt и обновить tweet_sentiment колонку, если слова нет в файле предполагать что sentiment =0 AFINN ReadMe: AFINN is a list of English words rated for valence with an integer between minus five (negative) and plus five (positive). The words have been manually labeled by Finn Årup Nielsen in 2009-2011. The file is tab-separated. There are two versions: AFINN-111: Newest version with 2477 words and phrases.

- Сценарий, реализующий обновление содержится в mark_sentiments_scores.py. По-умолчанию использует базу по пути ./db.sqlite. Запустить подсчет можно следующим образом: ```python3 ./mark_sentiments_scores.py```. Имеем 1632 строк с рассчитанными значениями и 5418 - с нулевыми.

#### 5. Написать 1 SQL скрипт, который выводит наиболее и наименее счастливую страну, локацию и пользователя
```
SELECT (
  SELECT c.code FROM Tweet_country_code c
    JOIN Tweet t ON (t.country_code_id = c.id)
    JOIN Tweet_text tt ON (tt.id = t.tweet_text_id)
    ORDER BY tt.tweet_sentiment DESC LIMIT 1
) AS happy_country,
(
  SELECT c.code FROM Tweet_country_code c
    JOIN Tweet t ON (t.country_code_id = c.id)
    JOIN Tweet_text tt ON (tt.id = t.tweet_text_id)
    ORDER BY tt.tweet_sentiment ASC LIMIT 1
) AS unhappy_country,
(
  SELECT l.location FROM Tweet_location l
    JOIN Tweet t ON (t.location_id = l.id)
    JOIN Tweet_text tt ON (tt.id = t.tweet_text_id)
    ORDER BY tt.tweet_sentiment DESC LIMIT 1
) AS happy_location,
(
  SELECT l.location FROM Tweet_location l
    JOIN Tweet t ON (t.location_id = l.id)
    JOIN Tweet_text tt ON (tt.id = t.tweet_text_id)
    ORDER BY tt.tweet_sentiment ASC LIMIT 1
) AS unhappy_location,
(
  SELECT t.name FROM Tweet t
    JOIN Tweet_text tt ON (tt.id = t.tweet_text_id)
    ORDER BY tt.tweet_sentiment DESC LIMIT 1
) AS happy_user,
(
  SELECT t.name FROM Tweet t
    JOIN Tweet_text tt ON (tt.id = t.tweet_text_id)
    ORDER BY tt.tweet_sentiment ASC LIMIT 1
) AS unhappy_user;
    
```

#### 6. Подумать как можно организовать процесс ежедневной оценки параметров в п. 5 (Production решение) и описать из каких шагов ETL будет состоять процесс трансформации до получения и хранения конечной информации. Первый этап: данные приходят на FTP (tweet.json+AFINN.txt), последний этап: данные отгружаются на другой FTP.

1. Загружаем данные с ФТП
2. Трансформируем данные(очищаем, фильтруем, согласуем, дополняем необходимым отсутствующими данными [например считаем индекс эмоц. окраски сообщения или значениями внешних справочников]
3. Сохраняем необходимые данные в формат для выгрузки
4. Загружаем данные на FTP