BEGIN TRANSACTION;

/* Создаем и заполняем таблицу с тестом твита */
CREATE TABLE Tweet_text (
  `id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  `text` TEXT,
  `tweet_sentiment` REAL DEFAULT NULL

);
ALTER TABLE Tweet ADD COLUMN tweet_text_id INTEGER DEFAULT NULL;
INSERT INTO Tweet_text (text, tweet_sentiment) SELECT tweet_text, tweet_sentiment FROM Tweet WHERE tweet_text IS NOT NULL GROUP BY tweet_text;
UPDATE Tweet SET tweet_text_id = (SELECT id FROM Tweet_text WHERE text = Tweet.tweet_text);


/* Создаем и заполняем таблицу с кодами стран */
/* Как вариант можно взять готовый список аббревиатура стран */
/* и делать lookup после загрузки его */
CREATE TABLE Tweet_country_code (
  `id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  `code` TEXT
);
ALTER TABLE Tweet ADD COLUMN country_code_id INTEGER DEFAULT NULL;
INSERT INTO Tweet_country_code (code) SELECT country_code FROM Tweet WHERE country_code IS NOT NULL GROUP BY country_code;
UPDATE Tweet SET country_code_id = (SELECT id FROM Tweet_country_code WHERE code = Tweet.country_code);


/* создаем и заполняем таблицу с списком языков */
CREATE TABLE Tweet_lang (
  `id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  `code` TEXT
);
ALTER TABLE Tweet ADD COLUMN lang_id INTEGER DEFAULT NULL;
INSERT INTO Tweet_lang (code) SELECT lang FROM Tweet WHERE lang IS NOT NULL GROUP BY lang;
UPDATE Tweet SET lang_id = (SELECT id FROM Tweet_lang WHERE code = Tweet.lang);


/* Создаем и заполняем таблицу со списком местоположений */
CREATE TABLE Tweet_location (
  `id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  `location` TEXT
);
ALTER TABLE Tweet ADD COLUMN location_id INTEGER DEFAULT NULL;
INSERT INTO Tweet_location (location) SELECT location FROM Tweet WHERE location IS NOT NULL GROUP BY location;
UPDATE Tweet SET location_id = (SELECT id FROM Tweet_location WHERE location = Tweet.location);


/* Удаляем дублирующие колонки из БД */
CREATE TEMPORARY TABLE Tweet_backup (
  `name` TEXT,
  `tweet_text_id` INTEGER,
  `country_code_id` INTEGER DEFAULT NULL,
  `display_url` TEXT,
  `lang_id` INTEGER DEFAULT NULL,
  `location_id` INTEGER DEFAULT NULL,
  `created_at` TIMESTAMP
);

INSERT INTO Tweet_backup
  SELECT name, tweet_text_id, country_code_id, display_url,
    lang_id, location_id, created_at FROM Tweet;

DROP TABLE Tweet;

CREATE TABLE Tweet (
  `name` TEXT,
  `tweet_text_id` INTEGER,
  `country_code_id` INTEGER DEFAULT NULL,
  `display_url` TEXT,
  `lang_id` INTEGER DEFAULT NULL,
  `location_id` INTEGER DEFAULT NULL,
  `created_at` TIMESTAMP
);
CREATE INDEX idx_tweet_text_id ON Tweet (tweet_text_id);
CREATE INDEX idx_country_code_id ON Tweet (country_code_id);
CREATE INDEX idx_lang_id ON Tweet (lang_id);
CREATE INDEX idx_location_id ON Tweet (location_id);

INSERT INTO Tweet SELECT * FROM Tweet_backup;

DROP TABLE Tweet_backup;

COMMIT;