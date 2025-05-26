import warnings
from itertools import chain
from config import apps, tags, languages
from app_store_web_scraper import AppStoreEntry
from google_play_scraper import Sort, reviews
import re
from pandas import DataFrame
import praw
import os
from dotenv import load_dotenv
from datetime import datetime

# Загрузка .env
load_dotenv()


# Сбор последних отзывов из appstore
def reviews_appstore(app_name: str) -> DataFrame:
    reviews_list = {'id': [], 'rating': [], 'content': [], 'date': [], 'tag': []}

    app_id = apps[app_name].get('ios_id')

    reviews = AppStoreEntry(app_id=app_id, country="us")

    for review in reviews.reviews(limit=500):
        current_text = clean(review.content)
        current_tag = find_tag(current_text)
        if len(current_text) < 300 and current_tag is not None:
            reviews_list['rating'].append(review.rating)
            reviews_list['content'].append(review.content)
            reviews_list['date'].append(review.date)
            reviews_list['id'].append(review.id)
            reviews_list['tag'].append(current_tag)

    return DataFrame(reviews_list)


# Сбор последних отзывов из google-play
def reviews_google(app_name: str) -> DataFrame:
    name = apps[app_name]['gp_id']
    reviews_list = {'id': [], 'rating': [], 'content': [], 'date': [], 'tag': []}

    result, continuation_token = reviews(
        name,
        lang='ru',  # defaults to 'en'
        country='ru',  # defaults to 'us'
        sort=Sort.NEWEST,  # defaults to Sort.NEWEST
        count=5000,  # defaults to 100
        filter_score_with=None  # defaults to None(means all score)
    )

    for review in result:
        current_text = clean(review['content'])
        current_tag = find_tag(current_text)
        if len(current_text) < 300 and current_tag is not None:
            reviews_list['rating'].append(review['score'])
            reviews_list['content'].append(review['content'])
            reviews_list['date'].append(review['at'])
            reviews_list['id'].append(review['reviewId'])
            reviews_list['tag'].append(current_tag)

    return DataFrame(reviews_list)


# Сбор последних отзывов из reddit
def reviews_reddit(subreddit: str, limit: int = 800) -> DataFrame():
    reddit = praw.Reddit(
        client_id=os.getenv('REDDIT_ID'),
        client_secret=os.getenv('REDDIT_SECRET'),
        user_agent=os.getenv('REDDIT_NAME'),
    )

    name = apps[subreddit]['sub']

    if name == 'no':
        warnings.warn(f'subreddit "{subreddit}" не существует')
        return DataFrame()

    subreddit_obj = reddit.subreddit(name)

    # words = chain.from_iterable(tags.values())
    # pattern = re.compile(r'\b(?:' + '|'.join(words) + r')\b', re.IGNORECASE)

    reviews_list = {'id': [], 'rating': [], 'content': [], 'date': [], 'tag': []}
    count = 0
    try:
        for post in subreddit_obj.new(limit=500):
            current_text = clean(post.selftext)
            if len(current_text) < 300 and find_tag(current_text) is not None:
                count += 1
                reviews_list['rating'].append(5)
                reviews_list['content'].append(post.selftext)
                reviews_list['date'].append(datetime.fromtimestamp(post.created_utc))
                reviews_list['id'].append(post.id)
                reviews_list['tag'].append(find_tag(current_text))
    except Exception:
        warnings.warn(f'subreddit "{subreddit}" не существует')
        return DataFrame()

    return DataFrame(reviews_list)


# Убираем лишние пробелы
def clean(txt: str) -> str:
    return re.sub(r"\s+", " ", txt).lower().strip()


# Поиск нужных тегов
def find_tag(text: str) -> str | None:
    clean_text = clean(text)
    words = chain.from_iterable(tags.values())
    pattern = re.compile(r'\b(?:' + '|'.join(words) + r')\b', re.IGNORECASE)
    result = pattern.search(clean_text)

    return result.group() if result is not None else None


def run_pipeline():
    gp = reviews_google()


if __name__ == "__main__":
    # good = 0
    # input_apps = 'duolingo, ling, memrise, mondly, lingodeer, drops, lingualeo, clozemaster, babbel, busuu, rosetta,' \
    #              ' lingvist, reword, duocards, quizlet, anki, habitica, fabulous, forestapp, zombiesrun'.split(', ')
    apps_names = [i for i in apps]
    print('Введите названия приложения', end='\n')
    input_apps = input().split()

    for i in input_apps:
        if i in apps_names:
            # print(reviews_appstore(i).to_string())
            # print('___')
            # print(reviews_google(i).to_string())
            new: DataFrame = reviews_appstore(i)
            print(new.to_string())
        else:
            print(f'Такого приложения нет, выберите из - {", ".join(apps_names)}')
