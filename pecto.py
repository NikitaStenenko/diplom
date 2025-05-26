#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
collect_reviews.py
Сбор отзывов → тегирование геймификационных механик → parquet + CSV
"""

import os, re, time, warnings, argparse
from pathlib import Path
from itertools import chain
from datetime import datetime, timezone

import pandas as pd
import requests
from google_play_scraper import reviews, Sort
from app_store_scraper import AppStore
from apify_client import ApifyClient

try:
    import praw
except ModuleNotFoundError:
    praw = None
    warnings.warn("⚠️  praw не найден → Reddit будет пропущен")

# Настройки
OUT_DIR = Path("data")
OUT_DIR.mkdir(exist_ok=True)
YEAR_FROM = 2023
GP_BATCH = 5000
RSS_PAGES = 15
_STOREFRONTS = ("ru", "us", "gb", "de", "fr")

TAGS = {  # ключевые слова для тегирования
    "streak": ["streak", "серия", "огонь", "streaks", "стрик"],
    "xp": ["xp", "points", "очки", "опыт", "балл"],
    "leaderboard": ["leaderboard", "rating", "league", "лига", "рейтинг"],
    "badge": ["badge", "achievement", "ачив", "бейдж", "достиж"],
    "mascot": ["owl", "сова", "duo", "талис"],
    "notification": ["push", "notification", "remind", "уведом"],
}

# ----------- СЛОВАРЬ ПРИЛОЖЕНИЙ (и их id) ------------------------------------
APPS = {
    # языковые
    "duolingo": dict(
        gp_id="com.duolingo", ios_slug="duolingo", ios_id=570060128, sub="duolingo"
    ),
    "ling": dict(
        gp_id="simya.app.ling",
        ios_slug="ling-learn-languages",
        ios_id=1416518883,
        sub="LingApp",
    ),
    "memrise": dict(
        gp_id="com.memrise.android.memrisecompanion",
        ios_slug="memrise-easy-language",
        ios_id=635966718,
        sub="memrise",
    ),
    "mondly": dict(
        gp_id="com.atistudios.italk",
        ios_slug="mondly-learn-35-languages",
        ios_id=987873536,
        sub="mondly",
    ),
    "lingodeer": dict(
        gp_id="com.lingodeer", ios_slug="lingodeer", ios_id=1261193709, sub="lingodeer"
    ),
    "drops": dict(
        gp_id="com.languagedrops.drops.international",
        ios_slug="drops-language-learning",
        ios_id=939540371,
        sub="languagedrops",
    ),
    "lingualeo": dict(
        gp_id="com.lingualeo.android",
        ios_slug="lingualeo-learning-languages",
        ios_id=435316152,
        sub="lingualeo",
    ),
    "clozemaster": dict(
        gp_id="com.clozemaster",
        ios_slug="clozemaster",
        ios_id=1302625842,
        sub="Clozemaster",
    ),
    "babbel": dict(
        gp_id="com.babbel.mobile.android.en",
        ios_slug="babbel-learn-languages",
        ios_id=829587759,
        sub="babbel",
    ),
    "busuu": dict(
        gp_id="com.busuu.android.enc",
        ios_slug="busuu-language-learning",
        ios_id=379968583,
        sub="busuu",
    ),
    "rosetta": dict(
        gp_id="air.com.rosettastone.mobile.CoursePlayer",
        ios_slug="rosetta-stone-learn-languages",
        ios_id=435588892,
        sub="RosettaStone",
    ),
    "lingvist": dict(
        gp_id="com.lingvist.android",
        ios_slug="lingvist-language-learning",
        ios_id=653427237,
        sub="lingvist",
    ),
    "reword": dict(
        gp_id="com.dlgames.ReWord",
        ios_slug="reword-learn-vocabulary",
        ios_id=1453624936,
        sub="ReWord",
    ),
    "duocards": dict(
        gp_id="com.duocards",
        ios_slug="duocards-flashcards",
        ios_id=1525883680,
        sub="Duocards",
    ),
    "quizlet": dict(
        gp_id="com.quizlet.quizletandroid",
        ios_slug="quizlet-learn-with-flashcards",
        ios_id=546473125,
        sub="quizlet",
    ),
    "anki": dict(
        gp_id="com.ichi2.anki",
        ios_slug="anki-mobile-flashcards",
        ios_id=373493387,
        sub="Anki",
    ),
    # нейронные / привычки
    "habitica": dict(
        gp_id="com.habitrpg.android.habitica",
        ios_slug="habitica-rpg-task-manager",
        ios_id=994882113,
        sub="habitica",
    ),
    "fabulous": dict(
        gp_id="co.thefabulous.app",
        ios_slug="fabulous-daily-habit-tracker",
        ios_id=1203637303,
        sub="fabulous",
    ),
    "forest": dict(
        gp_id="cc.forestapp",
        ios_slug="forest-stay-focused",
        ios_id=866450515,
        sub="ForestApp",
    ),
    "zombiesrun": dict(
        gp_id="com.sixtostart.zombiesrunclient",
        ios_slug="zombies-run",
        ios_id=503519713,
        sub="zombiesrun",
    ),
    # агрегатор
    "all": {},
}

from dotenv import load_dotenv

load_dotenv()
raw = os.getenv("APIFY_API_TOKEN", "")
# отрезаем возможный префикс
if raw.startswith("apify_api_"):
    raw = raw.split("_", 2)[2].strip()

# принимаем и классический 32‑символьный, и PAT из консоли Apify (36‑символов)
if len(raw) in (32, 36):
    os.environ["APIFY_API_TOKEN"] = raw
    print(f"DEBUG  используем APIFY_API_TOKEN: {raw} (len={len(raw)})")
else:
    print(f"DEBUG  APIFY_API_TOKEN некорректен: '{raw}' (len={len(raw)})")


###############################################################################
# 2. Google Play
###############################################################################
def fetch_google_play(pkg: str, max_count: int = GP_BATCH) -> pd.DataFrame:
    if not pkg:
        return pd.DataFrame()
    raw, _ = reviews(pkg, lang="ru", country="ru", sort=Sort.NEWEST, count=max_count)
    if not raw:
        return pd.DataFrame()

    df = (
        pd.DataFrame(raw)
        .rename(
            columns={
                "reviewId": "id",
                "score": "rating",
                "content": "text",
                "at": "date",
            }
        )
        .assign(source="googleplay")
    )
    return df[["id", "rating", "text", "date", "source"]]


###############################################################################
# 3.  App Store → lib → RSS → Apify fallback
###############################################################################
def _fetch_appstore_lib(slug: str, app_id: int, cc: str, how_many: int) -> pd.DataFrame:
    try:
        app = AppStore(country=cc, app_name=slug, app_id=app_id)
        app.review(how_many=how_many)
        if not app.reviews:
            return pd.DataFrame()
        rows = [(r["id"], r["rating"], r["review"], r["date"]) for r in app.reviews]
        df = pd.DataFrame(rows, columns=["id", "rating", "text", "date"])
        df["date"] = pd.to_datetime(df["date"])
        df["source"] = f"appstore_{cc}"
        return df
    except Exception as e:
        warnings.warn(f"App Store lib ({cc}) error: {e}")
        return pd.DataFrame()


def _fetch_appstore_rss(app_id: int, pages: int, cc: str) -> pd.DataFrame:
    rec = []
    for p in range(1, pages+1):
        url = (
            f"https://itunes.apple.com/{cc}/rss/customerreviews/"
            f"page={p}/id={app_id}/sortby=mostrecent/json"
        )
        try:
            data = requests.get(url, timeout=30).json()
            entry = data.get("feed", {}).get("entry", [])
        except Exception:
            break

        if isinstance(entry, dict):
            feed = [entry]
        else:
            feed = entry

        # Первый элемент — заголовок ленты, дальше отзывы
        if len(feed) < 2:
            break

        for it in feed[1:]:
            rec.append((
                it["id"]["label"],
                int(it["im:rating"]["label"]),
                it["content"]["label"],
                it["updated"]["label"][:10]
            ))

        time.sleep(0.5)

    if not rec:
        return pd.DataFrame()

    df = pd.DataFrame(rec, columns=["id","rating","text","date"])
    df["date"]   = pd.to_datetime(df["date"])
    df["source"] = f"appstore_{cc}"
    return df


def _fetch_appstore_apify(app_id: int, cc: str, limit: int = 750) -> pd.DataFrame:
    token = os.getenv("APIFY_API_TOKEN", "")
    print(f"DEBUG  [Apify] токен перед использованием: '{token}' (len={len(token)})")
    # если токен есть и имеет длину 32 или 36 — пробуем Apify,
    # иначе тихо пропускаем
    if len(token) not in (32, 36):
        print(f"DEBUG  пропускаем Apify для {cc}: некорректный токен")
        return pd.DataFrame()

    client = ApifyClient(token)
    try:
        run = client.actor("apify/apple-app-reviews-scraper").call(
            run_input={
                "appId": str(app_id),
                "country": cc,
                "resultsPerPage": 50,
                "maxReviews": limit,
                "sort": "mostRecent",
            },
            timeout_secs=1200,
        )
        items = client.dataset(run["defaultDatasetId"]).list_items().items
    except Exception as e:
        print(f"WARNING [Apify] error for {cc}: {e}")
        return pd.DataFrame()

    rows = [(it["id"], it["rating"], it["reviewText"], it["date"]) for it in items]
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["id", "rating", "text", "date"])
    df["date"] = pd.to_datetime(df["date"])
    df["source"] = f"appstore_{cc}"
    return df


def fetch_appstore(
    slug: str, app_id: int, pages: int = RSS_PAGES, storefronts=_STOREFRONTS
) -> pd.DataFrame:
    """
    Cобираем отзывы витрина-за-витриной:
        1. lib → 2. RSS → 3. Apify
    Всё, что удалось достать, складываем в список, в конце склеиваем.
    """
    dfs = []  # ← здесь накапливаем

    # ◉ 1. lib
    print("DEBUG ▶ lib phase")
    for cc in storefronts:
        d = _fetch_appstore_lib(slug, app_id, cc, how_many=pages * 50)
        print(f"lib {cc}: {len(d)} rows")
        if not d.empty:
            dfs.append(d)

    # ◉ 2. RSS  (запускаем ТОЛЬКО для тех витрин,
    #            где lib ничего не вернул)
    print("DEBUG ▶ rss phase")
    for cc in storefronts:
        if any(df["source"].iloc[0].endswith(cc) for df in dfs):
            continue  # уже есть lib-данные
        d = _fetch_appstore_rss(app_id, pages, cc)
        print(f"rss {cc}: {len(d)} rows")
        if not d.empty:
            dfs.append(d)

    # ◉ 3. Apify  (если после lib+rss витрина всё ещё пустая)
    print("DEBUG ▶ apify phase")
    for cc in storefronts:
        if any(df["source"].iloc[0].endswith(cc) for df in dfs):
            continue
        d = _fetch_appstore_apify(app_id, cc, limit=pages * 50)
        print(f"apify {cc}: {len(d)} rows")
        if not d.empty:
            dfs.append(d)

    if not dfs:
        warnings.warn("AppStore: ничего не удалось добыть")
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


###############################################################################
# 4. Reddit
###############################################################################
def fetch_reddit(subreddit: str, limit: int = 800) -> pd.DataFrame:
    if praw is None or not subreddit:
        return pd.DataFrame()

    try:
        reddit = praw.Reddit(
            client_id=os.getenv("REDDIT_ID"),
            client_secret=os.getenv("REDDIT_SECRET"),
            user_agent="review-scraper/0.2",
        )
    except Exception as e:
        warnings.warn(f"Reddit auth error: {e}")
        return pd.DataFrame()

    kw_re = re.compile("|".join(chain(*TAGS.values())), re.I)
    rows = []
    try:
        for post in reddit.subreddit(subreddit).new(limit=limit):
            if kw_re.search(f"{post.title} {post.selftext or ''}"):
                rows.append(
                    (
                        post.id,
                        5,
                        f"{post.title}\n{post.selftext or ''}",
                        datetime.fromtimestamp(post.created_utc, tz=timezone.utc),
                    )
                )
            post.comments.replace_more(limit=0)
            for c in post.comments.list():
                if kw_re.search(c.body):
                    rows.append(
                        (
                            c.id,
                            5,
                            c.body,
                            datetime.fromtimestamp(c.created_utc, tz=timezone.utc),
                        )
                    )
    except Exception as e:
        warnings.warn(f"Reddit error: {e}")

    df = pd.DataFrame(rows, columns=["id", "rating", "text", "date"])
    if not df.empty:
        df["source"] = "reddit"
    return df


###############################################################################
# 5. Пред-обработка
###############################################################################
def _clean(txt: str) -> str:
    return re.sub(r"\s+", " ", txt).lower().strip()


def _tag(txt: str):
    txt = _clean(txt)
    return [
        t for t, kws in TAGS.items() if any(re.search(rf"\b{k}\w*", txt) for k in kws)
    ] or ["—"]


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])[lambda d: d["date"].dt.year >= YEAR_FROM]
    df["text"] = df["text"].astype(str)
    df["tags"] = df["text"].apply(_tag)
    df["sentiment"] = df["rating"].apply(
        lambda r: "positive" if r >= 4 else ("negative" if r <= 2 else "neutral")
    )
    return df.drop_duplicates("id").reset_index(drop=True)


###############################################################################
# 6. Пайплайн одного приложения
###############################################################################
def run_pipeline(
    tag: str, cfg: dict, *, storefronts=_STOREFRONTS, skip_reddit: bool = False
) -> None:
    print(f"\n=== {tag.upper():<12} ===")

    gp = fetch_google_play(cfg.get("gp_id"))
    ios = fetch_appstore(
        cfg.get("ios_slug"), cfg.get("ios_id"), storefronts=storefronts
    )
    rd = pd.DataFrame() if skip_reddit else fetch_reddit(cfg.get("sub", ""))

    df = preprocess(pd.concat([gp, ios, rd], ignore_index=True))
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M")
    out_csv = OUT_DIR / f"{tag}_{ts}.csv"
    out_parq = OUT_DIR / f"{tag}_{ts}.parquet"

    if df.empty:
        print("нет данных :(")
        return

    df.to_csv(out_csv, index=False, encoding="utf-8")
    df.to_parquet(out_parq, index=False)
    print(f"✓ {len(df):,} отзывов сохранено → {out_csv}")

    sent_cols = ["positive", "neutral", "negative"]
    agg = (
        df.explode("tags")
        .groupby("tags")["sentiment"]
        .value_counts(normalize=True)
        .mul(100)
        .round(1)
        .unstack(fill_value=0)
        .reindex(columns=sent_cols, fill_value=0)
        .sort_values("positive", ascending=False)
    )
    print(agg.to_string())


###############################################################################
# 7. CLI
###############################################################################
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Сбор отзывов + тегирование геймификационных механик"
    )
    parser.add_argument(
        "--app", required=True, nargs="+", help="ключ(и) из словаря APPS, либо all"
    )
    parser.add_argument(
        "--storefronts",
        nargs="+",
        default=_STOREFRONTS,
        help="ISO-коды App Store витрин (ru us gb …)",
    )
    parser.add_argument(
        "--skip-reddit", action="store_true", help="не обращаться к Reddit"
    )
    args = parser.parse_args()

    targets = APPS.keys() if "all" in args.app else [a for a in args.app if a in APPS]
    if not targets:
        parser.error("Неизвестные --app; доступны: " + ", ".join(APPS.keys()) + ", all")

    for t in targets:
        run_pipeline(
            t,
            APPS[t],
            storefronts=tuple(args.storefronts),
            skip_reddit=args.skip_reddit,
        )
