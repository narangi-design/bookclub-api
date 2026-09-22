from fastapi import FastAPI, APIRouter, Depends, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv
import hmac
import mimetypes
import os
import math
import random
from datetime import date, datetime, timedelta, timezone

import httpx
from io import BytesIO
from PIL import Image

from db import get_connection, get_data
from auth import hash_password, create_access_token, get_current_user, verify_telegram_auth, is_chat_member
from matching import find_match, fuzzy_find, dedup_book_ids, TITLE_MATCH_THRESHOLD, AUTHOR_MATCH_THRESHOLD
from cover_search import find_covers

load_dotenv()

# Added to days_since_poll so books that have never appeared in a poll
# still get a meaningful weight (otherwise days_since_poll = 0 collapses their chance)
POLL_RECENCY_BOOST = 90

# Book of the Year survey — hardcoded for this year, see prepare_survey_candidates.py
# for how survey_candidates gets populated before the survey opens.
# TEMP for testing: 2022 instead of 2026 so the heuristic/close_survey.py flow
# can be tested against real, already-complete data — 2022 has no existing
# award_votes/award_events rows (unlike 2023-2025), so close_survey.py can
# actually run instead of hitting its "already closed" guard. Switch back to
# 2026 before launch.
SURVEY_YEAR = 2022
SURVEY_DEADLINE = datetime(2027, 1, 10, 23, 59, 59, tzinfo=timezone.utc)
TIEBREAK_DURATION_DAYS = 2

# TEMP for testing: lets whoever has shell access flip the survey to
# "closed" on demand (`touch`/`rm` this path in the running container)
# without waiting for SURVEY_DEADLINE or rebuilding. Doesn't touch
# SURVEY_DEADLINE itself. Remove this override before the real launch.
SURVEY_FORCE_CLOSED_FLAG = '/tmp/survey_force_closed'

def _survey_is_closed() -> bool:
    if os.path.exists(SURVEY_FORCE_CLOSED_FLAG):
        return True
    return datetime.now(timezone.utc) > SURVEY_DEADLINE


app = FastAPI()


def resolve_member_id(cursor, telegram_id: int, telegram_username: str | None) -> int | None:
    """Look up member by telegram_id, fall back to telegram_username, backfill id if found via username."""
    cursor.execute('SELECT id FROM members WHERE telegram_id = %s', (telegram_id,))
    row = cursor.fetchone()
    if not row and telegram_username:
        cursor.execute('SELECT id FROM members WHERE telegram_username = %s', (telegram_username,))
        row = cursor.fetchone()
        if row:
            cursor.execute('UPDATE members SET telegram_id = %s WHERE id = %s', (telegram_id, row[0]))
    return row[0] if row else None


def find_or_create_member(cursor, telegram_id: int, telegram_username: str | None, telegram_fullname: str | None = None) -> int:
    member_id = resolve_member_id(cursor, telegram_id, telegram_username)
    if member_id is None:
        cursor.execute(
            'INSERT INTO members (telegram_id, telegram_username, telegram_fullname) VALUES (%s, %s, %s) RETURNING id',
            (telegram_id, telegram_username, telegram_fullname),
        )
        member_id = cursor.fetchone()[0]
    return member_id


def verify_bot_secret(x_bot_secret: str | None = Header(default=None)):
    secret = os.getenv('BOT_SECRET', '')
    if not x_bot_secret or not hmac.compare_digest(x_bot_secret, secret):
        raise HTTPException(status_code=403, detail='Forbidden')


origins = os.getenv('ALLOWED_ORIGINS', 'http://localhost:5173').split(',')

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=['GET', 'POST', 'PUT'],
    allow_headers=['*'],
)

# Book covers are stored on local disk (mounted as a volume in prod) and
# served straight back out by this same app, rather than in Supabase Storage.
# python:3.12-slim has no /etc/mime.types, so the stdlib mimetypes module
# doesn't know .webp — register it explicitly or StaticFiles serves covers
# as application/octet-stream.
mimetypes.add_type('image/webp', '.webp')
COVERS_DIR = os.getenv('COVERS_DIR', 'covers')
os.makedirs(COVERS_DIR, exist_ok=True)
app.mount('/covers', StaticFiles(directory=COVERS_DIR), name='covers')


# --- Public endpoints ---

@app.get('/api/books')
def get_books():
    return get_data('books')


# --- Bot endpoints (все защищены verify_bot_secret) ---

bot_router = APIRouter(prefix='/api/bot', dependencies=[Depends(verify_bot_secret)])

@bot_router.get('/poll-candidates')
def get_poll_candidates(n: int = 12):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            SELECT
                b.id,
                b.title,
                b.added_at,
                a.name                                AS author_name,
                COALESCE(m.telegram_username, m.telegram_fullname) AS member_display_name,
                COUNT(pv.id)                          AS appearances_count,
                MAX(p.date)                           AS last_poll_date
            FROM books b
            LEFT JOIN authors a     ON a.id = b.author_id
            LEFT JOIN members m     ON m.id = b.added_by_member_id
            LEFT JOIN poll_votes pv ON pv.book_id = b.id
            LEFT JOIN polls p       ON p.id = pv.poll_id
            WHERE b.status = \'to_read\'
            GROUP BY b.id, a.name, m.telegram_username, m.telegram_fullname
        ''')
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        conn.close()

    today = date.today()

    # A book's weight determines its probability of being included in the next poll.
    #
    # Inputs:
    #   days_in_list    — how long the book has been on the list. Longer wait → higher chance.
    #   days_since_poll — days since the book last appeared in a poll.
    #                     Falls back to days_in_list if the book has never been in a poll.
    #   appearances     — how many polls the book has already appeared in. More → lower priority.
    #
    # sqrt dampens the effect of large values: the difference between 100 and 400 days matters,
    # but shouldn't give a linear 4x advantage.
    #
    # POLL_RECENCY_BOOST (+90) is added to days_since_poll so that newly added books
    # that have never been in a poll don't get a near-zero weight.
    def calc_weight(book: dict) -> float:
        days_in_list = (today - book['added_at']).days if book['added_at'] else 1
        days_since_poll = (
            (today - book['last_poll_date']).days
            if book['last_poll_date'] else days_in_list
        )
        appearances = book['appearances_count'] or 0

        return (
            math.sqrt(max(days_in_list, 1))
            * math.sqrt(days_since_poll + POLL_RECENCY_BOOST)
            / math.sqrt(1 + appearances)
        )

    weighted = sorted(
        [{"book": b, "weight": calc_weight(b)} for b in rows],
        key=lambda x: x["weight"],
        reverse=True,
    )

    # Weighted sampling without replacement
    pool = list(weighted)
    selected = []
    for _ in range(min(n, len(pool))):
        total = sum(x["weight"] for x in pool)
        r = random.uniform(0, total)
        cumulative = 0
        for i, item in enumerate(pool):
            cumulative += item["weight"]
            if cumulative >= r:
                selected.append(item["book"])
                pool.pop(i)
                break

    return [
        {
            "id": b["id"],
            "title": b["title"],
            "author_name": b["author_name"],
            "member_display_name": b["member_display_name"],
        }
        for b in selected
    ]


class BotAddBookData(BaseModel):
    title: str
    author_name: str
    telegram_id: int
    telegram_username: str | None = None
    telegram_fullname: str | None = None

@bot_router.post('/books')
def bot_add_book(data: BotAddBookData):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Step 1: check for duplicate title among active books
        cursor.execute("SELECT title FROM books WHERE status != 'removed'")
        all_titles = [row[0] for row in cursor.fetchall()]
        if all_titles:
            title_match = find_match(data.title, all_titles)
            if title_match:
                return {'exists': True, 'existing_title': title_match}

        # Step 2: find or create author
        cursor.execute('SELECT id, name FROM authors')
        all_authors = cursor.fetchall()
        author_id = None
        if all_authors:
            matched_name = find_match(data.author_name, [a[1] for a in all_authors], threshold=AUTHOR_MATCH_THRESHOLD)
            if matched_name:
                author_id = next(a[0] for a in all_authors if a[1] == matched_name)
        if author_id is None:
            cursor.execute('INSERT INTO authors (name) VALUES (%s) RETURNING id', (data.author_name,))
            author_id = cursor.fetchone()[0]

        # Step 3: find or create member
        member_id = find_or_create_member(cursor, data.telegram_id, data.telegram_username, data.telegram_fullname)

        # Step 4: restore removed book or insert new one
        cursor.execute("SELECT id, title FROM books WHERE status = 'removed'")
        removed_titles = cursor.fetchall()
        removed_match = find_match(data.title, [r[1] for r in removed_titles]) if removed_titles else None
        if removed_match:
            book_id = next(r[0] for r in removed_titles if r[1] == removed_match)
            cursor.execute(
                "UPDATE books SET status = 'to_read', added_by_member_id = %s, added_at = CURRENT_DATE, author_id = %s WHERE id = %s",
                (member_id, author_id, book_id),
            )
        else:
            cursor.execute(
                "INSERT INTO books (title, author_id, added_by_member_id, added_at, status) VALUES (%s, %s, %s, CURRENT_DATE, 'to_read') RETURNING id",
                (data.title, author_id, member_id),
            )
            book_id = cursor.fetchone()[0]

        conn.commit()
        return {'ok': True, 'book_id': book_id}
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail='Не удалось добавить книгу')
    finally:
        conn.close()


class BotCreatePollData(BaseModel):
    stage: int
    date: str
    telegram_poll_id: str
    book_ids: list[int]
    parent_poll_id: int | None = None

@bot_router.post('/polls')
def bot_create_poll(data: BotCreatePollData):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'INSERT INTO polls (stage, date, telegram_poll_id, parent_poll_id) VALUES (%s, %s, %s, %s) RETURNING id',
            (data.stage, data.date, data.telegram_poll_id, data.parent_poll_id),
        )
        poll_id = cursor.fetchone()[0]
        unique_book_ids = dedup_book_ids(data.book_ids)
        for i, book_id in enumerate(unique_book_ids):
            cursor.execute(
                'INSERT INTO poll_book_options (poll_id, option_index, book_id) VALUES (%s, %s, %s)',
                (poll_id, i, book_id),
            )
        conn.commit()
        return {'ok': True, 'poll_id': poll_id}
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail='Не удалось создать опрос')
    finally:
        conn.close()



class PollOptionResult(BaseModel):
    option_index: int
    votes_count: int

class BotSavePollResultsData(BaseModel):
    telegram_poll_id: str
    total_voters: int
    options: list[PollOptionResult]

@bot_router.post('/polls/results')
def bot_save_poll_results(data: BotSavePollResultsData):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT id FROM polls WHERE telegram_poll_id = %s', (data.telegram_poll_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail='Poll not found')
        poll_id = row[0]

        cursor.execute('UPDATE polls SET total_voters = %s WHERE id = %s', (data.total_voters, poll_id))

        cursor.execute('SELECT COUNT(*) FROM poll_votes WHERE poll_id = %s', (poll_id,))
        already_saved = cursor.fetchone()[0] > 0

        if not already_saved:
            cursor.execute(
                'SELECT option_index, book_id FROM poll_book_options WHERE poll_id = %s',
                (poll_id,),
            )
            option_to_book = {r[0]: r[1] for r in cursor.fetchall()}
            for opt in data.options:
                book_id = option_to_book.get(opt.option_index)
                if book_id is not None:
                    cursor.execute(
                        'INSERT INTO poll_votes (poll_id, book_id, votes_count) VALUES (%s, %s, %s)',
                        (poll_id, book_id, opt.votes_count),
                    )

        cursor.execute(
            'SELECT pv.book_id, pv.votes_count, b.title, a.name, m.telegram_username '
            'FROM poll_votes pv '
            'JOIN books b ON b.id = pv.book_id '
            'LEFT JOIN authors a ON a.id = b.author_id '
            'LEFT JOIN members m ON m.id = b.added_by_member_id '
            'WHERE pv.poll_id = %s',
            (poll_id,),
        )
        vote_rows = cursor.fetchall()
        max_votes = max((r[1] for r in vote_rows), default=0)
        top_books = [r for r in vote_rows if r[1] == max_votes]
        is_tie = len(top_books) > 1

        winner_info = None
        tied_books = None

        if is_tie:
            tied_books = [{'id': r[0], 'title': r[2], 'author_name': r[3], 'votes': r[1]} for r in top_books]
        else:
            winner_book_id = top_books[0][0] if top_books else None
            if winner_book_id:
                cursor.execute('SELECT date, parent_poll_id FROM polls WHERE id = %s', (poll_id,))
                poll_date, parent_poll_id = cursor.fetchone()
                cursor.execute('UPDATE polls SET winner_book_id = %s WHERE id = %s', (winner_book_id, poll_id))
                if parent_poll_id:
                    cursor.execute('UPDATE polls SET winner_book_id = %s WHERE id = %s', (winner_book_id, parent_poll_id))
                cursor.execute(
                    "UPDATE books SET status = 'read', elected_poll_id = %s, elected_at = %s WHERE id = %s",
                    (poll_id, poll_date, winner_book_id),
                )
                cursor.execute("""
                    SELECT b.added_at, b.cover_url, m.telegram_fullname,
                           (SELECT COUNT(*)
                            FROM poll_votes pv2
                            JOIN polls p2 ON p2.id = pv2.poll_id
                            WHERE pv2.book_id = b.id AND p2.parent_poll_id IS NULL) AS poll_appearances
                    FROM books b
                    LEFT JOIN members m ON m.id = b.added_by_member_id
                    WHERE b.id = %s
                """, (winner_book_id,))
                extra = cursor.fetchone()
                winner_info = {
                    'book_id': winner_book_id,
                    'book_title': top_books[0][2],
                    'author_name': top_books[0][3],
                    'member_username': top_books[0][4],
                    'member_fullname': extra[2],
                    'votes': max_votes,
                    'added_at': extra[0].isoformat() if extra[0] else None,
                    'cover_url': extra[1],
                    'poll_appearances': extra[3],
                }

        conn.commit()
        return {'ok': True, 'poll_id': poll_id, 'winner': winner_info, 'tied_books': tied_books, 'total_voters': data.total_voters}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail='Не удалось сохранить результаты')
    finally:
        conn.close()


@bot_router.get('/members/{telegram_id}/books')
def bot_get_member_books(telegram_id: int, telegram_username: str | None = None):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        member_id = resolve_member_id(cursor, telegram_id, telegram_username)
        if member_id is None:
            return []
        cursor.execute('''
            SELECT b.id, b.title, a.name
            FROM books b
            LEFT JOIN authors a ON a.id = b.author_id
            WHERE b.added_by_member_id = %s AND b.status = \'to_read\'
            ORDER BY b.added_at DESC
        ''', (member_id,))
        rows = cursor.fetchall()
        conn.commit()
    finally:
        conn.close()
    return [{'id': r[0], 'title': r[1], 'author_name': r[2]} for r in rows]


@bot_router.get('/books/recently-read')
def bot_get_recently_read(n: int = 5):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT b.id, b.title, a.name
            FROM books b
            LEFT JOIN authors a ON a.id = b.author_id
            WHERE b.status = 'read' AND (b.discussion_url IS NULL OR b.discussion_url = '')
            ORDER BY b.elected_at DESC NULLS LAST
            LIMIT %s
        """, (n,))
        rows = cursor.fetchall()
    finally:
        conn.close()
    return [{'id': r[0], 'title': r[1], 'author_name': r[2]} for r in rows]


@bot_router.put('/books/{book_id}/discussion_url')
def bot_save_discussion_url(book_id: int, data: dict):
    discussion_url = data.get('discussion_url', '').strip()
    if not discussion_url:
        raise HTTPException(status_code=400, detail='discussion_url is required')
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('UPDATE books SET discussion_url = %s WHERE id = %s RETURNING id', (discussion_url, book_id))
        if cursor.fetchone() is None:
            raise HTTPException(status_code=404, detail='Book not found')
        conn.commit()
        return {'ok': True}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail='Не удалось сохранить ссылку')
    finally:
        conn.close()


@bot_router.get('/books/without-cover')
def bot_get_books_without_cover():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT b.id, b.title, a.name
            FROM books b
            LEFT JOIN authors a ON a.id = b.author_id
            WHERE b.status != 'removed' AND (b.cover_url IS NULL OR b.cover_url = '')
            ORDER BY b.title
        """)
        rows = cursor.fetchall()
    finally:
        conn.close()
    return [{'id': r[0], 'title': r[1], 'author_name': r[2]} for r in rows]


@bot_router.get('/books/search')
def bot_search_books(q: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT b.id, b.title, a.name FROM books b LEFT JOIN authors a ON a.id = b.author_id WHERE b.status = 'to_read'"
        )
        rows = cursor.fetchall()
    finally:
        conn.close()

    titles = [r[1] for r in rows]
    matched_title = find_match(q, titles)
    if not matched_title:
        return []

    # Return all rows whose title fuzzy-matches the query
    from rapidfuzz import fuzz
    results = []
    for book_id, title, author in rows:
        if fuzz.token_sort_ratio(q.lower(), title.lower()) >= TITLE_MATCH_THRESHOLD:
            results.append({'id': book_id, 'title': title, 'author_name': author})
    return results


@bot_router.get('/books/{book_id}/covers')
def bot_get_book_covers(book_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT b.title, a.name FROM books b LEFT JOIN authors a ON b.author_id = a.id WHERE b.id = %s', (book_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail='Book not found')
        title, author = row
        return find_covers(title, author)
    finally:
        conn.close()


# Caps for the two cover orientations — portrait/square covers (the vast
# majority) are capped by height, landscape ones by width, ratio preserved.
# Never upscales: covers already under the cap are left at their own size.
COVER_MAX_PORTRAIT_HEIGHT = 800
COVER_MAX_LANDSCAPE_WIDTH = 600


def _upload_to_storage(book_id: int, image_bytes: bytes) -> str:
    """Re-encodes the cover to WebP (uniform format regardless of what the
    source — Google Books/LitRes/manual upload — sent) and downsizes it."""
    public_base_url = os.getenv('PUBLIC_API_URL', 'http://localhost:8000').rstrip('/')
    filename = f'{book_id}.webp'

    image = Image.open(BytesIO(image_bytes))
    if image.mode not in ('RGB', 'RGBA'):
        image = image.convert('RGBA' if 'transparency' in image.info or image.mode in ('P', 'LA') else 'RGB')

    width, height = image.size
    if width <= height and height > COVER_MAX_PORTRAIT_HEIGHT:
        new_height = COVER_MAX_PORTRAIT_HEIGHT
        new_width = round(width * (COVER_MAX_PORTRAIT_HEIGHT / height))
        image = image.resize((new_width, new_height), Image.Resampling.LANCZOS)
    elif width > height and width > COVER_MAX_LANDSCAPE_WIDTH:
        new_width = COVER_MAX_LANDSCAPE_WIDTH
        new_height = round(height * (COVER_MAX_LANDSCAPE_WIDTH / width))
        image = image.resize((new_width, new_height), Image.Resampling.LANCZOS)

    image.save(os.path.join(COVERS_DIR, filename), format='WEBP', quality=85)

    return f'{public_base_url}/covers/{filename}'


@bot_router.put('/books/{book_id}/cover')
async def bot_save_cover_bytes(book_id: int, request: Request):
    image_bytes = await request.body()
    if not image_bytes:
        raise HTTPException(status_code=400, detail='image body is required')

    try:
        stored_url = _upload_to_storage(book_id, image_bytes)
    except Exception:
        raise HTTPException(status_code=502, detail='Не удалось загрузить обложку в хранилище')

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('UPDATE books SET cover_url = %s WHERE id = %s RETURNING title', (stored_url, book_id))
        row = cursor.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail='Book not found')
        conn.commit()
        return {'ok': True, 'title': row[0]}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail='Не удалось сохранить обложку')
    finally:
        conn.close()


@bot_router.put('/books/{book_id}/cover_url')
def bot_save_cover_url(book_id: int, data: dict):
    source_url = data.get('cover_url', '').strip()
    if not source_url:
        raise HTTPException(status_code=400, detail='cover_url is required')

    try:
        r = httpx.get(source_url, timeout=15, follow_redirects=True)
        r.raise_for_status()
        image_bytes = r.content
    except Exception:
        raise HTTPException(status_code=502, detail='Не удалось скачать обложку')

    try:
        stored_url = _upload_to_storage(book_id, image_bytes)
    except Exception:
        raise HTTPException(status_code=502, detail='Не удалось загрузить обложку в хранилище')

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('UPDATE books SET cover_url = %s WHERE id = %s RETURNING title', (stored_url, book_id))
        row = cursor.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail='Book not found')
        conn.commit()
        return {'ok': True, 'title': row[0]}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail='Не удалось сохранить обложку')
    finally:
        conn.close()


@bot_router.delete('/books/{book_id}')
def bot_remove_book(book_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE books SET status = 'removed' WHERE id = %s AND status = 'to_read' RETURNING id",
            (book_id,),
        )
        found = cursor.fetchone() is not None
        conn.commit()
        return {'found': found}
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail='Не удалось удалить книгу')
    finally:
        conn.close()


app.include_router(bot_router)


@app.get('/api/authors')
def get_authors():
    return get_data('authors')

@app.get('/api/polls')
def get_polls():
    return get_data('polls')

@app.get('/api/poll-votes')
def get_poll_votes():
    return get_data('poll_votes')


@app.get('/api/award-votes')
def get_award_votes():
    return get_data('award_votes')

@app.get('/api/award-events')
def get_award_events():
    return get_data('award_events')


# --- Protected endpoints ---

@app.get('/api/members')
def get_members(current_user: dict = Depends(get_current_user)):
    return get_data('members')


# --- Survey ("Книга года") ---

def require_telegram_member(current_user: dict) -> int:
    """Returns the member_id for a Telegram-authenticated user, or raises 403.
    Password logins are dashboard-only and have no reliable mapping to a
    members row, so the survey is Telegram-only."""
    if current_user['auth_method'] != 'telegram':
        raise HTTPException(status_code=403, detail='Опрос доступен только через вход по Telegram')
    return current_user['user_id']


@app.get('/api/survey/meta')
def get_survey_meta():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT COUNT(*) FROM survey_candidates WHERE year = %s', (SURVEY_YEAR,))
        candidate_count = cursor.fetchone()[0]
    finally:
        conn.close()
    return {
        'year': SURVEY_YEAR,
        'deadline': SURVEY_DEADLINE.isoformat(),
        'candidate_count': candidate_count,
        'is_closed': _survey_is_closed(),
    }


@app.get('/api/survey/candidates')
def get_survey_candidates():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT b.* FROM survey_candidates sc JOIN books b ON b.id = sc.book_id WHERE sc.year = %s',
            (SURVEY_YEAR,),
        )
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        conn.close()
    return rows


@app.get('/api/survey/response')
def get_survey_response(current_user: dict = Depends(get_current_user)):
    member_id = require_telegram_member(current_user)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT id, books_read_count, open_text FROM survey_responses WHERE year = %s AND member_id = %s',
            (SURVEY_YEAR, member_id),
        )
        row = cursor.fetchone()
        if not row:
            return {'favorite_book_ids': [], 'least_favorite_book_ids': [], 'books_read_count': None, 'open_text': None}
        response_id, books_read_count, open_text = row

        cursor.execute('SELECT book_id, kind FROM survey_response_books WHERE response_id = %s', (response_id,))
        favorite_ids, least_favorite_ids = [], []
        for book_id, kind in cursor.fetchall():
            (favorite_ids if kind == 'favorite' else least_favorite_ids).append(book_id)
    finally:
        conn.close()
    return {
        'favorite_book_ids': favorite_ids,
        'least_favorite_book_ids': least_favorite_ids,
        'books_read_count': books_read_count,
        'open_text': open_text,
    }


class SurveyResponseData(BaseModel):
    favorite_book_ids: list[int]
    least_favorite_book_ids: list[int]
    books_read_count: int | None = None
    open_text: str | None = None

@app.put('/api/survey/response')
def put_survey_response(data: SurveyResponseData, current_user: dict = Depends(get_current_user)):
    member_id = require_telegram_member(current_user)

    if _survey_is_closed():
        raise HTTPException(status_code=403, detail='Приём ответов уже закрыт')

    favorite_ids = dedup_book_ids(data.favorite_book_ids)
    least_favorite_ids = dedup_book_ids(data.least_favorite_book_ids)

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT book_id FROM survey_candidates WHERE year = %s', (SURVEY_YEAR,))
        candidate_ids = {r[0] for r in cursor.fetchall()}
        submitted_ids = set(favorite_ids) | set(least_favorite_ids)
        if not submitted_ids <= candidate_ids:
            raise HTTPException(status_code=400, detail='Среди выбранных книг есть те, что не участвуют в премии')

        if data.books_read_count is not None and not (0 <= data.books_read_count <= len(candidate_ids)):
            raise HTTPException(status_code=400, detail='Некорректное число прочитанных книг')

        cursor.execute(
            '''
            INSERT INTO survey_responses (year, member_id, books_read_count, open_text, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (year, member_id)
            DO UPDATE SET books_read_count = EXCLUDED.books_read_count, open_text = EXCLUDED.open_text, updated_at = now()
            RETURNING id
            ''',
            (SURVEY_YEAR, member_id, data.books_read_count, data.open_text),
        )
        response_id = cursor.fetchone()[0]

        cursor.execute('DELETE FROM survey_response_books WHERE response_id = %s', (response_id,))
        for book_id in favorite_ids:
            cursor.execute(
                'INSERT INTO survey_response_books (response_id, book_id, kind) VALUES (%s, %s, %s)',
                (response_id, book_id, 'favorite'),
            )
        for book_id in least_favorite_ids:
            cursor.execute(
                'INSERT INTO survey_response_books (response_id, book_id, kind) VALUES (%s, %s, %s)',
                (response_id, book_id, 'least_favorite'),
            )

        conn.commit()
        return {'ok': True}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail='Не удалось сохранить ответ')
    finally:
        conn.close()


@app.get('/api/survey/open-texts')
def get_survey_open_texts():
    if not _survey_is_closed():
        return []
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT open_text FROM survey_responses WHERE year = %s AND open_text IS NOT NULL AND open_text != ''",
            (SURVEY_YEAR,),
        )
        rows = cursor.fetchall()
    finally:
        conn.close()
    return [r[0] for r in rows]


@app.get('/api/survey/tiebreaks')
def get_survey_tiebreaks():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT id, category, deadline, resolved FROM survey_tiebreaks WHERE year = %s',
            (SURVEY_YEAR,),
        )
        rows = cursor.fetchall()
        result = []
        for tb_id, category, deadline, resolved in rows:
            cursor.execute('SELECT book_id FROM survey_tiebreak_candidates WHERE tiebreak_id = %s', (tb_id,))
            result.append({
                'category': category,
                'deadline': deadline.isoformat(),
                'candidate_book_ids': [r[0] for r in cursor.fetchall()],
                'resolved': resolved,
            })
    finally:
        conn.close()
    return result


@app.get('/api/survey/tiebreaks/{category}/vote')
def get_tiebreak_vote(category: str, current_user: dict = Depends(get_current_user)):
    member_id = require_telegram_member(current_user)
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            '''
            SELECT tv.book_id
            FROM survey_tiebreak_votes tv
            JOIN survey_tiebreaks tb ON tb.id = tv.tiebreak_id
            WHERE tb.year = %s AND tb.category = %s AND tv.member_id = %s
            ''',
            (SURVEY_YEAR, category, member_id),
        )
        row = cursor.fetchone()
    finally:
        conn.close()
    return {'book_id': row[0] if row else None}


class TiebreakVoteData(BaseModel):
    book_id: int

@app.put('/api/survey/tiebreaks/{category}/vote')
def put_tiebreak_vote(category: str, data: TiebreakVoteData, current_user: dict = Depends(get_current_user)):
    member_id = require_telegram_member(current_user)

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT id, deadline FROM survey_tiebreaks WHERE year = %s AND category = %s',
            (SURVEY_YEAR, category),
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail='Тай-брейк не найден')
        tiebreak_id, deadline = row

        if datetime.now(timezone.utc) > deadline:
            raise HTTPException(status_code=403, detail='Приём голосов второго тура уже закрыт')

        cursor.execute(
            'SELECT 1 FROM survey_tiebreak_candidates WHERE tiebreak_id = %s AND book_id = %s',
            (tiebreak_id, data.book_id),
        )
        if not cursor.fetchone():
            raise HTTPException(status_code=400, detail='Эта книга не участвует во втором туре')

        cursor.execute(
            '''
            INSERT INTO survey_tiebreak_votes (tiebreak_id, member_id, book_id, updated_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (tiebreak_id, member_id)
            DO UPDATE SET book_id = EXCLUDED.book_id, updated_at = now()
            ''',
            (tiebreak_id, member_id, data.book_id),
        )

        conn.commit()
        return {'ok': True}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail='Не удалось сохранить голос')
    finally:
        conn.close()


# --- Auth ---

class LoginData(BaseModel):
    username: str
    password: str

@app.post('/api/auth/login')
def login(data: LoginData):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT id, username FROM users WHERE username = %s AND password_hash = %s',
            (data.username, hash_password(data.password))
        )
        user = cursor.fetchone()
    finally:
        conn.close()

    if not user:
        raise HTTPException(status_code=401, detail='Неверный логин или пароль')

    token = create_access_token(user[0], user[1], 'password')
    return {'access_token': token, 'token_type': 'bearer', 'user_id': user[0], 'name': user[1]}


class TelegramLoginData(BaseModel):
    id: int
    first_name: str | None = None
    last_name: str | None = None
    username: str | None = None
    photo_url: str | None = None
    auth_date: int
    hash: str

@app.post('/api/auth/telegram-login')
def telegram_login(data: TelegramLoginData):
    bot_token = os.getenv('BOT_TOKEN', '')
    chat_id = os.getenv('TELEGRAM_CHAT_ID', '')
    if not bot_token or not chat_id:
        raise HTTPException(status_code=500, detail='Вход через Telegram не настроен')

    if not verify_telegram_auth(data.model_dump(exclude_none=True), bot_token):
        raise HTTPException(status_code=401, detail='Недействительные данные Telegram')

    if not is_chat_member(data.id, chat_id, bot_token):
        raise HTTPException(status_code=403, detail='Вы не состоите в чате клуба')

    fullname = ' '.join(part for part in (data.first_name, data.last_name) if part) or None
    display_name = data.username or fullname or str(data.id)

    conn = get_connection()
    cursor = conn.cursor()
    try:
        member_id = find_or_create_member(cursor, data.id, data.username, fullname)
        conn.commit()
        print(f'[telegram-login] member_id={member_id} telegram_id={data.id} username={data.username}')
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail='Не удалось выполнить вход')
    finally:
        conn.close()

    token = create_access_token(member_id, display_name, 'telegram')
    return {'access_token': token, 'token_type': 'bearer', 'user_id': member_id, 'name': display_name}

@app.get('/api/auth/me')
def get_me(current_user: dict = Depends(get_current_user)):
    return {'user_id': current_user['user_id'], 'name': current_user['name'], 'auth_method': current_user['auth_method']}


class UpdateAccountData(BaseModel):
    current_password: str
    new_username: str | None = None
    new_password: str | None = None

@app.put('/api/auth/me')
def update_account(data: UpdateAccountData, current_user: dict = Depends(get_current_user)):
    if current_user['auth_method'] != 'password':
        raise HTTPException(status_code=400, detail='Смена пароля недоступна для входа через Telegram')

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT id FROM users WHERE id = %s AND password_hash = %s',
            (current_user['user_id'], hash_password(data.current_password))
        )
        if not cursor.fetchone():
            raise HTTPException(status_code=401, detail='Неверный пароль')

        updates = []
        params = []
        if data.new_username:
            updates.append('username = %s')
            params.append(data.new_username)
        if data.new_password:
            updates.append('password_hash = %s')
            params.append(hash_password(data.new_password))

        if updates:
            params.append(current_user['user_id'])
            cursor.execute(f'UPDATE users SET {", ".join(updates)} WHERE id = %s', params)
            conn.commit()

        cursor.execute('SELECT id, username FROM users WHERE id = %s', (current_user['user_id'],))
        updated = cursor.fetchone()
        return {'ok': True, 'user_id': updated[0], 'name': updated[1]}
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail='Не удалось обновить данные')
    finally:
        conn.close()
