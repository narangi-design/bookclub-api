from fastapi import FastAPI, APIRouter, Depends, HTTPException, Header
from mangum import Mangum
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import hmac
import os
import math
import random
from datetime import date

import httpx

from db import get_connection, get_data
from auth import hash_password, create_access_token, get_current_user
from matching import find_match, fuzzy_find, dedup_book_ids, TITLE_MATCH_THRESHOLD, AUTHOR_MATCH_THRESHOLD
from cover_search import find_covers

load_dotenv()

# Added to days_since_poll so books that have never appeared in a poll
# still get a meaningful weight (otherwise days_since_poll = 0 collapses their chance)
POLL_RECENCY_BOOST = 90


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
        member_id = resolve_member_id(cursor, data.telegram_id, data.telegram_username)
        if member_id is None:
            cursor.execute(
                'INSERT INTO members (telegram_id, telegram_username, telegram_fullname) VALUES (%s, %s, %s) RETURNING id',
                (data.telegram_id, data.telegram_username, data.telegram_fullname),
            )
            member_id = cursor.fetchone()[0]

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
                winner_info = {
                    'book_id': winner_book_id,
                    'book_title': top_books[0][2],
                    'author_name': top_books[0][3],
                    'member_username': top_books[0][4],
                    'votes': max_votes,
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


def _upload_to_storage(book_id: int, image_bytes: bytes, content_type: str) -> str:
    supabase_url = os.getenv('SUPABASE_URL')
    service_key = os.getenv('SUPABASE_SERVICE_KEY')
    ext = 'jpg' if 'jpeg' in content_type else content_type.split('/')[-1]
    filename = f'{book_id}.{ext}'
    r = httpx.put(
        f'{supabase_url}/storage/v1/object/covers/{filename}',
        content=image_bytes,
        headers={
            'Authorization': f'Bearer {service_key}',
            'Content-Type': content_type,
            'x-upsert': 'true',
        },
        timeout=30,
    )
    r.raise_for_status()
    return f'{supabase_url}/storage/v1/object/public/covers/{filename}'


@bot_router.put('/books/{book_id}/cover_url')
def bot_save_cover_url(book_id: int, data: dict):
    source_url = data.get('cover_url', '').strip()
    if not source_url:
        raise HTTPException(status_code=400, detail='cover_url is required')

    try:
        r = httpx.get(source_url, timeout=15, follow_redirects=True)
        r.raise_for_status()
        image_bytes = r.content
        content_type = r.headers.get('content-type', 'image/jpeg').split(';')[0]
    except Exception:
        raise HTTPException(status_code=502, detail='Не удалось скачать обложку')

    try:
        stored_url = _upload_to_storage(book_id, image_bytes, content_type)
    except Exception:
        raise HTTPException(status_code=502, detail='Не удалось загрузить обложку в хранилище')

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('UPDATE books SET cover_url = %s WHERE id = %s RETURNING id', (stored_url, book_id))
        if cursor.fetchone() is None:
            raise HTTPException(status_code=404, detail='Book not found')
        conn.commit()
        return {'ok': True}
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

    token = create_access_token(user[0], user[1])
    return {'access_token': token, 'token_type': 'bearer', 'user_id': user[0], 'name': user[1]}

@app.get('/api/auth/me')
def get_me(current_user: dict = Depends(get_current_user)):
    return {'user_id': current_user['user_id'], 'name': current_user['name']}


class UpdateAccountData(BaseModel):
    current_password: str
    new_username: str | None = None
    new_password: str | None = None

@app.put('/api/auth/me')
def update_account(data: UpdateAccountData, current_user: dict = Depends(get_current_user)):
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


handler = Mangum(app)
