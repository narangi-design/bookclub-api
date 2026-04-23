from fastapi import FastAPI, APIRouter, Depends, HTTPException, Header
from mangum import Mangum
from rapidfuzz import process, fuzz
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import os
import math
import random
from datetime import date

from db import get_connection, get_data
from auth import hash_password, create_access_token, get_current_user

load_dotenv()

app = FastAPI()


def fuzzy_find(query: str, choices: list[str], threshold: int = 90) -> str | None:
    result = process.extractOne(query, choices, scorer=fuzz.token_sort_ratio, score_cutoff=threshold)
    return result[0] if result else None


def verify_bot_secret(x_bot_secret: str = Header()):
    if x_bot_secret != os.getenv('BOT_SECRET'):
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
    conn.close()

    today = date.today()

    def calc_weight(book: dict) -> float:
        days_in_list = (today - book['added_at']).days if book['added_at'] else 1
        days_since_poll = (
            (today - book['last_poll_date']).days
            if book['last_poll_date'] else days_in_list
        )
        appearances = book['appearances_count'] or 0

        return (
            math.sqrt(max(days_in_list, 1))
            * math.sqrt(days_since_poll + 90)
            / math.sqrt(1 + appearances)
        )

    weighted = sorted(
        [{"book": b, "weight": calc_weight(b)} for b in rows],
        key=lambda x: x["weight"],
        reverse=True,
    )

    # Взвешенная выборка без повторений
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

    cursor.execute('SELECT title FROM books WHERE status != \'removed\'')
    all_titles = [row[0] for row in cursor.fetchall()]
    if all_titles:
        title_match = fuzzy_find(data.title, all_titles)
        if title_match:
            conn.close()
            return {'exists': True, 'existing_title': title_match}

    cursor.execute('SELECT id, name FROM authors')
    all_authors = cursor.fetchall()
    author_id = None
    if all_authors:
        names = [a[1] for a in all_authors]
        matched_name = fuzzy_find(data.author_name, names)
        if matched_name:
            author_id = next(a[0] for a in all_authors if a[1] == matched_name)
    if author_id is None:
        cursor.execute('INSERT INTO authors (name) VALUES (%s) RETURNING id', (data.author_name,))
        author_id = cursor.fetchone()[0]

    cursor.execute('SELECT id FROM members WHERE telegram_id = %s', (data.telegram_id,))
    member_row = cursor.fetchone()
    if member_row:
        member_id = member_row[0]
    else:
        cursor.execute(
            'INSERT INTO members (telegram_id, telegram_username, telegram_fullname) VALUES (%s, %s, %s) RETURNING id',
            (data.telegram_id, data.telegram_username, data.telegram_fullname),
        )
        member_id = cursor.fetchone()[0]

    cursor.execute(
        "INSERT INTO books (title, author_id, added_by_member_id, added_at, status) VALUES (%s, %s, %s, CURRENT_DATE, 'to_read') RETURNING id",
        (data.title, author_id, member_id),
    )
    book_id = cursor.fetchone()[0]
    conn.commit()
    conn.close()
    return {'ok': True, 'book_id': book_id}


class BotCreatePollData(BaseModel):
    stage: int
    date: str
    telegram_poll_id: str
    book_ids: list[int]

@bot_router.post('/polls')
def bot_create_poll(data: BotCreatePollData):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'INSERT INTO polls (stage, date, telegram_poll_id) VALUES (%s, %s, %s) RETURNING id',
        (data.stage, data.date, data.telegram_poll_id),
    )
    poll_id = cursor.fetchone()[0]
    for i, book_id in enumerate(data.book_ids):
        cursor.execute(
            'INSERT INTO poll_book_options (poll_id, option_index, book_id) VALUES (%s, %s, %s)',
            (poll_id, i, book_id),
        )
    conn.commit()
    conn.close()
    return {'ok': True, 'poll_id': poll_id}


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

    cursor.execute('SELECT id FROM polls WHERE telegram_poll_id = %s', (data.telegram_poll_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail='Poll not found')
    poll_id = row[0]

    cursor.execute('UPDATE polls SET total_voters = %s WHERE id = %s', (data.total_voters, poll_id))

    cursor.execute(
        'SELECT option_index, book_id FROM poll_book_options WHERE poll_id = %s',
        (poll_id,),
    )
    option_to_book = {r[0]: r[1] for r in cursor.fetchall()}

    winner_book_id = None
    winner_votes = -1
    for opt in data.options:
        book_id = option_to_book.get(opt.option_index)
        if book_id is None:
            continue
        cursor.execute(
            'INSERT INTO poll_votes (poll_id, book_id, votes_count) VALUES (%s, %s, %s)',
            (poll_id, book_id, opt.votes_count),
        )
        if opt.votes_count > winner_votes:
            winner_votes = opt.votes_count
            winner_book_id = book_id

    if winner_book_id:
        cursor.execute('SELECT date FROM polls WHERE id = %s', (poll_id,))
        poll_date = cursor.fetchone()[0]
        cursor.execute('UPDATE polls SET winner_book_id = %s WHERE id = %s', (winner_book_id, poll_id))
        cursor.execute(
            "UPDATE books SET status = 'read', elected_poll_id = %s, elected_at = %s WHERE id = %s",
            (poll_id, poll_date, winner_book_id),
        )

    conn.commit()
    conn.close()
    return {'ok': True, 'winner_book_id': winner_book_id}


@bot_router.delete('/books')
def bot_remove_book(title: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE books SET status = 'removed' WHERE lower(title) = lower(%s) AND status != 'removed' RETURNING id",
        (title,),
    )
    found = cursor.fetchone() is not None
    conn.commit()
    conn.close()
    return {'found': found}


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
    cursor.execute(
        'SELECT id, username FROM users WHERE username = %s AND password_hash = %s',
        (data.username, hash_password(data.password))
    )
    user = cursor.fetchone()
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

    cursor.execute(
        'SELECT id FROM users WHERE id = %s AND password_hash = %s',
        (current_user['user_id'], hash_password(data.current_password))
    )
    if not cursor.fetchone():
        conn.close()
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
    conn.close()
    return {'ok': True, 'user_id': updated[0], 'name': updated[1]}


handler = Mangum(app)
