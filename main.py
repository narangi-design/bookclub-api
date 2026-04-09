from fastapi import FastAPI, APIRouter, Depends, HTTPException, Header
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
