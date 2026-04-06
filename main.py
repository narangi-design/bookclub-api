from fastapi import FastAPI, HTTPException
import psycopg2
from dotenv import load_dotenv
import os
import hashlib
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv()

app = FastAPI()

def get_connection():
    return psycopg2.connect(os.getenv('DATABASE_URL'))

origins = os.getenv('ALLOWED_ORIGINS', 'http://localhost:5173').split(',')

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=['GET', 'POST', 'PUT'],
    allow_headers=['*'],
)

def get_data(table_name: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM {table_name}')
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    conn.close()
    return [dict(zip(columns, row)) for row in rows]

@app.get('/api/books')
def get_books():
    return get_data('books')

@app.get('/api/members')
def get_members():
    return get_data('members')

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


# --- Auth ---

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

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

    return {'ok': True, 'user_id': user[0], 'name': user[1]}


class UpdateAccountData(BaseModel):
    user_id: int
    current_password: str
    new_username: str | None = None
    new_password: str | None = None

@app.put('/api/auth/me')
def update_account(data: UpdateAccountData):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        'SELECT id FROM users WHERE id = %s AND password_hash = %s',
        (data.user_id, hash_password(data.current_password))
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
        params.append(data.user_id)
        cursor.execute(f'UPDATE users SET {", ".join(updates)} WHERE id = %s', params)
        conn.commit()

    cursor.execute('SELECT id, username FROM users WHERE id = %s', (data.user_id,))
    updated = cursor.fetchone()
    conn.close()
    return {'ok': True, 'user_id': updated[0], 'name': updated[1]}
