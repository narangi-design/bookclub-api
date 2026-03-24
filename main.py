from fastapi import FastAPI
import psycopg2
from dotenv import load_dotenv
import os
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

app = FastAPI()

def get_connection():
    return psycopg2.connect(os.getenv('DATABASE_URL'))

origins = os.getenv('ALLOWED_ORIGINS', 'http://localhost:5173').split(',')

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=['GET'],
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

@app.get('/api/users')
def get_users():
    return get_data('users')

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