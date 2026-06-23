# bookclub-api

REST API for a book club. Stores data in PostgreSQL (Supabase) and book cover images in Supabase Storage.

Part of the [Book Club](https://github.com/stars/narangi-design/lists/book-club) project — also includes a [Telegram Bot](https://github.com/narangi-design/bookclub-chatbot) that uses this API for all bot commands, and a [Web Dashboard](https://github.com/narangi-design/bookclub-frontend) that reads club data through the public endpoints.

> All club content (book titles, member names, bot messages) is in Russian.

---

## Stack

- **FastAPI 0.135** + **Mangum** — API framework and Vercel adapter
- **psycopg2** — PostgreSQL connection
- **httpx** — cover image downloading
- **rapidfuzz** — fuzzy title and author matching
- **python-jose** — JWT auth
- **beautifulsoup4** — LitRes cover scraping

---

## Endpoints

### Public (for the frontend)

| Method | Path | Description |
|---|---|---|
| GET | `/api/books` | All books |
| GET | `/api/authors` | All authors |
| GET | `/api/polls` | All polls |
| GET | `/api/poll-votes` | All votes |
| GET | `/api/members` | All members |
| GET | `/api/award-votes` | Award votes |
| GET | `/api/award-events` | Award events |
| POST | `/api/auth/login` | Log in |
| GET | `/api/auth/me` | Current user |
| PUT | `/api/auth/me` | Update profile |

### Bot endpoints (`/api/bot/...`, require `x-bot-secret` header)

| Method | Path | Description |
|---|---|---|
| GET | `/poll-candidates` | Weighted random sample of books for a poll |
| POST | `/books` | Add a book nomination |
| DELETE | `/books/{id}` | Remove a nomination |
| GET | `/books/search` | Search nominations by title |
| GET | `/books/without-cover` | Books missing a cover image |
| GET | `/books/recently-read` | Recently read books without a discussion recording |
| PUT | `/books/{id}/cover_url` | Save chosen cover URL |
| PUT | `/books/{id}/discussion_url` | Save discussion recording link |
| GET | `/books/{id}/covers` | Find cover options via Google Books and LitRes |
| GET | `/members/{telegram_id}/books` | A member's nominations |
| POST | `/polls` | Create a poll record |
| POST | `/polls/results` | Save poll results, detect winner or tie |

---

## Getting started

**Prerequisites:** Python 3.11+

```bash
pip install -r requirements.txt
uvicorn main:app --reload
```

Required `.env`:
```
DATABASE_URL=
BOT_SECRET=
JWT_SECRET=
SUPABASE_URL=
SUPABASE_SERVICE_KEY=
GOOGLE_BOOKS_API_KEY=
ALLOWED_ORIGINS=http://localhost:5173
```

---

## Technical decisions

### Poll candidate sampling
Candidates are drawn with weighted random sampling — books that haven't appeared in a poll recently get a higher weight. A constant boost (`POLL_RECENCY_BOOST = 90` days) is added so books that have never been in a poll still get a meaningful weight rather than collapsing to zero.

### Idempotent poll results
`POST /polls/results` checks whether votes for a poll are already stored before inserting. This makes it safe to call from both `/results` and `/second_round` without duplicating data.

### Tie detection and winner backfill
When a second-round poll is saved, its winner is backfilled to the parent (first-round) poll's `winner_book_id`. The frontend reads the winner from the child poll if it exists, so this keeps both records consistent without a join.

### Fuzzy title and author matching
When adding a book, titles are matched against existing books with rapidfuzz to catch near-duplicates before inserting. Author names go through the same matching to avoid creating duplicate author records.

### Cover storage
Covers are downloaded and re-uploaded to Supabase Storage rather than storing external URLs. This avoids broken images if Google Books or LitRes change their URLs.

---

## Future improvements

- Endpoints for automated weekly rubric posts (Vercel Cron)
- Store discussion recording duration alongside the URL

---

## File structure

```
main.py         # All endpoints
auth.py         # JWT auth
db.py           # DB connection
matching.py     # Fuzzy string matching
cover_search.py # Cover lookup via Google Books and LitRes
```
