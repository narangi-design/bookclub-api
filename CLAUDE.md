# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

REST API for a book club (FastAPI on Vercel via Mangum). Stores data in PostgreSQL (Supabase) and book cover images in Supabase Storage. All club content (book titles, member names, bot-facing error messages) is in Russian — keep new user-facing strings in Russian too.

Part of a 3-repo project: this API, a Telegram bot (`bookclub-chatbot`) that drives all bot-facing endpoints, and a web dashboard (`bookclub-frontend`) that reads the public endpoints.

## Commands

```bash
pip install -r requirements.txt
uvicorn main:app --reload          # run locally, http://localhost:8000

pytest                              # run all tests
pytest test_matching.py             # single file
pytest test_matching.py::TestFindMatch::test_exact_match_case_insensitive  # single test
```

No lint/format tooling is configured in this repo.

Required `.env` (note: the code reads `JWT_SECRET_KEY`, not `JWT_SECRET` as an older README section says):
```
DATABASE_URL=
JWT_SECRET_KEY=
BOT_SECRET=
SUPABASE_URL=
SUPABASE_SERVICE_KEY=
GOOGLE_BOOKS_API_KEY=
LITRES_COOKIES=        # JSON dict of cookies, optional — enables LitRes cover fallback
ALLOWED_ORIGINS=http://localhost:5173
API_URL=                # only used by cover_search.py's dev CLI
```

## Architecture

Everything lives in five flat modules — there's no package structure to navigate:

- **main.py** — the entire route surface. One `FastAPI()` app plus one `APIRouter(prefix='/api/bot', dependencies=[Depends(verify_bot_secret)])` mounted into it. Deployed on Vercel as a single serverless function (`handler = Mangum(app)`); `vercel.json` routes everything to `main.py`.
- **db.py** — `get_connection()` opens a fresh `psycopg2` connection per call (no pooling). `get_data(table_name)` is a generic `SELECT *` used by the simple public list endpoints.
- **auth.py** — JWT auth for the dashboard's protected endpoints (`get_current_user` dependency, `HTTPBearer`). Passwords are SHA-256 (unsalted) via `hash_password`, not bcrypt — this is existing behavior, don't "fix" it as a drive-by.
- **matching.py** — rapidfuzz-based fuzzy matching (`find_match`: exact case-insensitive match first, then `fuzzy_find` via `token_sort_ratio`). `TITLE_MATCH_THRESHOLD` (90) and `AUTHOR_MATCH_THRESHOLD` (93) are deliberately different — see `test_matching.py`'s Сорокин/Серкин tests for why the author threshold is tuned tighter (a lower threshold would confuse two distinct real author names that score ~90% similar).
- **cover_search.py** — cover lookup: tries Google Books first, falls back to LitRes (scraped via a `__NEXT_DATA__` JSON blob, requires `LITRES_COOKIES`) only if Google returns nothing. Also has a `python cover_search.py <book_id>` dev CLI that hits a running API (`API_URL`) to look up a book and print candidate covers.

### Two audiences, two auth schemes, in one app

- **Public endpoints** (`GET /api/books`, `/authors`, `/polls`, `/poll-votes`, `/award-votes`, `/award-events`) — no auth, read-only, thin wrappers over `get_data()`.
- **Protected endpoints** (`GET/PUT /api/auth/me`, `GET /api/members`) — JWT bearer auth via `get_current_user`, used by the dashboard's logged-in views.
- **Bot endpoints** (`/api/bot/...`) — shared-secret auth via the `x-bot-secret` header (`verify_bot_secret`, checked with `hmac.compare_digest`), called only by the Telegram bot. This is where almost all the write logic and business rules live.

### Member identity resolution

Members are matched primarily by `telegram_id`, falling back to `telegram_username` with an id backfill (`resolve_member_id` in main.py). This exists because a member's Telegram id isn't always known/stable at first contact but their username is.

### Poll candidate weighting (`GET /api/bot/poll-candidates`)

Weighted random sampling without replacement, favoring books that have waited longest / been in the fewest polls / been out of a poll the longest. See the `calc_weight` docstring-comment block in `main.py` for the exact formula; `POLL_RECENCY_BOOST = 90` (days) exists so a book that has never been in a poll doesn't get a near-zero weight from `days_since_poll = 0`.

### Poll lifecycle: first round → optional second round → results

- `POST /api/bot/polls` creates a poll row and its `poll_book_options` (deduped via `dedup_book_ids`, which preserves first-seen order).
- `POST /api/bot/polls/results` is idempotent — it checks whether `poll_votes` already exist for the poll before inserting, so it's safe to call for both the first round and a `parent_poll_id`-linked second round without double-counting.
- On a decisive (non-tied) result, the winner is written to `polls.winner_book_id` on *both* the poll and its `parent_poll_id` (if any) — the frontend reads the winner off the child poll when one exists, so this keeps both rows consistent without a join. The book itself is flipped to `status = 'read'` with `elected_poll_id`/`elected_at` set.
- `poll_appearances` in the winner payload only counts rows where `parent_poll_id IS NULL`, i.e. first-round appearances — a second-round rerun of the same book shouldn't inflate its appearance count.
- A tie (multiple books sharing max votes) returns `tied_books` instead of a winner and leaves `books.status` untouched, so the bot can re-run a second-round poll.

### Book nomination flow (`POST /api/bot/books`)

Sequential steps, each doing its own fuzzy-match/create-or-reuse: (1) reject if title fuzzy-matches an existing non-removed book, (2) find-or-create the author, (3) find-or-create the member, (4) if a book with a fuzzy-matching title was previously `removed`, restore it in place rather than inserting a new row (preserves history/covers), else insert fresh.

### Cover handling

Covers are always downloaded and re-uploaded into Supabase Storage (`_upload_to_storage` in main.py, bucket `covers`, filename `{book_id}.{ext}`) rather than storing the external Google Books/LitRes URL directly — this avoids broken images if those providers change/expire URLs. Two ways in: `PUT /books/{id}/cover_url` (bot passes a discovered URL, server fetches+re-uploads it) and `PUT /books/{id}/cover` (raw image bytes in the request body, e.g. a manual upload).

## Notes for changes

- DB access throughout is raw `psycopg2` with manually opened/closed connections in `try/finally` — no ORM, no connection pooling, no context-manager helper. Match this style rather than introducing one for a single new endpoint.
- Write endpoints in main.py all follow the same shape: open conn/cursor, `try` the work, `commit`, `except Exception: rollback` + generic Russian `HTTPException(500, ...)`, `finally: conn.close()`. `HTTPException`s raised intentionally (e.g. 404s) are re-raised before the generic except.
- Tests (`test_matching.py`, `test_polls.py`) currently cover only `matching.py`'s pure functions — there's no DB fixture/mocking setup for testing the route handlers themselves.
