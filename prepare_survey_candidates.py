"""One-off, run manually before opening the Book of the Year survey (before
/survey_start): builds a draft candidate list for SURVEY_YEAR by
approximating each read book's discussion date as the elected_at date of the
NEXT book (the club opens a new vote right after discussing the current
pick — see survey.compute_discussion_years), then inserts the draft into
survey_candidates.

Review the printed list — especially books flagged as near a year boundary,
and the most recently elected book (no successor yet, always flagged
separately) — and adjust survey_candidates by hand (INSERT/DELETE) before the
survey opens if the heuristic got something wrong.

Usage:
    python prepare_survey_candidates.py [year]   # default: main.SURVEY_YEAR

Reads DATABASE_URL from the environment, same as main.py. Safe to re-run for
the same year before the survey opens (upserts, doesn't duplicate) — but note
it will NOT remove a book you deleted from survey_candidates by hand if the
heuristic still thinks it belongs; re-review the printed list after
re-running.
"""
import sys

from dotenv import load_dotenv

from db import get_connection
from main import SURVEY_YEAR
from survey import compute_discussion_years

load_dotenv()


def main():
    year = int(sys.argv[1]) if len(sys.argv) > 1 else SURVEY_YEAR

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id, elected_at FROM books WHERE status = 'read' AND elected_at IS NOT NULL")
        books = [{'id': r[0], 'elected_at': r[1]} for r in cursor.fetchall()]
        books_by_id = {b['id']: b for b in books}
        discussion_years = compute_discussion_years(books)

        candidate_ids = [book_id for book_id, discussion_year in discussion_years.items() if discussion_year == year]
        candidate_ids.sort(key=lambda bid: books_by_id[bid]['elected_at'])

        print(f'Черновик кандидатов на {year} год ({len(candidate_ids)} книг):')
        for book_id in candidate_ids:
            elected_at = books_by_id[book_id]['elected_at']
            flag = ' [на стыке годов — проверь глазами]' if elected_at.month in (1, 12) else ''
            print(f'  book_id={book_id} elected_at={elected_at}{flag}')

        newest_book_id = max(books_by_id, key=lambda bid: books_by_id[bid]['elected_at']) if books_by_id else None
        if newest_book_id is not None and discussion_years.get(newest_book_id) is None:
            print(f'\n[!] book_id={newest_book_id} — самая свежая избранная книга, ещё не обсуждена (нет следующей '
                  f'книги, по которой эвристика могла бы определить дату обсуждения). В черновик не попала — реши '
                  f'сама, входит ли она в {year} год, и добавь вручную через SQL при необходимости.')

        for book_id in candidate_ids:
            cursor.execute(
                'INSERT INTO survey_candidates (year, book_id) VALUES (%s, %s) ON CONFLICT DO NOTHING',
                (year, book_id),
            )
        conn.commit()
        print(f'\nЗаписано в survey_candidates за {year} год. Поправь вручную через SQL при необходимости, '
              f'затем можно открывать премию.')
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()
