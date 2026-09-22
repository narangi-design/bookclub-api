"""One-off, run manually after a specific tiebreak's own deadline has
passed: tallies survey_tiebreak_votes and writes the final round-2 numbers
into award_votes — round2_votes for a 'favorite' tiebreak (rendered by the
existing AwardCard component), anti_round2_votes for 'least_favorite'. Also
sets is_winner for a resolved 'favorite' tiebreak. 'least_favorite' needs no
winner flag — the anti-book of the year is always computed live (frontend),
preferring anti_round2_votes over disliked_votes when present.

If the tiebreak itself ends in another tie (or nobody voted in it), nothing
is written automatically — resolve it by hand in the DB. This is an
intentionally unhandled, very unlikely edge case.

Usage:
    python close_tiebreak.py <favorite|least_favorite>
"""
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

from db import get_connection
from main import SURVEY_YEAR
from survey import find_leaders

load_dotenv()


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ('favorite', 'least_favorite'):
        print('Usage: python close_tiebreak.py <favorite|least_favorite>')
        sys.exit(1)
    category = sys.argv[1]

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT id, deadline, resolved FROM survey_tiebreaks WHERE year = %s AND category = %s',
            (SURVEY_YEAR, category),
        )
        row = cursor.fetchone()
        if not row:
            print(f'Нет тай-брейка категории "{category}" за {SURVEY_YEAR} год.')
            return
        tiebreak_id, deadline, resolved = row
        if resolved:
            print('Этот тай-брейк уже закрыт.')
            return
        if datetime.now(timezone.utc) <= deadline:
            print(f'Дедлайн тай-брейка ещё не прошёл ({deadline}).')
            return

        cursor.execute('SELECT book_id FROM survey_tiebreak_candidates WHERE tiebreak_id = %s', (tiebreak_id,))
        vote_counts = {book_id: 0 for (book_id,) in cursor.fetchall()}

        cursor.execute('SELECT book_id FROM survey_tiebreak_votes WHERE tiebreak_id = %s', (tiebreak_id,))
        for (book_id,) in cursor.fetchall():
            vote_counts[book_id] = vote_counts.get(book_id, 0) + 1

        leaders = find_leaders(vote_counts)
        if len(leaders) != 1:
            print(f'[!] Второй тур тоже закончился ничьей или без голосов ({leaders}) — ничего не проставлено '
                  f'автоматически, реши вручную через SQL.')
            return

        winner_id = leaders[0]
        column = 'round2_votes' if category == 'favorite' else 'anti_round2_votes'
        for book_id, votes in vote_counts.items():
            cursor.execute(
                f'UPDATE award_votes SET {column} = %s WHERE year = %s AND book_id = %s',
                (votes, SURVEY_YEAR, book_id),
            )
        if category == 'favorite':
            cursor.execute(
                'UPDATE award_votes SET is_winner = (book_id = %s) WHERE year = %s AND book_id = ANY(%s)',
                (winner_id, SURVEY_YEAR, list(vote_counts.keys())),
            )

        cursor.execute('UPDATE survey_tiebreaks SET resolved = true WHERE id = %s', (tiebreak_id,))
        conn.commit()
        print(f'Тай-брейк "{category}" закрыт, победитель book_id={winner_id}.')
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()
