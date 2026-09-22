"""One-off, run manually once after SURVEY_DEADLINE has passed: aggregates
survey_response_books into award_votes/award_events for SURVEY_YEAR, so the
existing AwardCard dashboard component picks the year up. Creates a
survey_tiebreaks row (candidates only, no winner yet) for any category where
two or more books are tied for the most votes — run close_tiebreak.py after
that tiebreak's own deadline passes.

NOT a blind delete+reinsert on every run — see the early-exit checks below.
A naive rerun would wipe out round2_votes/is_winner already written by
close_tiebreak.py and crash on survey_tiebreaks' (year, category) unique
constraint. Safe to re-run: if the year is already closed (an award_events
row exists) and there's no unresolved tiebreak, it's a no-op.

Usage:
    python close_survey.py
"""
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from db import get_connection
from main import SURVEY_YEAR, TIEBREAK_DURATION_DAYS
from survey import tally_votes, find_leaders

load_dotenv()


def main():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT 1 FROM award_events WHERE year = %s', (SURVEY_YEAR,))
        already_closed = cursor.fetchone() is not None

        cursor.execute('SELECT category FROM survey_tiebreaks WHERE year = %s AND resolved = false', (SURVEY_YEAR,))
        unresolved = [r[0] for r in cursor.fetchall()]

        if already_closed and unresolved:
            print(f'Год {SURVEY_YEAR} уже закрыт, но есть незакрытые тай-брейки: {unresolved}. '
                  f'Дождись их дедлайна, запусти close_tiebreak.py — пересчитывать здесь нечего.')
            return
        if already_closed:
            print(f'Год {SURVEY_YEAR} уже посчитан, пересчитывать нечего.')
            return

        cursor.execute(
            '''
            SELECT srb.book_id, srb.kind
            FROM survey_response_books srb
            JOIN survey_responses sr ON sr.id = srb.response_id
            WHERE sr.year = %s
            ''',
            (SURVEY_YEAR,),
        )
        tallies = tally_votes(cursor.fetchall())

        cursor.execute('SELECT book_id FROM survey_candidates WHERE year = %s', (SURVEY_YEAR,))
        for (book_id,) in cursor.fetchall():
            tallies.setdefault(book_id, {'liked': 0, 'disliked': 0})

        liked_counts = {book_id: t['liked'] for book_id, t in tallies.items()}
        disliked_counts = {book_id: t['disliked'] for book_id, t in tallies.items()}
        favorite_leaders = find_leaders(liked_counts)
        least_favorite_leaders = find_leaders(disliked_counts)

        for book_id, t in tallies.items():
            is_winner = len(favorite_leaders) == 1 and book_id == favorite_leaders[0]
            cursor.execute(
                '''
                INSERT INTO award_votes (year, book_id, liked_votes, disliked_votes, round2_votes, anti_round2_votes, is_winner)
                VALUES (%s, %s, %s, %s, NULL, NULL, %s)
                ''',
                (SURVEY_YEAR, book_id, t['liked'], t['disliked'], is_winner),
            )

        cursor.execute('SELECT COUNT(*) FROM survey_responses WHERE year = %s', (SURVEY_YEAR,))
        total_voters = cursor.fetchone()[0]
        cursor.execute('INSERT INTO award_events (year, total_voters) VALUES (%s, %s)', (SURVEY_YEAR, total_voters))

        tiebreak_deadline = datetime.now(timezone.utc) + timedelta(days=TIEBREAK_DURATION_DAYS)

        def open_tiebreak(category: str, leaders: list[int]):
            cursor.execute(
                'INSERT INTO survey_tiebreaks (year, category, deadline) VALUES (%s, %s, %s) RETURNING id',
                (SURVEY_YEAR, category, tiebreak_deadline),
            )
            tiebreak_id = cursor.fetchone()[0]
            for book_id in leaders:
                cursor.execute(
                    'INSERT INTO survey_tiebreak_candidates (tiebreak_id, book_id) VALUES (%s, %s)',
                    (tiebreak_id, book_id),
                )
            print(f'Ничья в категории "{category}" между {leaders} — создан тай-брейк, дедлайн {tiebreak_deadline}.')

        if len(favorite_leaders) > 1:
            open_tiebreak('favorite', favorite_leaders)
        if len(least_favorite_leaders) > 1:
            open_tiebreak('least_favorite', least_favorite_leaders)

        conn.commit()
        print(f'Готово: {SURVEY_YEAR} год посчитан, {total_voters} проголосовавших, {len(tallies)} книг в award_votes.')
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()
