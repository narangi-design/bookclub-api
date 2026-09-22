"""Pure helper functions for the Book of the Year survey — vote tallying,
tie detection, and the "which year was this book actually discussed in"
heuristic used to build the yearly candidate list. Kept dependency-free so
it can be unit tested the same way matching.py is."""


def tally_votes(rows: list[tuple[int, str]]) -> dict[int, dict[str, int]]:
    """rows: (book_id, kind) pairs from survey_response_books, kind is
    'favorite' or 'least_favorite'. Returns {book_id: {'liked': n, 'disliked': n}}."""
    counts: dict[int, dict[str, int]] = {}
    for book_id, kind in rows:
        entry = counts.setdefault(book_id, {'liked': 0, 'disliked': 0})
        if kind == 'favorite':
            entry['liked'] += 1
        elif kind == 'least_favorite':
            entry['disliked'] += 1
    return counts


def find_leaders(vote_counts: dict[int, int]) -> list[int]:
    """Book ids tied for the highest vote count. Empty if there are no
    candidates, or if the max is 0 (nobody voted in that category)."""
    if not vote_counts:
        return []
    max_votes = max(vote_counts.values())
    if max_votes == 0:
        return []
    return [book_id for book_id, count in vote_counts.items() if count == max_votes]


def compute_discussion_years(books: list[dict]) -> dict[int, int | None]:
    """books: [{'id': int, 'elected_at': date}, ...]. A book's discussion
    year is approximated by the elected_at year of the NEXT book
    chronologically — the club opens a new vote right after discussing the
    current pick, so that next vote's date is a good proxy for when the
    current book was actually discussed (elected_at only marks when it was
    picked to read next, not when it was discussed). The most recently
    elected book has no successor yet, so its discussion year is unknown
    (None) — always needs a human call."""
    ordered = sorted(books, key=lambda b: b['elected_at'])
    result: dict[int, int | None] = {}
    for i, book in enumerate(ordered):
        result[book['id']] = ordered[i + 1]['elected_at'].year if i + 1 < len(ordered) else None
    return result
