from rapidfuzz import process, fuzz

TITLE_MATCH_THRESHOLD = 90
AUTHOR_MATCH_THRESHOLD = 93


def fuzzy_find(query: str, choices: list[str], threshold: int = TITLE_MATCH_THRESHOLD) -> str | None:
    result = process.extractOne(query, choices, scorer=fuzz.token_sort_ratio, score_cutoff=threshold)
    return result[0] if result else None


def find_match(query: str, choices: list[str], threshold: int = TITLE_MATCH_THRESHOLD) -> str | None:
    """Exact match (case-insensitive) first, then fuzzy."""
    lower = query.lower()
    exact = next((c for c in choices if c.lower() == lower), None)
    return exact if exact else fuzzy_find(query, choices, threshold)
