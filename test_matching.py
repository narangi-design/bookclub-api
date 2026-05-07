import pytest
from matching import find_match, fuzzy_find, TITLE_MATCH_THRESHOLD, AUTHOR_MATCH_THRESHOLD


# ─── fuzzy_find ──────────────────────────────────────────────────────────────

class TestFuzzyFind:
    TITLES = ['Мастер и Маргарита', 'Преступление и наказание', 'Война и мир']

    def test_exact_string_matches(self):
        assert fuzzy_find('Мастер и Маргарита', self.TITLES) == 'Мастер и Маргарита'

    def test_close_typo_matches(self):
        assert fuzzy_find('Мастер и Маргарита!', self.TITLES) == 'Мастер и Маргарита'

    def test_below_threshold_returns_none(self):
        assert fuzzy_find('Гарри Поттер', self.TITLES) is None

    def test_empty_choices_returns_none(self):
        assert fuzzy_find('Мастер и Маргарита', []) is None

    def test_custom_threshold_respected(self):
        # With a very high threshold, even a close match should fail
        assert fuzzy_find('Мастер и Маргарита!', self.TITLES, threshold=100) is None


# ─── find_match ──────────────────────────────────────────────────────────────

class TestFindMatch:
    TITLES = ['Тошнота', 'Скотный двор', 'Превращение']
    AUTHORS = ['Владимир Сорокин', 'Владимир Серкин', 'Лев Толстой']

    def test_exact_match_case_insensitive(self):
        assert find_match('тошнота', self.TITLES) == 'Тошнота'
        assert find_match('ТОШНОТА', self.TITLES) == 'Тошнота'

    def test_exact_match_returns_original_casing(self):
        result = find_match('скотный двор', self.TITLES)
        assert result == 'Скотный двор'

    def test_fuzzy_match_when_no_exact(self):
        # Slight variation → fuzzy picks it up
        assert find_match('Тошнота.', self.TITLES) == 'Тошнота'

    def test_no_match_returns_none(self):
        assert find_match('Дюна', self.TITLES) is None

    def test_empty_choices_returns_none(self):
        assert find_match('Тошнота', []) is None

    # ── Author threshold guards against Серкин/Сорокин false positive ────────

    def test_sorokin_serkin_not_confused_at_author_threshold(self):
        # token_sort_ratio for these two names sits around 90%,
        # so with AUTHOR_MATCH_THRESHOLD=93 neither should match the other
        result = find_match('Владимир Серкин', self.AUTHORS, threshold=AUTHOR_MATCH_THRESHOLD)
        assert result == 'Владимир Серкин'

    def test_sorokin_does_not_match_serkin(self):
        result = find_match('Владимир Сорокин', ['Владимир Серкин'], threshold=AUTHOR_MATCH_THRESHOLD)
        # Exact match check fails, fuzzy score ~90 < 93 threshold → None
        assert result is None

    def test_serkin_does_not_match_sorokin(self):
        result = find_match('Владимир Серкин', ['Владимир Сорокин'], threshold=AUTHOR_MATCH_THRESHOLD)
        assert result is None

    # ── Exact match takes priority even when fuzzy score would also fire ──────

    def test_exact_beats_fuzzy_near_miss(self):
        choices = ['Сорокин', 'Серкин']
        # 'Сорокин' is an exact match; fuzzy might also surface 'Серкин'
        result = find_match('Сорокин', choices)
        assert result == 'Сорокин'

    # ── Title threshold is more permissive than author threshold ─────────────

    def test_title_threshold_lower_than_author(self):
        assert TITLE_MATCH_THRESHOLD < AUTHOR_MATCH_THRESHOLD
