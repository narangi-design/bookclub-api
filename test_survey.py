from datetime import date

from survey import tally_votes, find_leaders, compute_discussion_years


class TestTallyVotes:
    def test_counts_favorite_and_least_favorite_separately(self):
        rows = [(1, 'favorite'), (1, 'favorite'), (2, 'least_favorite'), (1, 'least_favorite')]
        counts = tally_votes(rows)
        assert counts[1] == {'liked': 2, 'disliked': 1}
        assert counts[2] == {'liked': 0, 'disliked': 1}

    def test_empty_rows(self):
        assert tally_votes([]) == {}

    def test_same_book_favorite_and_least_favorite_both_counted(self):
        # Allowed by design — a member can rate the same book both ways.
        counts = tally_votes([(1, 'favorite'), (1, 'least_favorite')])
        assert counts[1] == {'liked': 1, 'disliked': 1}


class TestFindLeaders:
    def test_single_leader(self):
        assert find_leaders({1: 3, 2: 1}) == [1]

    def test_tie_returns_all_leaders(self):
        assert sorted(find_leaders({1: 2, 2: 2, 3: 1})) == [1, 2]

    def test_three_way_tie(self):
        assert sorted(find_leaders({1: 1, 2: 1, 3: 1})) == [1, 2, 3]

    def test_all_zero_returns_empty(self):
        # Nobody voted in this category — not a tie requiring a runoff.
        assert find_leaders({1: 0, 2: 0}) == []

    def test_no_candidates_returns_empty(self):
        assert find_leaders({}) == []


class TestComputeDiscussionYears:
    def test_discussion_year_is_next_books_elected_at_year(self):
        books = [
            {'id': 1, 'elected_at': date(2025, 12, 20)},
            {'id': 2, 'elected_at': date(2026, 1, 7)},
        ]
        years = compute_discussion_years(books)
        assert years[1] == 2026  # elected in 2025, but discussed once the next vote (Jan 2026) opened
        assert years[2] is None  # most recently elected, no successor yet

    def test_book_elected_late_december_discussed_next_year(self):
        books = [
            {'id': 1, 'elected_at': date(2026, 12, 25)},
            {'id': 2, 'elected_at': date(2027, 1, 5)},
        ]
        years = compute_discussion_years(books)
        assert years[1] == 2027

    def test_book_elected_and_discussed_same_year(self):
        books = [
            {'id': 1, 'elected_at': date(2026, 3, 1)},
            {'id': 2, 'elected_at': date(2026, 4, 1)},
        ]
        years = compute_discussion_years(books)
        assert years[1] == 2026

    def test_unordered_input_sorted_internally(self):
        books = [
            {'id': 2, 'elected_at': date(2026, 6, 1)},
            {'id': 1, 'elected_at': date(2026, 1, 1)},
            {'id': 3, 'elected_at': date(2026, 11, 1)},
        ]
        years = compute_discussion_years(books)
        assert years[1] == 2026
        assert years[2] == 2026
        assert years[3] is None

    def test_single_book_has_no_successor(self):
        books = [{'id': 1, 'elected_at': date(2026, 1, 1)}]
        assert compute_discussion_years(books) == {1: None}
