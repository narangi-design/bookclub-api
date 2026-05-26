from matching import dedup_book_ids


class TestDedupBookIds:
    def test_removes_duplicates(self):
        assert dedup_book_ids([1, 2, 2, 3, 1]) == [1, 2, 3]

    def test_preserves_order(self):
        assert dedup_book_ids([5, 3, 5, 1, 3]) == [5, 3, 1]

    def test_no_duplicates_unchanged(self):
        assert dedup_book_ids([1, 2, 3]) == [1, 2, 3]

    def test_all_same(self):
        assert dedup_book_ids([7, 7, 7]) == [7]

    def test_empty_list(self):
        assert dedup_book_ids([]) == []

    def test_single_element(self):
        assert dedup_book_ids([42]) == [42]
