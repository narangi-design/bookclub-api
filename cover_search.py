import httpx
import json
import os
import re
import sys
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from rapidfuzz import fuzz

load_dotenv()

GOOGLE_BOOKS_API_KEY = os.getenv('GOOGLE_BOOKS_API_KEY')
GOOGLE_BOOKS_URL = 'https://www.googleapis.com/books/v1/volumes'
LITRES_COOKIES: dict = json.loads(os.getenv('LITRES_COOKIES', '{}'))

TITLE_MATCH_THRESHOLD = 70

COVER_GOOGLE = 'cover_g'
COVER_LITRES = 'cover_l'


def _title_score(query: str, found: str) -> float:
    return fuzz.token_sort_ratio(query.lower(), found.lower())


def _google_volume_id(url: str) -> str | None:
    m = re.search(r'[?&]id=([^&]+)', url)
    return m.group(1) if m else None


def _litres_cover_id(url: str) -> str | None:
    m = re.search(r'/cover/(\d+)', url)
    return m.group(1) if m else None


def _google_url(volume_id: str) -> str:
    return (
        f'https://books.google.com/books/content'
        f'?id={volume_id}&printsec=frontcover&img=1&zoom=0&source=gbs_api'
    )


def _litres_url(cover_id: str) -> str:
    return f'https://www.litres.ru/pub/c/cover/{cover_id}.jpg'


def cover_url_from_ref(source: str, ref_id: str) -> str | None:
    if source == COVER_GOOGLE:
        return _google_url(ref_id)
    if source == COVER_LITRES:
        return _litres_url(ref_id)
    return None


def _search_google(title: str) -> list[dict]:
    params = {'q': title, 'maxResults': 5, 'printType': 'books'}
    if GOOGLE_BOOKS_API_KEY:
        params['key'] = GOOGLE_BOOKS_API_KEY
    try:
        r = httpx.get(GOOGLE_BOOKS_URL, params=params, timeout=10)
        results = []
        for item in r.json().get('items', []):
            info = item.get('volumeInfo', {})
            if _title_score(title, info.get('title', '')) < TITLE_MATCH_THRESHOLD:
                continue
            thumbnail = info.get('imageLinks', {}).get('thumbnail')
            if not thumbnail:
                continue
            volume_id = _google_volume_id(thumbnail)
            if volume_id:
                results.append({'source': COVER_GOOGLE, 'ref_id': volume_id, 'url': _google_url(volume_id)})
        return results
    except Exception:
        return []


def _search_litres(title: str, author: str | None) -> list[dict]:
    if not LITRES_COOKIES:
        return []
    q = f'{author} - {title}' if author else title
    try:
        r = httpx.get(
            'https://www.litres.ru/search/',
            params={'q': q},
            cookies=LITRES_COOKIES,
            headers={'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
            timeout=10,
        )
        soup = BeautifulSoup(r.text, 'html.parser')
        script = soup.find('script', id='__NEXT_DATA__')
        if not script:
            return []
        data = json.loads(script.text)
        state = json.loads(data['props']['pageProps']['initialState'])
        search_data = next(
            (v for k, v in state['rtkqApi']['queries'].items() if 'getSearchData' in k),
            None,
        )
        if not search_data:
            return []
        results = []
        for book in search_data['data']['data']:
            instance = book.get('instance', {})
            if _title_score(title, instance.get('title', '')) < TITLE_MATCH_THRESHOLD:
                continue
            cover_path = instance.get('cover_url', '')
            cover_id = _litres_cover_id(cover_path)
            if cover_id:
                results.append({'source': COVER_LITRES, 'ref_id': cover_id, 'url': _litres_url(cover_id)})
        return results
    except Exception:
        return []


def find_covers(title: str, author: str | None) -> list[dict]:
    """Returns list of {source, ref_id, url}. Tries Google Books first, falls back to Litres."""
    covers = _search_google(title)
    if not covers:
        covers = _search_litres(title, author)
    return covers


# ── dev CLI ──────────────────────────────────────────────────────────────────

def _cli_search(book_id: int) -> None:
    API_URL = os.getenv('API_URL', 'http://localhost:8000')
    books = httpx.get(f'{API_URL}/api/books', timeout=10).json()
    authors = httpx.get(f'{API_URL}/api/authors', timeout=10).json()
    author_by_id = {a['id']: a['name'] for a in authors}

    book = next((b for b in books if b['id'] == book_id), None)
    if not book:
        print(f'Книга с id={book_id} не найдена')
        return

    title = book['title']
    author = author_by_id.get(book['author_id']) if book.get('author_id') else None
    print(f'Книга: {title}' + (f' / {author}' if author else ''))

    covers = find_covers(title, author)
    if not covers:
        print('Обложки не найдены')
        return

    print(f'Найдено: {len(covers)}')
    for i, c in enumerate(covers):
        print(f'  [{i+1}] {c["source"]} / {c["ref_id"]}\n      {c["url"]}')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python cover_search.py <book_id>')
        sys.exit(1)
    _cli_search(int(sys.argv[1]))
