import httpx
import json
import os
import sys
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from rapidfuzz import fuzz

load_dotenv()

API_URL = os.getenv('API_URL', 'http://localhost:8000')
GOOGLE_BOOKS_API_KEY = os.getenv('GOOGLE_BOOKS_API_KEY')
GOOGLE_BOOKS_URL = 'https://www.googleapis.com/books/v1/volumes'
LITRES_COOKIES: dict = json.loads(os.getenv('LITRES_COOKIES', '{}'))

TITLE_MATCH_THRESHOLD = 70

Cover = tuple[str, float, str]  # (found_title, score, url)


def title_score(query: str, found: str) -> float:
    return fuzz.token_sort_ratio(query.lower(), found.lower())


def search_google(title: str) -> list[Cover]:
    params = {'q': title, 'maxResults': 5, 'printType': 'books'}
    if GOOGLE_BOOKS_API_KEY:
        params['key'] = GOOGLE_BOOKS_API_KEY
    try:
        r = httpx.get(GOOGLE_BOOKS_URL, params=params, timeout=10)
        covers = []
        for item in r.json().get('items', []):
            info = item.get('volumeInfo', {})
            found_title = info.get('title', '')
            score = title_score(title, found_title)
            thumbnail = info.get('imageLinks', {}).get('thumbnail')
            if thumbnail and score >= TITLE_MATCH_THRESHOLD:
                large = thumbnail.replace('zoom=1', 'zoom=0').replace('zoom=5', 'zoom=0').replace('&edge=curl', '')
                covers.append((found_title, score, large))
        return covers
    except Exception as e:
        print(f'  Google Books error: {e}')
        return []


def search_litres(title: str, author: str | None) -> list[Cover]:
    if not LITRES_COOKIES:
        print('  Litres: LITRES_COOKIES не заданы в .env')
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
            print('  Litres: __NEXT_DATA__ не найден — куки устарели?')
            return []
        data = json.loads(script.text)
        state = json.loads(data['props']['pageProps']['initialState'])
        search_data = next(
            (v for k, v in state['rtkqApi']['queries'].items() if 'getSearchData' in k),
            None,
        )
        if not search_data:
            return []
        covers = []
        for book in search_data['data']['data']:
            instance = book.get('instance', {})
            found_title = instance.get('title', '')
            score = title_score(title, found_title)
            cover_path = instance.get('cover_url', '')
            if cover_path and score >= TITLE_MATCH_THRESHOLD:
                url = f'https:{cover_path}' if cover_path.startswith('//') else f'https://www.litres.ru{cover_path}'
                covers.append((found_title, score, url))
        return covers
    except Exception as e:
        print(f'  Litres error: {e}')
        return []


def search_book_cover(book_id: int, books: list, author_by_id: dict) -> None:
    book = next((b for b in books if b['id'] == book_id), None)
    if not book:
        print(f'Книга с id={book_id} не найдена')
        return

    title = book['title']
    author = author_by_id.get(book['author_id']) if book.get('author_id') else None
    print(f'Книга:  {title}' + (f' / {author}' if author else ''))

    print('Google Books...')
    covers = search_google(title)

    if not covers:
        print('Не найдено, пробуем Литрес...')
        covers = search_litres(title, author)

    if not covers:
        print('Обложки не найдены')
        return

    print(f'Найдено: {len(covers)}')
    for i, (found_title, score, url) in enumerate(covers):
        print(f'  [{i+1}] {found_title} ({score:.0f}%)\n      {url}')


def main() -> None:
    if len(sys.argv) < 2:
        print('Usage: python dev_book_covers.py <book_id>')
        sys.exit(1)

    book_id = int(sys.argv[1])
    books = httpx.get(f'{API_URL}/api/books', timeout=10).json()
    authors = httpx.get(f'{API_URL}/api/authors', timeout=10).json()
    author_by_id = {a['id']: a['name'] for a in authors}

    search_book_cover(book_id, books, author_by_id)


if __name__ == '__main__':
    main()
