"""One-off migration step: takes covers downloaded by migrate_covers_download.py
and finishes the move to local storage — runs each file through the same
_upload_to_storage() main.py uses for real uploads (WebP conversion + resize,
written into COVERS_DIR) and rewrites the book's cover_url in the DB to
point at PUBLIC_API_URL/covers/... instead of Supabase.

Usage:
    python migrate_covers_upload.py [input_dir]   # default: ./covers_download

Reads DATABASE_URL and PUBLIC_API_URL from the environment, same as main.py.
Safe to re-run: only touches rows whose cover_url still points at Supabase.
"""
import json
import os
import sys

from dotenv import load_dotenv

from db import get_connection
from main import _upload_to_storage

load_dotenv()


def main():
    input_dir = sys.argv[1] if len(sys.argv) > 1 else 'covers_download'

    with open(os.path.join(input_dir, 'manifest.json')) as f:
        manifest = json.load(f)

    conn = get_connection()
    cursor = conn.cursor()

    moved, skipped, failed = 0, 0, 0
    try:
        for key, entry in manifest.items():
            if entry.get('status') != 'ok':
                failed += 1
                print(f"  [skip] book {key}: not downloaded ({entry.get('error')})")
                continue

            cursor.execute('SELECT cover_url FROM books WHERE id = %s', (entry['book_id'],))
            row = cursor.fetchone()
            if row is None:
                print(f"  [skip] book {entry['book_id']}: no longer exists")
                skipped += 1
                continue
            if row[0] and 'supabase' not in row[0]:
                print(f"  [skip] book {entry['book_id']}: cover_url already migrated ({row[0]})")
                skipped += 1
                continue

            src = os.path.join(input_dir, entry['filename'])
            with open(src, 'rb') as f:
                image_bytes = f.read()
            new_url = _upload_to_storage(entry['book_id'], image_bytes)

            cursor.execute('UPDATE books SET cover_url = %s WHERE id = %s', (new_url, entry['book_id']))
            moved += 1
            print(f"  [ok] book {entry['book_id']}: {entry['filename']} -> {new_url}")

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f'\nDone: {moved} migrated, {skipped} skipped, {failed} failed (not downloaded)')


if __name__ == '__main__':
    main()
