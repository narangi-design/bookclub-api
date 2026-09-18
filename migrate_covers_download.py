"""One-off migration step: download all book covers from Supabase Storage's
public URLs (no SUPABASE_SERVICE_KEY needed — the `covers` bucket is public,
so this is a plain GET on the cover_url already stored in the DB) into a
local directory, ready for the next step to serve from the new host.

Usage:
    python migrate_covers_download.py [output_dir]   # default: ./covers_download

Reads DATABASE_URL from the environment (.env), same as the running API.
Writes <output_dir>/{book_id}.{ext} plus a manifest.json summarizing results.
Safe to re-run: skips book_ids that are already downloaded.
"""
import json
import os
import sys

import httpx
from dotenv import load_dotenv

from db import get_connection

load_dotenv()


def main():
    output_dir = sys.argv[1] if len(sys.argv) > 1 else 'covers_download'
    os.makedirs(output_dir, exist_ok=True)

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id, cover_url FROM books WHERE cover_url IS NOT NULL AND cover_url != ''")
        rows = cursor.fetchall()
    finally:
        conn.close()

    print(f'{len(rows)} books have a cover_url')

    manifest_path = os.path.join(output_dir, 'manifest.json')
    manifest = {}
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            manifest = json.load(f)

    ok, skipped, failed = 0, 0, 0
    for book_id, cover_url in rows:
        key = str(book_id)
        if key in manifest and manifest[key].get('status') == 'ok' \
                and os.path.exists(os.path.join(output_dir, manifest[key]['filename'])):
            skipped += 1
            continue

        try:
            r = httpx.get(cover_url, timeout=30, follow_redirects=True)
            r.raise_for_status()
            content_type = r.headers.get('content-type', 'image/jpeg').split(';')[0]
            ext = 'jpg' if 'jpeg' in content_type else content_type.split('/')[-1]
            filename = f'{book_id}.{ext}'
            with open(os.path.join(output_dir, filename), 'wb') as f:
                f.write(r.content)
            manifest[key] = {
                'book_id': book_id,
                'original_url': cover_url,
                'filename': filename,
                'content_type': content_type,
                'size': len(r.content),
                'status': 'ok',
            }
            ok += 1
            print(f'  [ok] book {book_id}: {filename} ({len(r.content)} bytes)')
        except Exception as e:
            manifest[key] = {
                'book_id': book_id,
                'original_url': cover_url,
                'status': 'failed',
                'error': str(e),
            }
            failed += 1
            print(f'  [FAILED] book {book_id}: {e}')

        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f'\nDone: {ok} downloaded, {skipped} already present, {failed} failed')
    if failed:
        print('Failed book_ids:', [k for k, v in manifest.items() if v.get('status') == 'failed'])


if __name__ == '__main__':
    main()
