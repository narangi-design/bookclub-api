import hashlib
import hmac
import os
import time
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

SECRET_KEY = os.getenv('JWT_SECRET_KEY')
ALGORITHM = 'HS256'
ACCESS_TOKEN_EXPIRE_DAYS = 30

# Login Widget payloads older than this are rejected, so a leaked/logged
# payload can't be replayed later to mint a session.
TELEGRAM_AUTH_MAX_AGE_SECONDS = 300

security = HTTPBearer()

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def create_access_token(user_id: int, name: str, auth_method: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)
    payload = {'sub': str(user_id), 'name': name, 'auth_method': auth_method, 'exp': expire}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = int(payload['sub'])
        name = payload['name']
        auth_method = payload['auth_method']
        return {'user_id': user_id, 'name': name, 'auth_method': auth_method}
    except (JWTError, KeyError, ValueError):
        raise HTTPException(status_code=401, detail='Токен недействителен')

def verify_telegram_auth(payload: dict, bot_token: str) -> bool:
    """Verify a Telegram Login Widget payload's hash and freshness.

    See https://core.telegram.org/widgets/login#checking-authorization —
    the hash is HMAC-SHA256 over the sorted 'key=value' fields (excluding
    'hash' itself), keyed by SHA-256(bot_token).
    """
    received_hash = payload.get('hash')
    if not received_hash:
        return False

    data_check_string = '\n'.join(
        f'{k}={v}' for k, v in sorted(payload.items()) if k != 'hash'
    )
    secret_key = hashlib.sha256(bot_token.encode()).digest()
    computed_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(computed_hash, received_hash):
        return False

    try:
        auth_date = int(payload['auth_date'])
    except (KeyError, ValueError):
        return False
    return time.time() - auth_date <= TELEGRAM_AUTH_MAX_AGE_SECONDS

def is_chat_member(telegram_id: int, chat_id: str, bot_token: str) -> bool:
    """Check membership in the club's Telegram group via the Bot API."""
    resp = httpx.get(
        f'https://api.telegram.org/bot{bot_token}/getChatMember',
        params={'chat_id': chat_id, 'user_id': telegram_id},
        timeout=10,
    )
    if resp.status_code != 200:
        return False
    status = resp.json().get('result', {}).get('status')
    return status not in (None, 'left', 'kicked')
