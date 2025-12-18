import json
import os
import threading
import secrets
import hashlib
from typing import Dict, Optional, List
from ..models.schema import User, UserRole

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
USERS_FILE = os.path.join(DATA_DIR, 'users.json')
_lock = threading.Lock()


def _ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)


def _load_users() -> Dict[str, dict]:
    _ensure_data_dir()
    if not os.path.exists(USERS_FILE):
        return {}
    with open(USERS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def _save_users(data: Dict[str, dict]) -> None:
    _ensure_data_dir()
    with open(USERS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _hash_password(password: str, salt: str) -> str:
    dk = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), bytes.fromhex(salt), 200_000)
    return dk.hex()


def verify_password(password: str, salt: str, password_hash: str) -> bool:
    return _hash_password(password, salt) == password_hash


def create_user(email: str, username: str, password: str, role: UserRole, phone: Optional[str] = None) -> User:
    with _lock:
        data = _load_users()
        if username in data:
            raise ValueError('用户名已存在')
        uid = max([u.get('id', 0) for u in data.values()] + [0]) + 1
        salt = secrets.token_hex(16)
        password_hash = _hash_password(password, salt)
        user = User(id=uid, email=email, phone=phone, username=username, password_hash=password_hash, salt=salt, role=role, is_active=True)
        data[username] = user.dict()
        _save_users(data)
        return user


def get_user_by_username(username: str) -> Optional[User]:
    data = _load_users()
    obj = data.get(username)
    if not obj:
        return None
    return User(**obj)


def list_users() -> List[User]:
    data = _load_users()
    return [User(**v) for v in data.values()]


def ensure_seed_users(seed: List[dict]) -> None:
    with _lock:
        data = _load_users()
        changed = False
        for item in seed:
            if item['username'] in data:
                continue
            uid = max([u.get('id', 0) for u in data.values()] + [0]) + 1
            salt = secrets.token_hex(16)
            password_hash = _hash_password(item['password'], salt)
            user = User(id=uid, email=item['email'], phone=item.get('phone'), username=item['username'], password_hash=password_hash, salt=salt, role=item['role'], is_active=True)
            data[item['username']] = user.dict()
            changed = True
        if changed:
            _save_users(data)
