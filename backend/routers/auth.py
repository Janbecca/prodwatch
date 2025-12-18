from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import jwt

from ..models.schema import User, UserCreate, UserRead, Token, UserRole
from ..storage.users import create_user, get_user_by_username, verify_password

router = APIRouter(prefix="/auth", tags=["auth"])

SECRET_KEY = "change-me-in-env"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 12

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


@router.post("/register", response_model=UserRead)
def register(user_in: UserCreate):
    try:
        user = create_user(
            email=user_in.email,
            username=user_in.username,
            password=user_in.password,
            role=UserRole.viewer,
            phone=user_in.phone,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return UserRead(**user.dict())


@router.post("/login", response_model=Token)
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = get_user_by_username(form_data.username)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="用户名或密码错误")
    if not verify_password(form_data.password, user.salt, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="用户名或密码错误")
    token = create_access_token({"sub": str(user.id), "role": user.role})
    return Token(access_token=token)


def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    credentials_exception = HTTPException(status_code=401, detail="凭证无效")
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = int(payload.get("sub"))
    except Exception:
        raise credentials_exception
    # look up by iterating since we store by username
    from ..storage.users import list_users

    for user in list_users():
        if user.id == user_id:
            return user
    raise credentials_exception


@router.get("/me", response_model=UserRead)
def read_me(current_user: User = Depends(get_current_user)):
    return UserRead(**current_user.dict())
