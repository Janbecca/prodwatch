from typing import Optional
from enum import Enum
from pydantic import BaseModel, EmailStr


class UserRole(str, Enum):
    admin = "admin"
    marketer = "marketer"
    viewer = "viewer"


class User(BaseModel):
    id: int
    email: EmailStr
    phone: Optional[str] = None
    username: str
    password_hash: str
    salt: str
    role: UserRole = UserRole.viewer
    is_active: bool = True


class UserCreate(BaseModel):
    email: EmailStr
    phone: Optional[str] = None
    username: str
    password: str


class UserRead(BaseModel):
    id: int
    email: EmailStr
    phone: Optional[str]
    username: str
    role: UserRole
    is_active: bool


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class LoginRequest(BaseModel):
    username: str
    password: str
