from sqlmodel import SQLModel, create_engine, Session
import os

DB_URL = os.getenv("DATABASE_URL", "sqlite:///./prodwatch.db")
engine = create_engine(DB_URL, echo=False, connect_args={"check_same_thread": False} if DB_URL.startswith("sqlite") else {})


def init_db() -> None:
    SQLModel.metadata.create_all(engine)


def get_session() -> Session:
    return Session(engine)
