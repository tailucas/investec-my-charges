import asyncio

from pylib import (
    app_config,
    log
)

from .crypto import encrypt, decrypt, digest

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker, Session

db_tablespace = app_config.get('sqlite', 'tablespace_path')
dburl = f'sqlite+aiosqlite:///{db_tablespace}'
engine = create_async_engine(dburl)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
Base = declarative_base()

from sqlalchemy import Column, Integer, String, JSON

from sqlalchemy import update, ForeignKey, UniqueConstraint
from sqlalchemy.future import select
from sqlalchemy.orm import relationship


class DbUser(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_user_id = Column(Integer, index=True, unique=True, nullable=False)
    UniqueConstraint(telegram_user_id)


class User(object):
    def __init__(self, db_user: DbUser) -> None:
        self.id = db_user.id
        self.telegram_user_id = db_user.telegram_user_id


class DbUserPref(Base):
    __tablename__ = 'user_prefs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('users.id'), index=True)


class UserPref(object):
    def __init__(self, user_id: int, db_user_pref: DbUserPref) -> None:
        self.user_id = user_id


class AppDB(object):
    def __init__(self, db_session: Session):
        self.db_session = db_session
        self.loop = asyncio.get_event_loop()

    async def _get_db_user(self, telegram_user_id: int) -> DbUser:
        q = await self.db_session.execute(select(DbUser).where(DbUser.telegram_user_id==telegram_user_id))
        return q.scalars().one_or_none()

    async def get_user_registration(self, telegram_user_id: int) -> User:
        log.debug(f'Fetching user information for Telegram user {telegram_user_id}.')
        db_user = await self._get_db_user(telegram_user_id=telegram_user_id)
        if db_user is None:
            return None
        else:
            return User(db_user=db_user)
        

async def get_user_registration(telegram_user_id: int) -> User:
    async with async_session() as session:
        async with session.begin():
            pdb = AppDB(session)
            return await pdb.get_user_registration(telegram_user_id=telegram_user_id)


async def db_startup():
    log.info(f'Database startup {db_tablespace}...')
    # create db tables
    async with engine.begin() as conn:
        log.debug('Creating database schema...')
        await conn.run_sync(Base.metadata.create_all)
