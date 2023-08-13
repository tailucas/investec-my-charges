import asyncio

import simplejson as json

from asyncio import AbstractEventLoop
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Sequence

from pylib import (
    app_config,
    log
)

from .crypto import encrypt, decrypt, digest

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker, Session

db_tablespace: str = app_config.get('sqlite', 'tablespace_path')
dburl: str = f'sqlite+aiosqlite:///{db_tablespace}'
engine: AsyncEngine = create_async_engine(dburl)
async_session: AsyncSession = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
Base = declarative_base()

from sqlalchemy import Column, Integer, String, JSON, DateTime

from sqlalchemy import update, ForeignKey, UniqueConstraint, Result
from sqlalchemy.future import select
from sqlalchemy.orm import relationship, Mapped, Query


"""
DAOs
"""
class DbUser(Base):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_user_id = Column(Integer, index=True, unique=True, nullable=False)
    investec_client_id = Column(JSON)
    investec_client_id_digest = Column(String(96), index=True)
    investec_credentials = Column(JSON)
    investec_credentials_digest = Column(String(96), index=True)


class DbAccessToken(Base):
    __tablename__ = 'access_token'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('user.id'), unique=True, index=True)
    access_token = Column(JSON)
    access_token_digest = Column(String(96), index=True)
    access_token_expiry = Column(DateTime, nullable=False)
    UniqueConstraint(user_id, access_token_digest)


class DbAccount(Base):
    __tablename__ = 'account'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('user.id'), index=True)
    account_id = Column(String(32), index=True)
    account_number = Column(JSON)
    account_number_digest = Column(String(96), index=True)
    account_info = Column(JSON)
    UniqueConstraint(user_id, account_id)


class DbCard(Base):
    __tablename__ = 'card'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('user.id'), index=True)
    card_id = Column(Integer, index=True)
    card_number = Column(JSON)
    card_number_digest = Column(String(96), index=True)
    UniqueConstraint(user_id, card_id)

"""
DTOs
"""
class User:
    def __init__(self, telegram_user_id: int, db: DbUser) -> None:
        self.id: int = db.id.type.python_type()
        self.telegram_user_id: int = db.telegram_user_id.type.python_type()
        self.investec_client_id: str = decrypt(header=str(telegram_user_id), payload=str(db.investec_client_id))
        self.investec_client_id_digest: str = db.investec_client_id_digest.type.python_type()
        self.investec_credentials: str = decrypt(header=str(telegram_user_id), payload=str(db.investec_credentials))
        self.investec_credentials_digest: str = db.investec_credentials_digest.type.python_type()


class AccessToken:

    @property
    def token(self) -> Optional[Tuple[str, datetime]]:
        if self.access_token is None or self.access_token_expiry is None:
            return None
        return (self.access_token, self.access_token_expiry)

    def __init__(self, telegram_user_id: int, db: DbAccessToken) -> None:
        self.telegram_user_id: int = telegram_user_id
        self.access_token: str = decrypt(header=str(telegram_user_id), payload=str(db.access_token))
        self.access_token_digest: str = db.access_token_digest.type.python_type()
        self.access_token_expiry: datetime = db.access_token_expiry.type.python_type()


class Account:

    @property
    def info(self) -> str:
        return self.account_info

    def __init__(self, telegram_user_id: int, db: DbAccount) -> None:
        self.telegram_user_id: int = telegram_user_id
        self.account_id: str = db.account_id.type.python_type()
        self.account_number: str = decrypt(header=str(telegram_user_id), payload=str(db.account_number))
        self.account_number_digest: str = db.account_number_digest.type.python_type()
        self.account_info: str = decrypt(header=str(telegram_user_id), payload=str(db.account_info))


class Card:
    def __init__(self, telegram_user_id: int, db: DbCard) -> None:
        self.telegram_user_id: int = telegram_user_id
        self.card_id: int = db.card_id.type.python_type()
        self.card_number: str = decrypt(header=str(telegram_user_id), payload=str(db.card_number))
        self.card_number_digest: str = db.card_number_digest.type.python_type()


"""
Implementation
"""
class AppDB:
    def __init__(self, db_session: AsyncSession):
        self.db_session: AsyncSession = db_session
        self.loop: AbstractEventLoop = asyncio.get_event_loop()

    """
    Create DAOs
    """
    async def _get_db_user(self, telegram_user_id: int) -> Optional[DbUser]:
        r: Result = await self.db_session.execute(select(DbUser).where(DbUser.telegram_user_id==telegram_user_id))
        return r.scalars().one_or_none()

    async def _get_db_access_token(self, user_id: int) -> Optional[DbAccessToken]:
        r: Result = await self.db_session.execute(select(DbAccessToken).where(DbAccessToken.user_id==user_id))
        return r.scalars().one_or_none()

    async def _get_db_account(self, user_id: int, account_id: str) -> Optional[DbAccount]:
        r: Result = await self.db_session.execute(select(DbAccount).where(DbAccount.user_id==user_id & DbAccount.account_id == account_id))
        return r.scalars().one_or_none()

    async def _get_db_accounts(self, user_id: int) -> Sequence[DbAccount]:
        r: Result = await self.db_session.execute(select(DbAccount).where(DbAccount.user_id==user_id))
        return r.scalars().all()

    async def _get_db_card(self, user_id: int, card_id: int) -> Optional[DbCard]:
        r: Result = await self.db_session.execute(select(DbCard).where(DbCard.user_id==user_id & DbCard.card_id == card_id))
        return r.scalars().one_or_none()

    async def _get_db_cards(self, user_id: int) -> Sequence[DbCard]:
        r: Result = await self.db_session.execute(select(DbCard).where(DbCard.user_id==user_id))
        return r.scalars().all()

    """
    Create DTOs
    """
    async def get_user(self, telegram_user_id: int) -> Optional[User]:
        log.debug(f'Fetching user information for Telegram user {telegram_user_id}.')
        db: DbUser = await self._get_db_user(telegram_user_id=telegram_user_id)
        if db is None:
            return None
        else:
            return User(telegram_user_id=telegram_user_id, db=db)

    async def add_user(self, telegram_user_id: int, investec_client_id: str, investec_credentials: str):
        log.debug(f'Adding user for Telegram user {telegram_user_id}.')
        db_user: DbUser = await self._get_db_user(telegram_user_id=telegram_user_id)
        if db_user is None:
            log.debug(f'Adding new database user information.')
            db_user = DbUser(
                telegram_user_id=telegram_user_id,
                investec_client_id=encrypt(str(telegram_user_id), investec_client_id),
                investec_client_id_digest=digest(investec_client_id),
                investec_credentials=encrypt(str(telegram_user_id), investec_credentials),
                investec_credentials_digest=digest(investec_credentials))
        else:
            log.debug(f'Updating database user credentials.')
            db_user.investec_credentials = encrypt(str(telegram_user_id), investec_credentials)
            db_user.investec_credentials_digest = digest(investec_credentials)
        self.db_session.add(db_user)
        await self.db_session.flush()

    async def get_access_token(self, telegram_user_id: int, user_id: int) -> Optional[Tuple[str, datetime]]:
        log.debug(f'Fetching access token for Telegram user {telegram_user_id} (DB user {user_id}).')
        db = await self._get_db_access_token(user_id=user_id)
        if db is None:
            return None
        else:
            return AccessToken(telegram_user_id=telegram_user_id, db=db).token

    async def update_access_token(self, telegram_user_id: int, user_id: int, access_token: str, access_token_expiry: datetime):
        log.debug(f'Updating access token for Telegram user {telegram_user_id} (DB user {user_id}).')
        db_token = await self._get_db_access_token(user_id=user_id)
        if db_token is None:
            db_token = DbAccessToken(
                user_id=user_id,
                access_token=encrypt(header=str(telegram_user_id), payload=access_token),
                access_token_digest=digest(payload=access_token),
                access_token_expiry=access_token_expiry)
        else:
            db_token.access_token=encrypt(header=str(telegram_user_id), payload=access_token)
            db_token.access_token_digest=digest(payload=access_token)
            db_token.access_token_expiry=access_token_expiry
        self.db_session.add(db_token)
        await self.db_session.flush()

    async def get_accounts(self, telegram_user_id: int, user_id: int) -> Optional[List[Account]]:
        log.debug(f'Fetching accounts for Telegram user {telegram_user_id} (DB user {user_id}).')
        db_accounts: Sequence[DbAccount] = await self._get_db_accounts(user_id=user_id)
        if db_accounts is None:
            return None
        else:
            accounts: List[Account] = []
            for db in db_accounts:
                accounts.append(Account(telegram_user_id=telegram_user_id, db=db))
            return accounts

    async def add_accounts(self, telegram_user_id: int, user_id: int, account_info: List[Dict[str, str]]):
        log.debug(f'Adding {len(account_info)} accounts for Telegram user {telegram_user_id} (DB user {user_id}).')
        for info in account_info:
            account_id = info['accountId']
            db_account: DbAccount = await self._get_db_account(user_id=user_id, account_id=account_id)
            if db_account is None:
                account_number = info['accountNumber']
                db_account: DbAccount = DbAccount(
                    user_id=user_id,
                    account_id=account_id,
                    account_number=encrypt(header=str(telegram_user_id), payload=account_number),
                    account_number_digest=digest(payload=account_number),
                    account_info=encrypt(header=str(telegram_user_id), payload=json.dumps(info)))
            else:
                db_account.account_info = encrypt(header=str(telegram_user_id), payload=json.dumps(info))
            self.db_session.add(db_account)
        await self.db_session.flush()

    async def get_cards(self, telegram_user_id: int, user_id: int) -> Optional[List[Card]]:
        log.debug(f'Fetching cards for Telegram user {telegram_user_id} (DB user {user_id}).')
        db_cards: Sequence[DbCard] = await self._get_db_cards(user_id=user_id)
        if db_cards is None:
            return None
        else:
            cards: List[Card] = []
            for db in db_cards:
                cards.append(Card(telegram_user_id=telegram_user_id, db=db))
            return cards

    async def add_cards(self, telegram_user_id: int, user_id: int, cards: List[Tuple]):
        log.debug(f'Adding {len(cards)} cards for Telegram user {telegram_user_id} (DB user {user_id}).')
        for card_id, card_number in cards:
            db_card: DbCard = await self._get_db_card(user_id=user_id, card_id=card_id)
            if db_card is None:
                db_card: DbCard = DbCard(
                    user_id=user_id,
                    card_id=card_id,
                    card_number=encrypt(header=str(telegram_user_id), payload=card_number),
                    card_number_digest=digest(payload=card_number))
                self.db_session.add(db_card)
        await self.db_session.flush()

"""
Module Methods
"""
async def get_user(telegram_user_id: int) -> Optional[User]:
    async with async_session() as session:
        async with session.begin():
            db = AppDB(session)
            return await db.get_user(telegram_user_id=telegram_user_id)

async def add_user(telegram_user_id: int, user_id: int, investec_client_id: str, investec_credentials: str) -> None:
    async with async_session() as session:
        async with session.begin():
            db = AppDB(session)
            return await db.add_user(
                telegram_user_id=telegram_user_id,
                user_id=user_id,
                investec_client_id=investec_client_id,
                investec_credentials=investec_credentials)

async def get_access_token(telegram_user_id: int, user_id: int) -> Optional[Tuple[str, datetime]]:
    async with async_session() as session:
        async with session.begin():
            db = AppDB(session)
            return await db.get_access_token(
                telegram_user_id=telegram_user_id,
                user_id=user_id)

async def update_access_token(telegram_user_id: int, user_id: int, access_token: str, access_token_expiry: datetime) -> None:
    async with async_session() as session:
        async with session.begin():
            db = AppDB(session)
            return await db.update_access_token(
                telegram_user_id=telegram_user_id,
                user_id=user_id,
                access_token=access_token,
                access_token_expiry=access_token_expiry)

async def get_accounts(telegram_user_id: int, user_id: int) -> Optional[List[Account]]:
    async with async_session() as session:
        async with session.begin():
            db = AppDB(session)
            return await db.get_accounts(
                telegram_user_id=telegram_user_id,
                user_id=user_id)

async def add_accounts(telegram_user_id: int, user_id: int, account_info: List[Dict[str, str]]) -> None:
    async with async_session() as session:
        async with session.begin():
            db = AppDB(session)
            return await db.add_accounts(
                telegram_user_id=telegram_user_id,
                user_id=user_id,
                account_info=account_info)

async def get_cards(telegram_user_id: int, user_id: int) -> Optional[List[Card]]:
    async with async_session() as session:
        async with session.begin():
            db = AppDB(session)
            return await db.get_cards(
                telegram_user_id=telegram_user_id,
                user_id=user_id)

async def add_cards(telegram_user_id: int, user_id: int, cards: List[Tuple]) -> None:
    async with async_session() as session:
        async with session.begin():
            db = AppDB(session)
            return await db.add_cards(
                telegram_user_id=telegram_user_id,
                user_id=user_id,
                cards=cards)

async def db_startup():
    log.info(f'Database startup {db_tablespace}...')
    # create db tables
    async with engine.begin() as conn:
        log.debug('Creating database schema...')
        await conn.run_sync(Base.metadata.create_all)
