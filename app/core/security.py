from datetime import datetime, timedelta
from jose import jwt
from passlib.context import CryptContext

from ..core.config import secretKey

pwdContext = CryptContext(schemes=['bcrypt'], deprecated='auto')

algorithm = 'HS256'
tokenExpireMinutes = 60 * 12


def create_access_token(data: dict, expiresDelta: timedelta = None) -> str:
    toEncode = data.copy()

    if expiresDelta:
        expire = datetime.utcnow() + expiresDelta
    else:
        expire = datetime.utcnow() + timedelta(minutes=tokenExpireMinutes)

    toEncode['exp'] = expire

    jwtEncoded = jwt.encode(toEncode, secretKey, algorithm)

    return jwtEncoded


def verify_password(plainPassword: str, hashedPassword: str) -> bool:
    return pwdContext.verify(plainPassword, hashedPassword)


def get_password_hash(password: str) -> str:
    return pwdContext.hash(password)
