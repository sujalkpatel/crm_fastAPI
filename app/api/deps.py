from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from jose import jwt

from ..core.config import secretKey, logger
from ..core.security import algorithm
from ..crud import user, profile

outh2Scheme = OAuth2PasswordBearer(tokenUrl='/auth/access_token')


async def get_current_user(token: str = Depends(outh2Scheme)) -> dict:
    try:
        tokenPayload = jwt.decode(token, secretKey, algorithm)

    except (jwt.JWTError):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not validate credentials.",
        )

    # logger.debug(tokenPayload)

    userDoc = await user.get_user_by_email(tokenPayload['sub'])

    if not userDoc:
        raise HTTPException(status_code=404, detail="User not found.")

    return userDoc


async def get_current_active_user(currentUser: dict = Depends(get_current_user)) -> dict:
    if currentUser['status'] != 'Active':
        raise HTTPException(status_code=400, detail="User not active.")

    return currentUser


async def verify_get_acess_with_profile(module_name: str, currentUser: dict = Depends(get_current_active_user)) -> dict:
    logger.debug(module_name)
    logger.debug(currentUser['email'])
    logger.debug(currentUser['profile'])
    activityList = []
    if await profile.get_count_by_profile_module_operation(
            currentUser['profile'], module_name, 'view', activityList) == 0:
        raise HTTPException(
            status_code=400, detail='View access not allowed for ' + module_name + '.')

    if module_name == 'Activity':
        currentUser['activity_modules'] = activityList
        logger.debug(activityList)
    return currentUser
