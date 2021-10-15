from fastapi import APIRouter, Request, Response, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm

from ....crud import user
from ....core import security
from ....core.config import logger

router = APIRouter()


@router.post('/new_password')
async def save_new_password(form_data: OAuth2PasswordRequestForm = Depends()):
    result = await user.save_new_password(form_data.username, form_data.password)
    if 'type' in result and result['type'] == 'error':
        raise HTTPException(status_code=400, detail=result['message'])

    return result


@router.post('/access_token')
async def create_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    userDoc = await user.authenticate_user(form_data.username, form_data.password)

    if userDoc is None:
        raise HTTPException(
            status_code=400, detail="Incorrect email or password.")

    elif userDoc['status'] != 'Active':
        raise HTTPException(
            status_code=400, detail="Inactive or Deleted user.")

    return {
        'access_token': security.create_access_token({'sub': userDoc['email']}),
        "token_type": "bearer"}
