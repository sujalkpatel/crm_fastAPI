from fastapi import APIRouter, Request, Response, Depends

from ....crud import company
from ... import deps

router = APIRouter()


@router.on_event('startup')
async def setup_collection():
    await company.create_company_collection()


@router.get('', status_code=200)
async def get_company(response: Response):
    result = await company.get_company_record()

    returnResponse = {}
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        returnResponse['message'] = result['message']
        return returnResponse

    returnResponse['record'] = result['record']
    returnResponse['message'] = result['message']
    return returnResponse


@router.put('', status_code=200)
async def update_company(request_data: dict, response: Response,
                         super_user: dict = Depends(deps.get_current_active_user)):
    result = await company.update_company_record(request_data)

    returnResponse = {}
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        returnResponse['message'] = result['message']
        return returnResponse

    returnResponse['message'] = result['message']
    return returnResponse
