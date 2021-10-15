from fastapi import APIRouter, Request, Response, Depends

from ....crud import default_permission

router = APIRouter()


@router.on_event('startup')
async def setup_collection():
    await default_permission.create_default_permission_collection()


@router.get('', status_code=200)
async def get_default_permissions_list(response: Response):
    result = await default_permission.get_default_permissions()

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        return returnResponse

    returnResponse['records'] = result['records']
    return returnResponse


@router.put('', status_code=200)
async def update_default_permissions(request_data: dict, response: Response):
    result = await default_permission.update_default_permissions(request_data)

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

    return returnResponse
