from fastapi import APIRouter, Request, Response, Depends
import json

from ....crud import user
from ....core.config import logger
from ... import deps

router = APIRouter()


@router.on_event('startup')
async def setup_collection():
    await user.create_user_collection()


@router.post('', status_code=201)
async def create_user(request_data: dict, response: Response,
                      current_user: dict = Depends(deps.get_current_active_user)):
    result = await user.create_user(request_data, current_user)

    returnResponse = {}
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        returnResponse['message'] = result['message']
        return returnResponse

    returnResponse['id'] = result['id']
    returnResponse['message'] = result['message']
    return returnResponse


@router.get('/{record_id}', status_code=200)
async def get_user(record_id: str, response: Response,
                   current_user: dict = Depends(deps.get_current_active_user)):
    result = await user.get_user(record_id)

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


@router.put('/{record_id}', status_code=200)
async def update_user(record_id: str, request_data: dict, response: Response,
                      current_user: dict = Depends(deps.get_current_active_user)):
    result = await user.update_user(record_id, request_data)

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


@router.put('/{record_id}/change_status', status_code=200)
async def change_user_status(record_id: str, status: str, response: Response,
                             current_user: dict = Depends(deps.get_current_active_user)):
    result = await user.change_user_status(record_id, status)

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


@router.delete('/{record_id}', status_code=200)
async def delete_user(record_id: str, response: Response,
                      current_user: dict = Depends(deps.get_current_active_user)):
    result = await user.delete_user(record_id)

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


@router.get('', status_code=200)
async def get_users_list(response: Response, start: int = 0, length: int = 10,
                         search: str = ''):
    #  , current_user: dict = Depends(deps.get_current_active_user)):
    result = await user.get_users_with_less_options(start, length, search)

    returnResponse = {}
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        returnResponse['message'] = result['message']
        return returnResponse

    returnResponse['data'] = result['records']
    returnResponse['recordsTotal'] = result['recordsTotal']
    returnResponse['message'] = result['message']
    return returnResponse


@router.post('/records', status_code=200)
async def get_users_list_for_datatables(request: Request, response: Response,
                                        current_user: dict = Depends(deps.get_current_active_user)):
    try:
        requestData: dict = await request.form()
        logger.debug(requestData)
        start: str = requestData['start']
        length: str = requestData['length']
        sortingColumnNo: str = requestData['order[0][column]']
        sort: str = requestData['columns[' + sortingColumnNo + '][data]']
        order: str = requestData['order[0][dir]']
        search: str = requestData['search[value]']
        status = json.loads(requestData['status'])

        result = await user.get_users_with_options(int(start), int(length), [sort], order, search, status)

        returnResponse = {}
        if 'type' in result:
            if result['type'] == 'error' or result['type'] == 'exception':
                response.status_code = 400

            if result['type'] == 'exception':
                returnResponse['errorType'] = result['errorType']
                returnResponse['errorMessage'] = result['errorMessage']

            returnResponse['message'] = result['message']
            return returnResponse

        returnResponse['data'] = result['records']
        returnResponse['recordsFiltered'] = result['recordsFiltered']
        returnResponse['recordsTotal'] = result['recordsTotal']
        returnResponse['message'] = result['message']
        return returnResponse
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        response.status_code = 400
        return {'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}
