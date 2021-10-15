from fastapi import APIRouter, Request, Response, Depends

from ....crud import group
from ....core.config import logger

router = APIRouter()


@router.on_event('startup')
async def setup_collection():
    await group.create_group_collection()


@router.post('', status_code=201)
async def create_group(request_data: dict, response: Response):
    result = await group.create_group(request_data)

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        return returnResponse

    returnResponse['id'] = result['id']
    return returnResponse


@router.get('', status_code=200)
async def get_groups_list(response: Response, start: int = 0, length: int = 10, search: str = ''):
    result = await group.get_groups_list_with_options(start, length, search)

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        return returnResponse

    returnResponse['group_name'] = result['group_name']
    returnResponse['recordsTotal'] = result['recordsTotal']
    return returnResponse


@router.get('/{record_id}', status_code=200)
async def get_group(record_id: str, response: Response):
    result = await group.get_group(record_id)

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        return returnResponse

    returnResponse['record'] = result['record']
    return returnResponse


@router.put('/{record_id}', status_code=200)
async def update_group(record_id: str, request_data: dict, response: Response):
    result = await group.update_group(record_id, request_data)

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

    return returnResponse


@router.delete('/{record_id}', status_code=200)
async def delete_group(record_id: str, response: Response):
    result = await group.delete_group(record_id)

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

    return returnResponse


@router.post('/records', status_code=200)
async def get_groups_list_for_datatable(request: Request, response: Response):
    try:
        requestData: dict = await request.form()
        # logger.debug(requestData)
        start: str = requestData['start']
        length: str = requestData['length']
        sortingColumnNo: str = requestData['order[0][column]']
        sort: str = requestData['columns[' + sortingColumnNo + '][data]']
        order: str = requestData['order[0][dir]']
        search: str = requestData['search[value]']

        result = await group.get_groups_list_for_datatable(
            int(start), int(length), [sort], order, search)

        returnResponse = {}
        returnResponse['message'] = result['message']
        if 'type' in result:
            if result['type'] == 'error' or result['type'] == 'exception':
                response.status_code = 400

            if result['type'] == 'exception':
                returnResponse['errorType'] = result['errorType']
                returnResponse['errorMessage'] = result['errorMessage']

            return returnResponse

        returnResponse['data'] = result['records']
        returnResponse['recordsFiltered'] = result['recordsFiltered']
        returnResponse['recordsTotal'] = result['recordsTotal']
        return returnResponse
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        response.status_code = 400
        return {'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}
