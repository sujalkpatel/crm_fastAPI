from fastapi import APIRouter, Request, Response, Depends

from ....crud import territory

router = APIRouter()


@router.on_event('startup')
async def setup_collection():
    await territory.create_territory_collection()


@router.post('', status_code=201)
async def create_territory(request_data: dict, response: Response):
    result = await territory.create_territory(request_data)

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
async def get_territories_list(response: Response, start: int = 0, length: int = 10, search: str = ''):
    result = await territory.get_territories_list_with_options(start, length, search)

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        return returnResponse

    returnResponse['territory_name'] = result['territory_name']
    returnResponse['recordsTotal'] = result['recordsTotal']
    return returnResponse


@router.get('/parent', status_code=200)
async def get_parent_territories_list(response: Response, start: int = 0, length: int = 10, search: str = ''):
    result = await territory.get_parent_territories_list_with_options(start, length, search)

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        return returnResponse

    returnResponse['territory_name'] = result['territory_name']
    returnResponse['recordsTotal'] = result['recordsTotal']
    return returnResponse


@router.get('/records', status_code=200)
async def get_territories_in_tree_structure(response: Response):
    result = await territory.get_territories_in_tree_structure()

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        return returnResponse

    returnResponse['structured_territories'] = result['structured_territories']
    return returnResponse


@router.get('/run_rules', status_code=200)
async def run_rules(response: Response):
    result = await territory.run_rules()

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

    return returnResponse


@router.get('/{record_id}', status_code=200)
async def get_territory(record_id: str, response: Response):
    result = await territory.get_territory(record_id)

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
async def update_territory(record_id: str, request_data: dict, response: Response):
    result = await territory.update_territory(record_id, request_data)

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

    return returnResponse


@router.delete('', status_code=200)
async def delete_territory(response: Response, territory_to_delete: str, territory_to_transfer: str = ''):
    result = await territory.delete_territory(territory_to_delete, territory_to_transfer)

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

    return returnResponse
