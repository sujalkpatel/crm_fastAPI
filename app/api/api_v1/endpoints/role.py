from fastapi import APIRouter, Request, Response, Depends

from ....crud import role

router = APIRouter()


@router.on_event('startup')
async def setup_collection():
    await role.create_role_collection()


@router.post('', status_code=201)
async def create_role(request_data: dict, response: Response):
    result = await role.create_role(request_data)

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


@router.get('', status_code=200)
async def get_roles_list(response: Response, start: int = 0, length: int = 10, search: str = ''):
    result = await role.get_roles_list_with_options(start, length, search)

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        return returnResponse

    returnResponse['role_name'] = result['role_name']
    returnResponse['recordsTotal'] = result['recordsTotal']
    return returnResponse


@router.get('/parent', status_code=200)
async def get_parent_roles_list(response: Response, start: int = 0, length: int = 10, search: str = ''):
    result = await role.get_parent_roles_list_with_options(start, length, search)

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        return returnResponse

    returnResponse['role_name'] = result['role_name']
    returnResponse['recordsTotal'] = result['recordsTotal']
    return returnResponse


@router.get('/records', status_code=200)
async def get_roles_in_tree_structure(response: Response):
    result = await role.get_roles_in_tree_structure()

    returnResponse = {}
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        returnResponse['message'] = result['message']
        return returnResponse

    returnResponse['structured_roles'] = result['structured_roles']
    returnResponse['message'] = result['message']
    return returnResponse


@router.get('/{record_id}', status_code=200)
async def get_role(record_id: str, response: Response):
    result = await role.get_role(record_id)

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
async def update_role(record_id: str, request_data: dict, response: Response):
    result = await role.update_role(record_id, request_data)

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
async def delete_role(role_to_delete: str, role_to_transfer: str, response: Response):
    result = await role.delete_role(role_to_delete, role_to_transfer)

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

    return returnResponse
