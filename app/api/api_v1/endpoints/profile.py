from fastapi import APIRouter, Request, Response, Depends

from ....crud import profile

router = APIRouter()


@router.on_event('startup')
async def setup_collection():
    await profile.create_profile_collection()


@router.get('/data/module_operations', status_code=200)
async def get_module_operations(response: Response):
    result = await profile.get_data_module_operations()

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


@router.get('/data/import_records', status_code=200)
async def get_modules_for_import_records(response: Response):
    result = await profile.get_data_import_records()

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


@router.get('/data/export_records', status_code=200)
async def get_modules_for_export_records(response: Response):
    result = await profile.get_data_export_records()

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


@router.get('/data/convert', status_code=200)
async def get_modules_for_convert(response: Response):
    result = await profile.get_data_convert()

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


@router.get('/data/print_view', status_code=200)
async def get_modules_for_print_view(response: Response):
    result = await profile.get_data_print_view()

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


@router.post('', status_code=201)
async def create_profile(request_data: dict, response: Response):
    result = await profile.create_profile(request_data)

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
async def get_profiles_list(response: Response, start: int = 0, length: int = 10, search: str = ''):
    result = await profile.get_profiles_list_with_options(start, length, search)

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        return returnResponse

    returnResponse['profile_name'] = result['profile_name']
    returnResponse['recordsTotal'] = result['recordsTotal']
    return returnResponse


@router.get('/{record_id}', status_code=200)
async def get_profile(record_id: str, response: Response):
    result = await profile.get_profile(record_id)

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
async def update_profile(record_id: str, request_data: dict, response: Response):
    result = await profile.update_profile(record_id, request_data)

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
async def get_profiles_list_for_datatables(request: Request, response: Response):
    try:
        requestData: dict = await request.form()
        # logger.debug(requestData)
        start: str = requestData['start']
        length: str = requestData['length']
        sortingColumnNo: str = requestData['order[0][column]']
        sort: str = requestData['columns[' + sortingColumnNo + '][data]']
        order: str = requestData['order[0][dir]']
        search: str = requestData['search[value]']

        result = await profile.get_profiles_list_for_datatables(
            int(start), int(length), sort, order, search)

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


@router.get('/{profile_name}/users', status_code=200)
async def get_users_list_by_profile(profile_name: str, response: Response):
    result = await profile.get_users_list_by_profile(profile_name)

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

    returnResponse['records'] = result['records']
    returnResponse['recordsTotal'] = result['recordsTotal']
    return returnResponse
