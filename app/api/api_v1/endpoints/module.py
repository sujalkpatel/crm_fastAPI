from fastapi import APIRouter, Request, Response, Depends, Form
import json

from ....crud import module, local_data
from ....core.config import logger
from ... import deps

router = APIRouter()


@router.on_event('startup')
async def load_data():
    await local_data.load_logger()
    await module.create_module_collections()
    await local_data.load_local_data()


@router.get('/{module_name}/field', status_code=200)
async def get_module_fields_list(module_name: str, response: Response):
    try:
        result = await module.get_module_fields_list(module_name)
        return result
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        response.status_code = 400
        return {'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


@router.post('/{module_name}', status_code=201)
async def create_new_module_record(module_name: str, request_data: dict, response: Response):
    try:
        result = await module.create_new_module_record(module_name, request_data['new_record'])
        if 'type' in result and result['type'] == 'error':
            response.status_code = 400
            return {'message': result['message']}

        return {'message': result['message'], 'id': result['id']}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        response.status_code = 400
        return {'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


@router.post('/{module_name}/records', status_code=200)
async def get_records_list_for_datatables(module_name: str, request: Request,
                                          response: Response,
                                          current_user: dict = Depends(deps.verify_get_acess_with_profile)):
    try:
        requestData: dict = await request.form()
        start: str = requestData['start']
        length: str = requestData['length']
        sortingColumnNo: str = requestData['order[0][column]']
        sortBy: str = requestData['columns[' + sortingColumnNo + '][data]']
        sortOrder: str = requestData['order[0][dir]']
        search: str = requestData['search[value]']
        record_id = json.loads(requestData['record_id'])

        result = await module.get_records_list_with_options(
            module_name, int(start), int(length), [sortBy], sortOrder, search, record_id)

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


@router.get('/{module_name}', status_code=200)
async def get_records_list(module_name: str, response: Response, start: int = 0,
                           length: int = 10, search: str = '',
                           current_user: dict = Depends(deps.verify_get_acess_with_profile)):
    result = await module.get_records_list_with_less_options(module_name, start, length, search)

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


@router.get('/{module_name}/{record_id}', status_code=200)
async def get_record(module_name: str, record_id: str, response: Response,
                     current_user: dict = Depends(deps.verify_get_acess_with_profile)):
    result = await module.get_record(module_name, record_id)

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


@router.put('/{module_name}/{record_id}', status_code=200)
async def update_record(module_name: str, record_id: str, request_data: dict, response: Response):
    result = await module.update_record(module_name, record_id, request_data)

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


@router.delete('/{module_name}/{record_id}', status_code=200)
async def delete_record(module_name: str, record_id: str, response: Response):
    result = await module.delete_record(module_name, record_id)

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


@router.post('/{module_name}/{record_id}/relationship', status_code=201)
async def create_relationship(module_name: str, record_id: str, request_data: dict, response: Response):
    result = await module.add_relationship(module_name, record_id, request_data)

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


@router.delete('/{module_name}/{record_id}/relationship', status_code=200)
async def delete_relationship(module_name: str, record_id: str, request_data: dict, response: Response):
    result = await module.delete_relationship(module_name, record_id, request_data)

    returnResponse = {}
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

    returnResponse['message'] = result['message']
    return returnResponse
