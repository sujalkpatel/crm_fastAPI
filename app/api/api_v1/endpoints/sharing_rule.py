from fastapi import APIRouter, Request, Response, Depends

from ....crud import sharing_rule
from ....core.config import logger

router = APIRouter()


@router.on_event('startup')
async def setup_collection():
    await sharing_rule.create_sharing_rule_collection()


@router.post('', status_code=201)
async def create_sharing_rule(request_data: dict, response: Response):
    result = await sharing_rule.create_sharing_rule(request_data)

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
async def get_sharing_rules_list(response: Response):
    result = await sharing_rule.get_sharing_rules_list()

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        return returnResponse

    returnResponse['sharing_rules'] = result['sharing_rules']
    return returnResponse


@router.get('/{record_id}', status_code=200)
async def get_sharing_rule(record_id: str, response: Response):
    result = await sharing_rule.get_sharing_rule(record_id)

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
async def update_sharing_rule(record_id: str, request_data: dict, response: Response) -> Response:
    result = await sharing_rule.update_sharing_rule(record_id, request_data)

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
async def delete_sharing_rule(record_id: str, response: Response):
    result = await sharing_rule.delete_sharing_rule(record_id)

    returnResponse = {}
    returnResponse['message'] = result['message']
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

    return returnResponse
