from fastapi import APIRouter, Request, Response, Depends

from ....core.config import logger
from ....crud import attachment

router = APIRouter()


async def verify_module_access(request: Request, module_name: str):
    logger.debug('Request type: ' + request.method)
    logger.debug('Module: ' + module_name)


@router.post('/{module_name}', dependencies=[Depends(verify_module_access)], status_code=201)
async def create_attachment(module_name: str, request_data: dict, response: Response):
    result = await attachment.post_attachment_new(module_name, request_data['new_document'])

    returnResponse = {}
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

    returnResponse['message'] = result['message']
    return returnResponse


@router.delete('/{module_name}/{file_id}', dependencies=[Depends(verify_module_access)], status_code=200)
async def delete_attachment(module_name: str, file_id: str, response: Response):
    result = await attachment.delete_attchment_from_api(module_name, file_id)

    returnResponse = {}
    if 'type' in result:
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

    returnResponse['message'] = result['message']
    return returnResponse
