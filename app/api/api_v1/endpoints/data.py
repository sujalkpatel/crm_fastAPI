from fastapi import APIRouter, Response

from ....crud import data

router = APIRouter()


@router.get('/{data_type}', status_code=200)
async def get_data(data_type: str, response: Response):
    result = await data.get_data(data_type)

    if 'type' in result:
        returnResponse = {}
        if result['type'] == 'error' or result['type'] == 'exception':
            response.status_code = 400

        if result['type'] == 'exception':
            returnResponse['errorType'] = result['errorType']
            returnResponse['errorMessage'] = result['errorMessage']

        returnResponse['message'] = result['message']
        return returnResponse

    return result
