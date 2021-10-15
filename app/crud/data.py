from ..core.config import collectionData, logger


async def get_data(type: str) -> dict:
    try:
        if type == 'country':
            resultDoc = await collectionData.find_one({'type': type})

            return {'country_name': resultDoc['value'], 'message': 'The list of countries retrieved successfully.'}
        return {'message': 'No data found.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}
