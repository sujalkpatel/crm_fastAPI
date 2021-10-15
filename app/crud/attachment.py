import base64
import datetime
from bson.objectid import ObjectId

from ..core.config import logger, gridFs
from . import module


async def post_attachment_new(moduleName: str, requestData: dict) -> dict:
    fileId = None
    try:
        if 'file_name' not in requestData \
            or 'file_type' not in requestData \
            or 'file_content' not in requestData \
                or 'record_id' not in requestData:
            return {'type': 'error', 'message': 'file_name, file_type, file_content and record_id are required.'}

        fileName = requestData['file_name']
        fileType = requestData['file_type']
        fileContent = base64.b64decode(requestData['file_content'])
        recordId = requestData['record_id']
        createdBy = 'system-default'

        metaData = {'module': moduleName, 'type': fileType,
                    'record': recordId, 'created_by': createdBy}

        fileId = await gridFs.upload_from_stream(fileName, fileContent, metadata=metaData)

        result = await module.add_attachment(moduleName, recordId, str(fileId), fileName)

        if result['updatedDocumentCount'] == 0:
            await gridFs.delete(fileId)
            return {'type': 'error', 'message': 'No related ' + moduleName + ' record found for the attachment.'}

        return {'message': result['message']}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        if fileId is not None:
            await gridFs.delete(fileId)
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def delete_attachment(fileId: str):
    await gridFs.delete(ObjectId(fileId))


async def delete_attchment_from_api(moduleName: str, fileId: str):
    try:
        cursor = gridFs.find({'_id': ObjectId(fileId)})

        if cursor is None:
            return {'type': 'error', 'message': 'No related ' + moduleName + ' attachment found for the given Id to be deleted.'}

        for fileDoc in await cursor.to_list(1):
            moduleResult = await module.delete_attchment_from_api(moduleName, fileDoc['metadata']['record'], fileId)

        await gridFs.delete(ObjectId(fileId))

        if 'type' in moduleResult:
            return {'message': 'The file is removed but ' + moduleResult['message']}

        return {'message': 'The file is removed.'}

    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}
