from ..core.config import (logger,
                           collectionModuleFields,
                           collectionDefaultPermission)
from ..db import setup_collection

defaultAccess = {
    "Private",
    "Public Read Only",
    "Public Read/Write/Delete"
}


async def create_home_record():
    homeRecord = {}

    homeRecord['module'] = {}
    homeRecord['module']['module_name'] = 'Home'
    homeRecord['module']['module_label'] = 'Home'
    homeRecord['module']['module_label_plural'] = 'Home'

    homeRecord['default_access'] = 'Private'

    resultDoc = await collectionDefaultPermission.update_one(
        {'module.module_name': 'Home'},
        {'$setOnInsert': homeRecord},
        upsert=True
    )

    if resultDoc.upserted_id is None:
        logger.debug('Default Permission for Home record is present.')
    else:
        logger.debug(
            'Default Permission for Home record inserted. id: ' + str(resultDoc.upserted_id))


async def create_module_records():
    async for module_document in collectionModuleFields.find(
        {'type': 'module'},
        projection={
            'module_name': 1,
            'module_label': 1,
            'module_label_plural': 1,
            'default_permission': 1
        }
    ):
        moduleRecord = {}
        moduleRecord['module'] = {}
        moduleRecord['module']['module_name'] = module_document['module_name']
        moduleRecord['module']['module_label'] = module_document['module_label']
        moduleRecord['module']['module_label_plural'] = module_document['module_label_plural']

        moduleRecord['default_access'] = module_document['default_permission']

        resultDoc = await collectionDefaultPermission.update_one(
            {'module.module_name': module_document['module_name']},
            {'$setOnInsert': moduleRecord},
            upsert=True
        )

        if resultDoc.upserted_id is None:
            logger.debug('Default Permission for ' +
                         module_document['module_name'] + ' record is present.')
        else:
            logger.debug(
                'Default Permission for ' + module_document['module_name'] + ' record inserted. id: ' + str(resultDoc.upserted_id))


async def create_default_permission_records():
    await create_home_record()
    await create_module_records()


async def create_default_permission_collection():
    await setup_collection.create_collection('default_permission')
    await create_default_permission_records()


async def get_default_permissions() -> dict:
    try:
        records = []

        async for permissionDoc in collectionDefaultPermission.find():
            permissionDoc['_id'] = str(permissionDoc['_id'])
            records.append(permissionDoc)

        return {'records': records, 'message': 'Default permissions retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def update_default_permissions(requestData: dict) -> dict:
    try:
        updateCount = 0
        for permissionData in requestData['updated_records']:
            if permissionData['default_access'] not in defaultAccess:
                return {'type': 'error', 'message': 'Access type is not valid for ' +
                        permissionData['module']['module_label_plural'] + '.'}

            result = await collectionDefaultPermission.update_one(
                {'module.module_name': permissionData['module']
                    ['module_name']},
                {'$set': {
                    'default_access': permissionData['default_access']
                }}
            )
            updateCount += result.modified_count

        return {'message': 'Default permissions have been updated for ' + str(updateCount) + ' modules.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}
