from bson.decimal128 import Decimal128
from bson.binary import Binary
from bson.objectid import ObjectId
import bson
import datetime
import base64

from ..core.config import (logger, database, collectionModuleFields,
                           moduleTypeFields, moduleFieldTypes,
                           moduleUniqueFields, moduleBsonTypeFields,
                           mongoClient)
from ..db import setup_collection
from . import attachment


async def create_module_collections():
    await setup_collection.create_module_collections()


async def get_module_list():
    result = {'modules': []}

    async for data in collectionModuleFields.find({'type': 'module'}, projection={
            'module_name': 1, 'module_label': 1, '_id': 0}):
        result['modules'].append(data)

    logger.debug(result)

    return result


async def get_module_fields_list(moduleName: str) -> dict:
    moduleFieldsDoc = await collectionModuleFields.find_one(
        {'type': 'module', 'module_name': moduleName},
        projection={'module_fields': 1, 'module_sections': 1, '_id': 0})

    if moduleFieldsDoc is None:
        return {
            'type': 'error',
            'message': 'Module ' + moduleName + ' is not present in the system.'
        }

    result = {}
    result['module_fields'] = []
    result['module_sections'] = []
    if moduleFieldsDoc is not None:
        if 'module_fields' in moduleFieldsDoc:
            result['module_fields'] = moduleFieldsDoc['module_fields']

        if 'module_sections' in moduleFieldsDoc:
            result['module_sections'] = moduleFieldsDoc['module_sections']

    return result


async def get_field_value(moduleField: dict, requestData: dict) -> any:
    fieldValue = None

    if moduleField['bson_type'] == 'int':
        fieldValue = 0
        if moduleField['field_name'] in requestData:
            fieldValue = requestData[moduleField['field_name']]

    elif moduleField['bson_type'] == 'bool':
        fieldValue = False
        if moduleField['field_name'] in requestData:
            fieldValue = requestData[moduleField['field_name']]

    elif moduleField['bson_type'] == 'decimal':
        fieldValue = Decimal128('0')
        if moduleField['field_name'] in requestData:
            fieldValue = Decimal128(
                requestData[moduleField['field_name']])

    elif moduleField['bson_type'] == 'date':
        fieldValue = datetime.datetime(1, 1, 1)
        if moduleField['field_name'] in requestData:
            fieldValue = datetime.datetime.fromisoformat(
                requestData[moduleField['field_name']])

    elif moduleField['bson_type'] == 'binData':
        if moduleField['field_name'] in requestData:
            fieldValue = Binary(base64.b64decode(
                requestData[moduleField['field_name']]))

    elif moduleField['bson_type'] == 'object':
        fieldValue = {}
        objectRequestData = {}

        if moduleField['field_name'] in requestData:
            objectRequestData = requestData[moduleField['field_name']]

        # Special case to handle in Activity Module
        if 'module_fields_required' in moduleField:
            if moduleField['field_name'] == 'record':
                if requestData['related_to']['module_name'] not in moduleField['module_fields_required']:
                    raise ValueError(
                        'Related To Module (' + requestData['related_to']['module_name'] + ') is not valid for Record.')

                for objectField in moduleField['field_object_attribute']:
                    if objectField['field_name'] not in moduleField['module_fields_required'][requestData['related_to']['module_name']]:
                        continue
                    fieldValue[objectField['field_name']] = await get_field_value(
                        objectField, objectRequestData)

            elif moduleField['field_name'] == 'contact_lead':
                if requestData['contact_lead_type'] not in moduleField['module_fields_required']:
                    raise ValueError(
                        'Contact Type (' + requestData['contact_lead_type'] + ') is not valid for Contact.')

                for objectField in moduleField['field_object_attribute']:
                    if objectField['field_name'] not in moduleField['module_fields_required'][requestData['contact_lead_type']]:
                        continue
                    fieldValue[objectField['field_name']] = await get_field_value(
                        objectField, objectRequestData)
        # Normal case
        else:
            for objectField in moduleField['field_object_attribute']:
                fieldValue[objectField['field_name']] = await get_field_value(
                    objectField, objectRequestData)

    elif moduleField['bson_type'] == 'array':
        fieldValue = []
        arrayRequestData = []
        if moduleField['field_name'] in requestData:
            arrayRequestData = requestData[moduleField['field_name']]
        if len(moduleField['field_array_element']) > 1 and len(arrayRequestData) > 0:
            for elementData in arrayRequestData:
                objectData = {}
                for objectField in moduleField['field_array_element']:
                    objectData[objectField['field_name']] = await get_field_value(objectField, elementData)

                fieldValue.append(objectData)
        else:
            fieldValue = arrayRequestData

    else:
        fieldValue = ''
        if moduleField['field_name'] in requestData:
            fieldValue = requestData[moduleField['field_name']]

    return fieldValue


async def create_new_module_record(moduleName: str, requestData: dict) -> dict:
    logger.debug('in post.')
    insertDocument = {}
    moduleFieldsDoc = await collectionModuleFields.find_one(
        {'type': 'module', 'module_name': moduleName},
        projection={'module_fields': 1, '_id': 0}
    )

    if moduleFieldsDoc is None:
        return {
            'type': 'error',
            'message': 'Module ' + moduleName + ' is not present in the system.'
        }

    moduleFields = moduleFieldsDoc['module_fields']

    if moduleName == 'Activity':
        insertDocument['activity_type'] = requestData['activity_type']

        if insertDocument['activity_type'] == 'Task':
            moduleFields = moduleFields[1]['activity_fields']['task_fields']

        elif insertDocument['activity_type'] == 'Event':
            moduleFields = moduleFields[1]['activity_fields']['event_fields']

        elif insertDocument['activity_type'] == 'Call':
            moduleFields = moduleFields[1]['activity_fields']['call_fields']

        else:
            return {
                'type': 'error',
                'message': 'Activity type is not valid.'
            }

    for moduleField in moduleFields:
        if moduleField['field_name'] == 'created_by':
            insertDocument['created_by'] = 'system_default'
            continue
        if moduleField['field_name'] == 'created_at':
            insertDocument['created_at'] = datetime.datetime.now()
            continue
        if moduleField['field_name'] == 'modified_by':
            insertDocument['modified_by'] = 'system_default'
            continue
        if moduleField['field_name'] == 'modified_at':
            insertDocument['modified_at'] = datetime.datetime.now()
            continue

        if moduleField['field_required'] \
                and moduleField['field_name'] not in requestData:
            return {'type': 'error', 'message': moduleField['field_name'] + ' is required.'}

        insertDocument[moduleField['field_name']
                       ] = await get_field_value(moduleField, requestData)

    logger.debug(insertDocument)
    collection = database.get_collection('module_' + moduleName)
    result = await collection.insert_one(insertDocument)

    return {'message': 'New ' + moduleName + ' record created.', 'id': str(result.inserted_id)}


async def add_attachment(moduleName: str, recordId: str, fileId: str, fileName: str):
    collection = database.get_collection('module_' + moduleName)

    updatedDoc = await collection.update_one({'_id': ObjectId(recordId)},
                                             {'$push': {'attachments': {'file_id': fileId, 'file_name': fileName}},
                                              '$set': {'modified_by': 'system_update', 'modified_at': datetime.datetime.now()}})

    if updatedDoc:
        return {'message': str(updatedDoc.modified_count) + ' ' + moduleName + ' record has been updated.', 'updatedDocumentCount': updatedDoc.modified_count}

    return {'type': 'error', 'message': 'Attachment could not be added to ' + moduleName + ' record.'}


async def convert_field_value_for_get(moduleField: dict, record: dict) -> any:
    fieldValue = record[moduleField['field_name']]

    if moduleField['bson_type'] == 'decimal':
        fieldValue = str(fieldValue)

    elif moduleField['bson_type'] == 'date':
        fieldValue = fieldValue.isoformat()

    elif moduleField['bson_type'] == 'binData':
        fieldValue = base64.b64encode(fieldValue)

    elif moduleField['bson_type'] == 'object':
        objectRecord = fieldValue

        # Special case to handle in Activity Module
        if 'module_fields_required' in moduleField:
            if moduleField['field_name'] == 'record':
                for objectField in moduleField['field_object_attribute']:
                    if objectField['field_name'] not in moduleField['module_fields_required'][record['related_to']['module_name']]:
                        continue
                    fieldValue[objectField['field_name']] = await convert_field_value_for_get(
                        objectField, objectRecord)

            elif moduleField['field_name'] == 'contact_lead':
                for objectField in moduleField['field_object_attribute']:
                    if objectField['field_name'] not in moduleField['module_fields_required'][record['contact_lead_type']]:
                        continue
                    fieldValue[objectField['field_name']] = await convert_field_value_for_get(
                        objectField, objectRecord)

        # Normal case
        else:
            for objectField in moduleField['field_object_attribute']:
                fieldValue[objectField['field_name']] = await convert_field_value_for_get(
                    objectField, objectRecord)

    elif moduleField['bson_type'] == 'array':
        if len(moduleField['field_array_element']) > 1:
            index = 0
            while index < len(fieldValue):
                for objectField in moduleField['field_array_element']:
                    fieldValue[index][objectField['field_name']] = await convert_field_value_for_get(objectField, fieldValue[index])
                index += 1

    return fieldValue


async def get_record(moduleName: str, recordId: str):
    try:
        if moduleName not in moduleUniqueFields:
            return {
                'type': 'error',
                'message': 'Module ' + moduleName + ' is not present in the system.'
            }

        collection = database.get_collection('module_' + moduleName)

        resultDoc = await collection.find_one({'_id': ObjectId(recordId)})

        if resultDoc is None:
            return {'type': 'error', 'message': 'No relevant ' + moduleName + ' record found for the provided record Id.'}

        moduleFieldsDoc = await collectionModuleFields.find_one(
            {'type': 'module', 'module_name': moduleName},
            projection={'module_fields': 1, '_id': 0})

        moduleFields = moduleFieldsDoc['module_fields']

        if moduleName == 'Activity':
            if resultDoc['activity_type'] == 'Task':
                moduleFields = moduleFields[1]['activity_fields']['task_fields']
            elif resultDoc['activity_type'] == 'Event':
                moduleFields = moduleFields[1]['activity_fields']['event_fields']
            else:
                moduleFields = moduleFields[1]['activity_fields']['call_fields']

        for moduleField in moduleFields:
            resultDoc[moduleField['field_name']] = await convert_field_value_for_get(moduleField, resultDoc)

        resultDoc['_id'] = str(resultDoc['_id'])

        logger.debug(resultDoc)

        return {'record': resultDoc, 'message': 'A ' + moduleName + ' record retrieved successfully.'}

    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {
            'type': 'exception',
            'errorType': str(type(e).__name__),
            'errorMessage': str(e),
            'message': 'An error has occured.'
        }


async def get_records_list_with_options(moduleName: str, start: int, length: int,
                                        sortBy: list, sortOrder: str, search: str,
                                        recordIds: list, currentUser: dict = {}):
    try:
        collection = database.get_collection('module_' + moduleName)
        moduleFieldsDoc = await collectionModuleFields.find_one(
            {'type': 'module', 'module_name': moduleName},
            projection={'module_fields': 1, '_id': 0})

        if moduleFieldsDoc is None:
            return {
                'type': 'error',
                'message': 'Module ' + moduleName + ' is not present in the system.'
            }

        query = {}
        results = {'records': []}
        sortFields = []

        if recordIds and len(recordIds) > 0:
            listRecordId = [ObjectId(recordId) for recordId in recordIds]
            query['_id'] = {'$in': listRecordId}

        if len(search) > 0:
            query['$or'] = []
            for stringField in moduleBsonTypeFields[moduleName]['string']:
                query['$or'].append(
                    {stringField: {'$regex': '^' + search, '$options': 'i'}})

            if search.isdigit():
                for intField in moduleBsonTypeFields[moduleName]['int']:
                    query['$or'].append(
                        {intField: {'$eq': int(search)}})

            if search.replace('.', '', 1).isdigit():
                for intField in moduleBsonTypeFields[moduleName]['decimal']:
                    query['$or'].append(
                        {intField: {'$eq': Decimal128(search)}})

        if sortBy and len(sortBy) > 0:
            i = 0
            while i < len(sortBy):
                if i == 0:
                    sortFields.append(
                        (sortBy[i], -1 if sortOrder == 'desc' else 1))
                else:
                    sortFields.append((sortBy[i], 1))
                i += 1

        # logger.debug(query)
        recordCount = await collection.count_documents(query)
        records = collection.find(query).skip(start).limit(
            length).sort(sortFields)

        if moduleName == 'Activity':
            for record in await records.to_list(length):
                if record['activity_type'] == 'Task':
                    moduleFields = moduleFieldsDoc['module_fields'][1]['activity_fields']['task_fields']
                elif record['activity_type'] == 'Event':
                    moduleFields = moduleFieldsDoc['module_fields'][1]['activity_fields']['event_fields']
                else:
                    moduleFields = moduleFieldsDoc['module_fields'][1]['activity_fields']['call_fields']

                for moduleField in moduleFields:
                    record[moduleField['field_name']] = await convert_field_value_for_get(moduleField, record)
                record['_id'] = str(record['_id'])
                results['records'].append(record)

        else:
            for record in await records.to_list(length):
                for moduleField in moduleFieldsDoc['module_fields']:
                    record[moduleField['field_name']] = await convert_field_value_for_get(moduleField, record)
                record['_id'] = str(record['_id'])
                results['records'].append(record)

        # logger.debug(results)

        if len(results['records']) > 0:
            results['message'] = 'Records retrieved successfully.'
        else:
            results['message'] = 'No record found.'

        results['recordsFiltered'] = recordCount
        results['recordsTotal'] = recordCount

        return results

    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {
            'type': 'exception',
            'errorType': str(type(e).__name__),
            'errorMessage': str(e),
            'message': 'An error has occured.'
        }


async def get_records_list_with_less_options(moduleName: str, start: int, length: int, search: str) -> dict:
    try:
        if moduleName not in moduleUniqueFields:
            return {
                'type': 'error',
                'message': 'Module ' + moduleName + ' is not present in the system.'
            }

        return await get_records_list_with_options(moduleName, start, length, moduleUniqueFields[moduleName], 'asc', search, [])
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {
            'type': 'exception',
            'errorType': str(type(e).__name__),
            'errorMessage': str(e),
            'message': 'An error has occured.'
        }


async def update_stage_history_for_deal(recordId: str, updateData: dict):
    collection = database.get_collection('module_Deal')
    dealData = await collection.find_one(
        {'_id': ObjectId(recordId)},
        projection={
            'stage_history': 1,
            'stage': 1,
            'amount': 1,
            'probability': 1,
            'expected_revenue': 1,
            'closing_date': 1,
            'created_at': 1
        }
    )

    if dealData is None:
        return {
            'type': 'error',
            'message': 'No matching Lead record found to be updated.'
        }

    stageHistory = dealData['stage_history']

    if dealData['stage'] != updateData['stage']:
        stageDuration = 0

        if len(stageHistory) == 0:
            stageDuration = (datetime.datetime.now().date() -
                             dealData['created_at'].date()).days
        else:
            stageDuration = (datetime.datetime.now().date() -
                             stageHistory[len(stageHistory) - 1]['modified_time'].date()).days

        stageHistory.append(
            {
                'stage': dealData['stage'],
                'amount': dealData['amount'],
                'probability': dealData['probability'],
                'expected_revenue': dealData['expected_revenue'],
                'closing_date': dealData['closing_date'],
                'stage_duration': stageDuration,
                'modified_time': updateData['modified_at'],
                'modified_by': 'system_update'
            }
        )

    updateData['stage_history'] = stageHistory


async def update_record(moduleName: str, recordId: str, requestData: dict) -> dict:
    try:
        logger.debug('in put.')
        requestData = requestData['updated_record']
        modifiedBy = 'system_update'
        modifiedAt = datetime.datetime.now()
        updateDocument = {}
        moduleFieldsDoc = await collectionModuleFields.find_one({'type': 'module', 'module_name': moduleName}, projection={'module_fields': 1, '_id': 0})

        if moduleFieldsDoc is None:
            return {
                'type': 'error',
                'message': 'Module ' + moduleName + ' is not present in the system.'
            }

        moduleFields = moduleFieldsDoc['module_fields']

        if moduleName == 'Activity':
            collection = database.get_collection('module_' + moduleName)
            activityDoc = await collection.find_one(
                {'_id': ObjectId(recordId)},
                projection={
                    'activity_type': 1
                }
            )

            if activityDoc is None:
                return {'type': 'error', 'message': 'No matching ' + moduleName + ' record found to be updated.'}
            if activityDoc['activity_type'] == 'Task':
                moduleFields = moduleFieldsDoc['module_fields'][1]['activity_fields']['task_fields']
            elif activityDoc['activity_type'] == 'Event':
                moduleFields = moduleFieldsDoc['module_fields'][1]['activity_fields']['event_fields']
            else:
                moduleFields = moduleFieldsDoc['module_fields'][1]['activity_fields']['call_fields']

        for moduleField in moduleFields:
            if moduleField['field_name'] == 'modified_by':
                updateDocument['modified_by'] = modifiedBy
                continue
            if moduleField['field_name'] == 'modified_at':
                updateDocument['modified_at'] = modifiedAt
                continue

            # Skip fields not provided in the request and created_by and created_at
            if moduleField['field_name'] not in requestData \
                    or moduleField['field_name'] in {'created_by', 'created_at'}:
                continue

            updateDocument[moduleField['field_name']
                           ] = await get_field_value(moduleField, requestData)

        if moduleName == 'Deal':
            result = await update_stage_history_for_deal(recordId, updateDocument)
            if result:
                return result

        logger.debug(updateDocument)
        collection = database.get_collection('module_' + moduleName)
        updateDocument.pop('modified_by')
        updateDocument.pop('modified_at')

        if not updateDocument:
            return {
                'type': 'error',
                'message': 'At least one field required to update ' + moduleName + ' record.'
            }

        async with await mongoClient.start_session() as transactionSession:
            async with transactionSession.start_transaction():
                result = await collection.update_one(
                    {'_id': ObjectId(recordId)},
                    {'$set': updateDocument},
                    session=transactionSession
                )

                if result.modified_count == 1:
                    result = await collection.update_one(
                        {'_id': ObjectId(recordId)},
                        {'$set': {
                            'modified_by': modifiedBy,
                            'modified_at': modifiedAt
                        }},
                        session=transactionSession
                    )

        if result.matched_count == 0:
            return {'type': 'error', 'message': 'No matching ' + moduleName + ' record found to be updated.'}

        return {'message': str(result.modified_count) + ' ' + moduleName + ' record updated.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def delete_record(moduleName: str, recordId: str) -> dict:
    try:
        if moduleName not in moduleUniqueFields:
            return {
                'type': 'error',
                'message': 'Module ' + moduleName + ' is not present in the system.'
            }

        collection = database.get_collection('module_' + moduleName)

        resultDoc = await collection.find_one_and_delete({'_id': ObjectId(recordId)})

        if resultDoc is None:
            return {'type': 'error', 'message': 'No relevant ' + moduleName + ' record found for the provided record Id to be deleted.'}

        for attachmentFile in resultDoc['attachments']:
            await attachment.delete_attachment(attachmentFile['file_id'])

        # logger.debug(resultDoc)

        return {'message': 'A ' + moduleName + ' record deleted successfully.'}

    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def delete_attchment_from_api(moduleName: str, recordId: str, fileId: str) -> dict:
    collection = database.get_collection('module_' + moduleName)

    resultDoc = await collection.update_one({'_id': ObjectId(recordId)}, {
        '$pull': {'attachments': {'file_id': fileId}}})

    if resultDoc is None or resultDoc.modified_count == 0:
        return {'type': 'error', 'message': 'the reference of given file not found/deleted in the provided ' + moduleName + ' record.'}

    return {'message': 'The given file removed from the provided ' + moduleName + ' record.'}


async def add_relationship(moduleName: str, recordId: str, requestData: dict) -> dict:
    try:
        if moduleName not in moduleUniqueFields:
            return {
                'type': 'error',
                'message': 'Module ' + moduleName + ' is not present in the system.'
            }

        logger.debug(moduleFieldTypes[moduleName])
        if requestData['field_name'] not in moduleFieldTypes[moduleName]:
            return {'type': 'error', 'message': requestData['field_name'] + ' is not present in ' + moduleName + '.'}

        collection = database.get_collection('module_' + moduleName)

        updatedDoc = await collection.update_one({'_id': ObjectId(recordId)},
                                                 {'$push': {requestData['field_name']: requestData['related_record_id']},
                                                  '$set': {'modified_by': 'system_update', 'modified_at': datetime.datetime.now()}})

        if updatedDoc and updatedDoc.modified_count == 1:
            return {'message': str(updatedDoc.modified_count) + ' ' + moduleName + ' record has been updated.'}

        return {'type': 'error', 'message': 'A ' + requestData['field_name'] + ' record could not be added to ' + moduleName + ' record.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def delete_relationship(moduleName: str, recordId: str, requestData: dict) -> dict:
    try:
        if moduleName not in moduleUniqueFields:
            return {
                'type': 'error',
                'message': 'Module ' + moduleName + ' is not present in the system.'
            }

        if requestData['field_name'] not in moduleFieldTypes[moduleName]:
            return {'type': 'error', 'message': requestData['field_name'] + ' is not present in ' + moduleName + '.'}

        collection = database.get_collection('module_' + moduleName)

        resultDoc = await collection.update_one({'_id': ObjectId(recordId)}, {
            '$pull': {requestData['field_name']: requestData['related_record_id']}})

        if resultDoc is None or resultDoc.modified_count == 0:
            return {'type': 'error', 'message': 'the reference of given ' + requestData['field_name'] + ' record not found/deleted in the provided ' + moduleName + ' record.'}

        return {'message': 'The given ' + requestData['field_name'] + ' record removed from the provided ' + moduleName + ' record.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}
