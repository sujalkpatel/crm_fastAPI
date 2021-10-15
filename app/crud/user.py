import datetime
from bson.objectid import ObjectId

from ..db import setup_collection
from ..core.config import logger, collectionModuleFields, collectionUser, collectionBsonTypeFields
from ..core import security


async def create_user_collection():
    await setup_collection.create_collection('user')


async def create_user(requestData: dict, currentUser: dict = None) -> dict:
    try:
        requestData = requestData['new_record']
        userData = {}
        userData['first_name'] = requestData['first_name']
        userData['last_name'] = requestData['last_name']
        userData['email'] = requestData['email']
        userData['role'] = requestData['role']
        userData['profile'] = requestData['profile']
        userData['territories'] = requestData['territories']

        userData['added_by'] = 'system_default' if not currentUser else currentUser['first_name'] + \
            ' ' + currentUser['last_name']
        userData['added_at'] = datetime.datetime.now()

        userData['phone'] = ''
        userData['mobile'] = ''
        userData['website'] = ''
        userData['fax'] = ''
        userData['date_of_birth'] = datetime.datetime(1, 1, 1)
        userData['street'] = ''
        userData['city'] = ''
        userData['state'] = ''
        userData['postal_code'] = ''
        userData['country'] = ''

        userData['status'] = 'Active'

        # logger.debug(userData)

        result = await collectionUser.insert_one(userData)

        return {'message': 'New user record created.', 'id': str(result.inserted_id)}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_user(recordId: str) -> dict:
    try:
        resultDoc = await collectionUser.find_one({'_id': ObjectId(recordId)}, projection={'password': 0})

        if resultDoc is None:
            return {'type': 'error', 'message': 'No user record found with the provided record id.'}

        resultDoc['_id'] = str(resultDoc['_id'])
        resultDoc['added_at'] = resultDoc['added_at'].isoformat()
        resultDoc['date_of_birth'] = resultDoc['date_of_birth'].isoformat()

        logger.debug(resultDoc)

        return {'record': resultDoc, 'message': 'A user record retrieved successfully.'}

    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def update_user(recordId: str, requestData: dict) -> dict:
    try:
        existingUserData = await collectionUser.find_one({'_id': ObjectId(recordId)})

        if existingUserData is None:
            return {'type': 'error', 'message': 'No matching user record found to be updated.'}

        requestData = requestData['updated_record']
        userData = {}
        userData['first_name'] = requestData['first_name']
        userData['last_name'] = requestData['last_name']
        userData['email'] = requestData['email']
        userData['role'] = requestData['role']
        userData['profile'] = requestData['profile']
        userData['territories'] = requestData['territories']

        userData['added_by'] = existingUserData['added_by']
        userData['added_at'] = existingUserData['added_at']

        userData['phone'] = requestData['phone']
        userData['mobile'] = requestData['mobile']
        userData['website'] = requestData['website']
        userData['fax'] = requestData['fax']
        userData['date_of_birth'] = datetime.datetime.fromisoformat(
            requestData['date_of_birth'])
        userData['street'] = requestData['street']
        userData['city'] = requestData['city']
        userData['state'] = requestData['state']
        userData['postal_code'] = requestData['postal_code']
        userData['country'] = requestData['country']

        userData['status'] = existingUserData['status']

        logger.debug(userData)

        result = await collectionUser.replace_one({'_id': ObjectId(recordId)}, userData)

        if result.matched_count == 0:
            return {'type': 'error', 'message': 'No matching user record found to be updated.'}

        return {'message': str(result.modified_count) + ' user record updated.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def change_user_status(recordId: str, status: str) -> dict:
    try:
        userFields = await collectionModuleFields.find_one(
            {'type': 'collection', 'collection_name': 'user'},
            projection={'collection_fields': 1, '_id': 0})

        availableStatus = []
        i = len(userFields['collection_fields']) - 1
        while i >= 0:
            if userFields['collection_fields'][i]['field_name'] != 'status':
                i -= 1
                continue
            availableStatus = userFields['collection_fields'][i]['field_value_dropdown']
            break

        # logger.debug(availableStatus)
        if len(availableStatus) == 0:
            return {'type': 'error', 'message': 'No user status available in the database for validation.'}

        if status not in availableStatus:
            return {'type': 'error', 'message': 'The provided status is not valid. It has to be one of ' + str(availableStatus) + '.'}

        updatedDoc = await collectionUser.update_one(
            {'_id': ObjectId(recordId)},
            {'$set': {'status': status}})

        if updatedDoc and updatedDoc.modified_count == 1:
            return {'message': str(updatedDoc.modified_count) + ' user record has been updated.'}

        return {'type': 'error', 'message': 'No user record updated.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def delete_user(recordId: str) -> dict:
    try:
        result = await collectionUser.delete_one({'_id': ObjectId(recordId)})

        if result.deleted_count == 0:
            return {'type': 'error',
                    'message': 'No relevant user record found for the provided record Id to be deleted.'}

        return {'message': 'A user record deleted successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_users_with_options(start: int, length: int, sortBy: list, sortOrder: str, search: str, statusList: list) -> dict:
    try:
        query = {}
        results = {'records': []}
        sortFields = []

        if statusList and len(statusList) > 0:
            query['status'] = {'$in': statusList}

        if len(search) > 0:
            query['$or'] = []
            for stringField in collectionBsonTypeFields['user']['string']:
                query['$or'].append({
                    stringField: {
                        '$regex': '^' + search,
                        '$options': 'i'}})

        if sortBy and len(sortBy) > 0:
            i = 0
            while i < len(sortBy):
                if i == 0:
                    sortFields.append(
                        (sortBy[i], -1 if sortOrder == 'desc' else 1))
                else:
                    sortFields.append((sortBy[i], 1))
                i += 1

        logger.debug(query)
        recordCount = await collectionUser.count_documents(query)
        records = collectionUser.find(query, projection={'password': 0}).skip(
            start).limit(length).sort(sortFields)

        for record in await records.to_list(length):
            record['_id'] = str(record['_id'])
            record['added_at'] = record['added_at'].isoformat()
            record['date_of_birth'] = record['date_of_birth'].isoformat()
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
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_users_with_less_options(start: int, length: int, search: str) -> dict:
    try:
        return await get_users_with_options(start, length, ['first_name', 'last_name', 'email'], 'asc', search, ['Active'])
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def save_new_password(email: str, password: str) -> dict:
    userExists = await collectionUser.find_one(
        {'email': email})

    if userExists is None:
        return {'type': 'error', 'message': 'No such user exists in the system.'}

    if 'password' in userExists:
        return {'type': 'error', 'message': 'The password for the given user is already set.'}

    result = await collectionUser.update_one(
        {'email': email}, {'$set': {'password': security.get_password_hash(password)}})

    if result and result.modified_count == 1:
        return {'message': str(result.modified_count) + ' user record has been updated.'}

    return {'type': 'error', 'message': 'No user record updated.'}


async def get_user_by_email(email: str) -> any:
    userDoc = await collectionUser.find_one({'email': email})

    if userDoc:
        userDoc['_id'] = str(userDoc['_id'])
        userDoc['added_at'] = userDoc['added_at'].isoformat()
        userDoc['date_of_birth'] = userDoc['date_of_birth'].isoformat()

    return userDoc


async def authenticate_user(email: str, password: str) -> dict:
    userDoc = await get_user_by_email(email)

    if userDoc is None:
        return None

    if not security.verify_password(password, userDoc['password']):
        return None

    return userDoc
