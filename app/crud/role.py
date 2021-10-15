from bson.objectid import ObjectId

from ..db import setup_collection
from ..core.config import (logger, mongoClient,
                           collectionRole, collectionUser,
                           collectionGroup,
                           collectionSharingRule)


async def create_role_collection():
    await setup_collection.create_collection('role')
    await create_ceo_role()
    await create_manager_role()


async def create_ceo_role():
    recordCount = await collectionRole.count_documents({'role_name': 'CEO'})
    if recordCount:
        logger.debug('A CEO role exists.')
        return

    roleData = {}
    roleData['role_name'] = 'CEO'
    roleData['reports_to'] = ''
    roleData['share_data_with_peers'] = False
    roleData['description'] = 'CEO role'
    roleData['associated_users'] = []

    result = await collectionRole.insert_one(roleData)
    logger.debug('New role record created for CEO with id: ' +
                 str(result.inserted_id))


async def create_manager_role():
    recordCount = await collectionRole.count_documents({'role_name': 'Manager'})
    if recordCount:
        logger.debug('A Manager role exists.')
        return

    roleData = {}
    roleData['role_name'] = 'Manager'
    roleData['reports_to'] = 'CEO'
    roleData['share_data_with_peers'] = False
    roleData['description'] = 'Manager role'
    roleData['associated_users'] = []

    result = await collectionRole.insert_one(roleData)
    logger.debug('New role record created for Manager with id: ' +
                 str(result.inserted_id))


async def create_role(requestData: dict) -> dict:
    try:
        requestData = requestData['new_record']

        roleRecordCount = await collectionRole.count_documents({'role_name': requestData['reports_to']})
        if roleRecordCount == 0:
            return{'type': 'error', 'message': 'The role of report_to is not present in the system.'}

        roleData = {}
        roleData['role_name'] = requestData['role_name']
        roleData['reports_to'] = requestData['reports_to']
        roleData['share_data_with_peers'] = requestData['share_data_with_peers'] if 'share_data_with_peers' in requestData else False
        roleData['description'] = requestData['description']
        roleData['associated_users'] = []

        logger.debug(roleData)

        result = await collectionRole.insert_one(roleData)

        return {'message': 'New role record created', 'id': str(result.inserted_id)}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_role_subordinates(roleName: str) -> list:
    resultList = []
    recordCount = collectionRole.count_documents({'reports_to': roleName})
    if recordCount == 0:
        return resultList

    subRoles = collectionRole.find(
        {'reports_to': roleName}).sort([('role_name', 1)])

    async for subRole in subRoles:
        role = {}
        role['role_name'] = subRole['role_name']
        role['_id'] = str(subRole['_id'])
        role['subordinates'] = await get_role_subordinates(subRole['role_name'])
        resultList.append(role)

    return resultList


async def get_roles_in_tree_structure() -> dict:
    try:
        structuredRoles = {}
        structuredRoles['role_name'] = 'CEO'
        structuredRoles['_id'] = ''
        structuredRoles['subordinates'] = await get_role_subordinates('CEO')

        return {'structured_roles': structuredRoles, 'message': 'roles retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_roles_list_with_options(start: int, length: int, search: str) -> dict:
    try:
        query = {}
        sortFields = [('role_name', 1)]
        roleNames = []

        if len(search) > 0:
            query['role_name'] = {'$regex': '^' + search, '$options': 'i'}

        recordCount = await collectionRole.count_documents(query)
        roles = collectionRole.find(query).skip(
            start).limit(length).sort(sortFields)

        async for role in roles:
            roleNames.append(role['role_name'])

        return {'role_name': roleNames, 'recordsTotal': recordCount, 'message': 'Roles retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_parent_roles_list_with_options(start: int, length: int, search: str) -> dict:
    try:
        pipeline = []
        roleNames = []
        recordCount = 0

        pipeline.append({'$sort': {'reports_to': 1}})
        pipeline.append({'$group': {'_id': '$reports_to'}})
        pipeline.append({'$match': {'_id': {'$ne': ''}}})

        if len(search) > 0:
            pipeline.append(
                {'$match': {'_id': {'$regex': '^' + search, '$options': 'i'}}})

        countPipeline = pipeline.copy()
        countPipeline.append({'$count': 'role_count'})
        pipeline.append({'$skip': start})
        pipeline.append({'$limit': length})

        countRoles = collectionRole.aggregate(countPipeline)
        roles = collectionRole.aggregate(pipeline)

        async for role in roles:
            roleNames.append(role['_id'])

        async for countRole in countRoles:
            recordCount = countRole['role_count']

        return {'role_name': roleNames, 'recordsTotal': recordCount, 'message': 'Roles retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_role(recordId: str) -> dict:
    try:
        resultDoc = await collectionRole.find_one({'_id': ObjectId(recordId), 'role_name': {'$ne': 'CEO'}})

        if resultDoc is None:
            return {'type': 'error', 'message': 'No role found for the provided Id.'}

        resultDoc['_id'] = str(resultDoc['_id'])

        logger.debug(resultDoc)

        return {'record': resultDoc, 'message': 'A role record retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def update_role(recordId: str, requestData: dict) -> dict:
    try:
        nameChanged = False
        sharingRuleUpdateCount = 0
        requestData = requestData['updated_record']

        existingData = await collectionRole.find_one({'_id': ObjectId(recordId)})

        if existingData is None:
            return {'type': 'error', 'message': 'No matching role record found to be updated.'}

        roleRecordCount = await collectionRole.count_documents({'role_name': requestData['reports_to']})
        if roleRecordCount == 0:
            return{'type': 'error', 'message': 'The role of report_to is not present in the system.'}

        async with await mongoClient.start_session() as transactionSession:
            async with transactionSession.start_transaction():
                resultDoc = await collectionRole.update_one(
                    {'_id': ObjectId(recordId), 'role_name': {'$ne': 'CEO'}},
                    {'$set': {
                        'role_name': requestData['role_name'],
                        'reports_to': requestData['reports_to'],
                        'share_data_with_peers': requestData['share_data_with_peers'] if 'share_data_with_peers' in requestData else False,
                        'description': requestData['description']
                    }},
                    session=transactionSession)

                if existingData['role_name'] != requestData['role_name']:
                    nameChanged = True
                    resultUserUpdate = await collectionUser.update_many(
                        {'role': existingData['role_name']},
                        {'$set': {
                            'role': requestData['role_name']}},
                        session=transactionSession)
                    resultReportToUpdate = await collectionRole.update_many(
                        {'reports_to': existingData['role_name']},
                        {'$set': {
                            'reports_to': requestData['role_name']}},
                        session=transactionSession)
                    resultGroupUpdate = await collectionGroup.update_many(
                        {'selected.role_name': existingData['role_name']},
                        {'$set': {
                            'selected.$.role_name': requestData['role_name']
                        }},
                        session=transactionSession)

                    resultSharingRuleUpdate = await collectionSharingRule.update_many(
                        {'records_shared_from_selected.role_name':
                            existingData['role_name']},
                        {'$set': {
                            'records_shared_from_selected.$.role_name': requestData['role_name']
                        }},
                        session=transactionSession
                    )
                    sharingRuleUpdateCount += resultSharingRuleUpdate.modified_count

                    resultSharingRuleUpdate = await collectionSharingRule.update_many(
                        {'records_shared_to_selected.role_name':
                            existingData['role_name']},
                        {'$set': {
                            'records_shared_to_selected.$.role_name': requestData['role_name']
                        }},
                        session=transactionSession
                    )
                    sharingRuleUpdateCount += resultSharingRuleUpdate.modified_count

        extraUpdateClause = ''
        if nameChanged:
            extraUpdateClause = ' ' + \
                str(resultReportToUpdate.modified_count) + ' roles with report_to updated. ' + \
                str(resultUserUpdate.modified_count) + ' users updated. ' + \
                str(resultGroupUpdate.modified_count) + ' groups updated. ' + \
                str(sharingRuleUpdateCount) + ' sharing rules updated.'

        return {'message': str(resultDoc.modified_count) + ' role record updated successfully.' + extraUpdateClause}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def delete_role(roleToDelete: str, roleToTransfer) -> dict:
    try:
        if roleToDelete == 'CEO':
            return{'type': 'error', 'message': 'The CEO role cannot be deleted.'}

        if roleToDelete == roleToTransfer:
            return{'type': 'error', 'message': 'Both roles cannot be the same.'}

        roleRecordCount = await collectionRole.count_documents({'role_name': roleToDelete})
        if roleRecordCount == 0:
            return{'type': 'error', 'message': 'The role to delete is not present in the system.'}

        roleRecordCount = await collectionRole.count_documents({'role_name': roleToTransfer})
        if roleRecordCount == 0:
            return{'type': 'error', 'message': 'The role to transfer is not present in the system.'}

        sharingRuleUpdateCount = 0

        async with await mongoClient.start_session() as transactionSession:
            async with transactionSession.start_transaction():
                userResultDoc = await collectionUser.update_many(
                    {'role': roleToDelete},
                    {'$set': {
                        'role': roleToTransfer
                    }},
                    session=transactionSession
                )
                resultReportTo = await collectionRole.update_many(
                    {'reports_to': roleToDelete},
                    {'$set': {
                        'reports_to': roleToTransfer
                    }},
                    session=transactionSession
                )
                resultDoc = await collectionRole.delete_one(
                    {'role_name': roleToDelete},
                    session=transactionSession
                )
                resultGroupUpdate = await collectionGroup.update_many(
                    {'selected.role_name': roleToDelete},
                    {'$set': {
                        'selected.$.role_name': roleToTransfer
                    }},
                    session=transactionSession
                )

                resultSharingRuleUpdate = await collectionSharingRule.update_many(
                    {'records_shared_from_selected.role_name': roleToDelete},
                    {'$set': {
                        'records_shared_from_selected.$.role_name': roleToTransfer
                    }},
                    session=transactionSession
                )
                sharingRuleUpdateCount += resultSharingRuleUpdate.modified_count

                resultSharingRuleUpdate = await collectionSharingRule.update_many(
                    {'records_shared_to_selected.role_name': roleToDelete},
                    {'$set': {
                        'records_shared_to_selected.$.role_name': roleToTransfer
                    }},
                    session=transactionSession
                )
                sharingRuleUpdateCount += resultSharingRuleUpdate.modified_count

        if resultDoc.deleted_count == 0:
            return{'type': 'error', 'message': 'No relevant role record found to be deleted.'}

        return {'message': str(userResultDoc.modified_count) + ' users updated. ' +
                str(resultReportTo.modified_count) + ' roles with reports_to updated. ' +
                str(resultGroupUpdate.modified_count) + ' groups updated. ' +
                str(sharingRuleUpdateCount) + ' sharing rules updated. ' +
                'The role record deleted successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def add_users_to_set_with_subordinate_roles(roleName: str, userIdSet: set):
    subordinateRoles = await collectionRole.find({'reports_to': roleName})

    async for role in subordinateRoles:
        roleUsers = await collectionUser.find({'role': role['role_name']},
                                              projection={'_id': 1})

        async for user in roleUsers:
            userIdSet.add(str(user['_id']))

        await add_users_to_set_with_subordinate_roles(role['role_name'], userIdSet)


async def get_users_with_subordinate_roles(roleName: str, userId: str) -> list:
    if roleName == 'CEO':
        return []

    userIdSet = set()
    userIdSet.add(userId)
    currentRole = await collectionRole.find_one({'role_name': roleName})

    if currentRole is None:
        return list(userIdSet)

    if currentRole['share_data_with_peers']:
        peerUsers = await collectionUser.find({'role': roleName},
                                              projection={'_id': 1})

        async for peer in peerUsers:
            userIdSet.add(str(peer['_id']))

    await add_users_to_set_with_subordinate_roles(roleName, userIdSet)

    return list(userIdSet)
