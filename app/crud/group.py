from bson.objectid import ObjectId

from ..db import setup_collection
from ..core.config import (collectionGroup,
                           logger, mongoClient,
                           collectionSharingRule,
                           collectionBsonTypeFields)

groupSources = {'Users',
                'Roles',
                'Roles & Subordinates',
                'Groups',
                'Territories',
                'Territories & Sub-Territories'}


async def create_group_collection():
    await setup_collection.create_collection('group')


async def prepare_selected_list(requestData: dict) -> list:
    resultList = []

    if requestData['group_source'] == 'Users':
        for userSelected in requestData['selected']:
            user = {}
            user['_id'] = userSelected['_id']
            user['first_name'] = userSelected['first_name']
            user['last_name'] = userSelected['last_name']
            user['email'] = userSelected['email']
            user['role'] = userSelected['role']

            resultList.append(user)

    elif requestData['group_source'] in {'Roles', 'Roles & Subordinates'}:
        for roleSelected in requestData['selected']:
            role = {}
            role['role_name'] = roleSelected['role_name']

            resultList.append(role)

    elif requestData['group_source'] == 'Groups':
        for groupSelected in requestData['selected']:
            group = {}
            group['group_name'] = groupSelected['group_name']

            resultList.append(group)

    elif requestData['group_source'] in {'Territories', 'Territories & Sub-Territories'}:
        for territorySelected in requestData['selected']:
            territory = {}
            territory['territory_name'] = territorySelected['territory_name']

            resultList.append(territory)

    return resultList


async def prepare_group_dict(requestData: dict) -> dict:
    groupData = {}
    groupData['group_name'] = requestData['group_name']
    groupData['group_description'] = requestData['group_description']
    groupData['group_source'] = requestData['group_source']

    groupData['selected'] = await prepare_selected_list(requestData)

    return groupData


async def create_group(requestData: dict) -> dict:
    try:
        requestData = requestData['new_record']

        if requestData['group_source'] not in groupSources:
            return {'type': 'error', 'message': 'Group Source is not valid.'}

        groupData = await prepare_group_dict(requestData)

        logger.debug(groupData)

        result = await collectionGroup.insert_one(groupData)

        return {'message': 'New group record created.', 'id': str(result.inserted_id)}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_groups_list_with_options(start: int, length: int, search: str) -> dict:
    try:
        query = {}
        sortFields = [('group_name', 1)]
        groupNames = []

        if len(search) > 0:
            query['group_name'] = {'$regex': '^' + search, '$options': 'i'}

        recordCount = await collectionGroup.count_documents(query)
        groups = collectionGroup.find(query, projection={'group_name': 1, '_id': 0}).skip(
            start).limit(length).sort(sortFields)

        async for group in groups:
            groupNames.append(group['group_name'])

        return {'group_name': groupNames, 'recordsTotal': recordCount, 'message': 'Group names retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_groups_list_for_datatable(start: int, length: int, sortBy: list, sortOrder: str, search: str) -> dict:
    try:
        query = {}
        results = {'records': []}
        sortFields = []

        if len(search) > 0:
            query['$or'] = [
                {'group_name': {
                    '$regex': '^' + search,
                    '$options': 'i'}}]
            query['$or'].append(
                {'group_description': {
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

        # logger.debug(query)
        recordCount = await collectionGroup.count_documents(query)
        records = collectionGroup.find(
            query,
            projection={
                'group_name': 1,
                'group_description': 1}
        ).skip(
            start
        ).limit(
            length
        ).sort(
            sortFields
        )

        for record in await records.to_list(length):
            record['_id'] = str(record['_id'])

            results['records'].append(record)

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


async def get_group(recordId: str) -> dict:
    try:
        resultDoc = await collectionGroup.find_one({'_id': ObjectId(recordId)})

        if resultDoc is None:
            return {'type': 'error', 'message': 'No group found for the provided Id.'}

        resultDoc['_id'] = str(resultDoc['_id'])

        return {'record': resultDoc, 'message': 'A group record retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def update_group(recordId: str, requestData: dict) -> dict:
    try:
        resultSelectedUpdate = None
        resultSharingRuleUpdate = None
        sharingRuleUpdateCount = 0

        requestData = requestData['updated_record']

        existingData = await collectionGroup.find_one({'_id': ObjectId(recordId)})

        if existingData is None:
            return {'type': 'error', 'message': 'No matching group record found to be updated.'}

        if requestData['group_source'] not in groupSources:
            return {'type': 'error', 'message': 'Group Source is not valid.'}

        logger.debug(requestData)

        async with await mongoClient.start_session() as transactionSession:
            async with transactionSession.start_transaction():
                result = await collectionGroup.update_one(
                    {'_id': ObjectId(recordId)},
                    {'$set': {
                        'group_name': requestData['group_name'],
                        'group_description': requestData['group_description'],
                        'group_source': requestData['group_source'],
                        'selected': await prepare_selected_list(requestData)
                    }},
                    session=transactionSession)

                if existingData['group_name'] != requestData['group_name']:
                    resultSelectedUpdate = await collectionGroup.update_many(
                        {'selected.group_name': existingData['group_name']},
                        {'$set': {
                            'selected.$.group_name': requestData['group_name']}},
                        session=transactionSession)

                    resultSharingRuleUpdate = await collectionSharingRule.update_many(
                        {'records_shared_from_selected.group_name':
                            existingData['group_name']},
                        {'$set': {
                            'records_shared_from_selected.$.group_name': requestData['group_name']
                        }},
                        session=transactionSession
                    )

                    sharingRuleUpdateCount += resultSharingRuleUpdate.modified_count

                    resultSharingRuleUpdate = await collectionSharingRule.update_many(
                        {'records_shared_to_selected.group_name':
                            existingData['group_name']},
                        {'$set': {
                            'records_shared_to_selected.$.group_name': requestData['group_name']
                        }},
                        session=transactionSession
                    )

                    sharingRuleUpdateCount += resultSharingRuleUpdate.modified_count

        selectedUpdateClause = ''
        sharingRuleUpdateClause = ''
        if resultSelectedUpdate is not None:
            selectedUpdateClause = ' ' + \
                str(resultSelectedUpdate.modified_count) + \
                ' group selections updated.'

            sharingRuleUpdateClause = ' ' + \
                str(sharingRuleUpdateCount) + \
                ' sharing rules updated.'

        return {'message': str(result.modified_count) +
                ' group record updated.' +
                selectedUpdateClause +
                sharingRuleUpdateClause}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def delete_group(recordId: str) -> dict:
    try:
        sharingRuleUpdateCount = 0
        async with await mongoClient.start_session() as transactionSession:
            async with transactionSession.start_transaction():
                resultDoc = await collectionGroup.find_one_and_delete(
                    {'_id': ObjectId(recordId)},
                    session=transactionSession)

                if resultDoc is None:
                    return {'type': 'error', 'message': 'No matching group record found to be deleted.'}

                resultSelectedDelete = await collectionGroup.update_many(
                    {'selected.group_name': resultDoc['group_name']},
                    {'$pull': {
                        'selected': {
                            'group_name': resultDoc['group_name']
                        }
                    }},
                    session=transactionSession
                )

                resultSharingRuleUpdate = await collectionSharingRule.update_many(
                    {'records_shared_from_selected.group_name':
                        resultDoc['group_name']},
                    {'$pull': {
                        'records_shared_from_selected': {
                            'group_name': resultDoc['group_name']
                        }
                    }},
                    session=transactionSession
                )

                sharingRuleUpdateCount += resultSharingRuleUpdate.modified_count

                resultSharingRuleUpdate = await collectionSharingRule.update_many(
                    {'records_shared_to_selected.group_name':
                        resultDoc['group_name']},
                    {'$pull': {
                        'records_shared_to_selected': {
                            'group_name': resultDoc['group_name']
                        }
                    }},
                    session=transactionSession
                )

                sharingRuleUpdateCount += resultSharingRuleUpdate.modified_count

        return {'message': '1 group deleted. This group deleted from ' +
                str(resultSelectedDelete.modified_count) +
                ' group records. This group is also deleted from ' +
                str(sharingRuleUpdateCount) + ' sharing rule records.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}
