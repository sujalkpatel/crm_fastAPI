from bson.objectid import ObjectId

from ..db import setup_collection
from ..core.config import logger, collectionSharingRule

sharingToFrom = {"Groups",
                 "Roles",
                 "Roles & Subordinates"}

accessTypes = {"Read/Write/Delete",
               "Read Only"}


async def create_sharing_rule_collection():
    await setup_collection.create_collection('sharing_rule')


async def prepare_sharing_rule_dict(requestData: dict) -> dict:
    sharingRuleDict = {}

    sharingRuleDict['module'] = []
    for module in requestData['module']:
        moduleToAdd = {}
        moduleToAdd['module_name'] = module['module_name']
        moduleToAdd['module_label'] = module['module_label']
        sharingRuleDict['module'].append(moduleToAdd)

    sharingRuleDict['records_shared_from'] = requestData['records_shared_from']

    sharingRuleDict['records_shared_from_selected'] = []
    if sharingRuleDict['records_shared_from'] == 'Groups':
        for recordFrom in requestData['records_shared_from_selected']:
            sharingRuleDict['records_shared_from_selected'].append({
                'group_name': recordFrom['group_name']})

    elif sharingRuleDict['records_shared_from'] in {'Roles', 'Roles & Subordinates'}:
        for recordFrom in requestData['records_shared_from_selected']:
            sharingRuleDict['records_shared_from_selected'].append({
                'role_name': recordFrom['role_name']})

    sharingRuleDict['records_shared_to'] = requestData['records_shared_to']

    sharingRuleDict['records_shared_to_selected'] = []
    if sharingRuleDict['records_shared_to'] == 'Groups':
        for recordFrom in requestData['records_shared_to_selected']:
            sharingRuleDict['records_shared_to_selected'].append({
                'group_name': recordFrom['group_name']})

    elif sharingRuleDict['records_shared_to'] in {'Roles', 'Roles & Subordinates'}:
        for recordFrom in requestData['records_shared_to_selected']:
            sharingRuleDict['records_shared_to_selected'].append({
                'role_name': recordFrom['role_name']})

    sharingRuleDict['access_type'] = requestData['access_type']
    sharingRuleDict['superiors_allowed'] = requestData['superiors_allowed']

    return sharingRuleDict


async def create_sharing_rule(requestData: dict) -> dict:
    try:
        requestData = requestData['new_record']

        if requestData['records_shared_from'] not in sharingToFrom or \
                requestData['records_shared_to'] not in sharingToFrom:
            return {'type': 'error', 'message': 'Records Shared To/From is not valid.'}

        if requestData['access_type'] not in accessTypes:
            return {'type': 'error', 'message': 'Access Type is not valid.'}

        sharingRuleData = await prepare_sharing_rule_dict(requestData)

        logger.debug(sharingRuleData)

        result = await collectionSharingRule.insert_one(sharingRuleData)

        return {'message': 'New sharing rule record created.', 'id': str(result.inserted_id)}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_sharing_rules_list() -> dict:
    try:
        query = {}
        sortFields = [('_id', -1)]
        sharingRules = []

        resultDocs = collectionSharingRule.find(query).sort(sortFields)

        async for result in resultDocs:
            result['_id'] = str(result['_id'])
            sharingRules.append(result)

        return {'sharing_rules': sharingRules, 'message': 'Sharing rules retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_sharing_rule(recordId: str) -> dict:
    try:
        resultDoc = await collectionSharingRule.find_one({'_id': ObjectId(recordId)})

        if resultDoc is None:
            return {'type': 'error', 'message': 'No sharing rule record found for the provided Id.'}

        resultDoc['_id'] = str(resultDoc['_id'])

        return {'record': resultDoc, 'message': 'A sharing rule record retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def update_sharing_rule(recordId: str, requestData: dict) -> dict:
    try:
        requestData = requestData['updated_record']

        if requestData['records_shared_from'] not in sharingToFrom or \
                requestData['records_shared_to'] not in sharingToFrom:
            return {'type': 'error', 'message': 'Records Shared To/From is not valid.'}

        if requestData['access_type'] not in accessTypes:
            return {'type': 'error', 'message': 'Access Type is not valid.'}

        sharingRuleData = await prepare_sharing_rule_dict(requestData)

        logger.debug(sharingRuleData)

        result = await collectionSharingRule.update_one({'_id': ObjectId(recordId)},
                                                        {'$set': sharingRuleData})

        if result.matched_count == 0:
            return {'type': 'error', 'message': 'No matching sharing rule record found to be updated.'}

        return {'message': str(result.modified_count) + ' sharing rule record updated.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def delete_sharing_rule(recordId: str) -> dict:
    try:
        resultDoc = await collectionSharingRule.find_one_and_delete({'_id': ObjectId(recordId)})

        if resultDoc is None:
            return {'type': 'error', 'message': 'No matching sharing rule found to be deleted.'}

        return {'message': '1 sharing rule deleted.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}
