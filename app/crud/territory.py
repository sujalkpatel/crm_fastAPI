from bson.objectid import ObjectId
from bson.decimal128 import Decimal128

from ..db import setup_collection
from ..core.config import logger, collectionTerritory, collectionModuleAccount, moduleBsonFieldTypes, mongoClient, collectionUser
from .local_data import load_module_bson_field_types


async def create_territory_collection():
    await setup_collection.create_collection('territory')


async def prepare_territory_dict(requestData: dict) -> dict:
    territoryData = {}
    territoryData['territory_name'] = requestData['territory_name']

    territoryData['territory_manager'] = {}
    territoryData['territory_manager']['_id'] = requestData['territory_manager']['_id']
    territoryData['territory_manager']['first_name'] = requestData['territory_manager']['first_name']
    territoryData['territory_manager']['last_name'] = requestData['territory_manager']['last_name']
    territoryData['territory_manager']['email'] = requestData['territory_manager']['email']
    territoryData['territory_manager']['role'] = requestData['territory_manager']['role']

    territoryData['parent_territory'] = requestData['parent_territory']

    territoryData['users'] = []
    for userInRequest in requestData['users']:
        user = {}
        user['_id'] = userInRequest['_id']
        user['first_name'] = userInRequest['first_name']
        user['last_name'] = userInRequest['last_name']
        user['email'] = userInRequest['email']
        user['role'] = userInRequest['role']

        territoryData['users'].append(user)

    territoryData['permissions'] = requestData['permissions']
    territoryData['description'] = requestData['description']

    territoryData['account_rules'] = []
    for accoutRuleInRequest in requestData['account_rules']:
        accountRule = {}
        accountRule['rule_number'] = accoutRuleInRequest['rule_number']
        accountRule['field'] = accoutRuleInRequest['field']
        accountRule['operator'] = accoutRuleInRequest['operator']
        accountRule['textValue'] = accoutRuleInRequest['textValue']
        accountRule['from'] = accoutRuleInRequest['from']
        accountRule['to'] = accoutRuleInRequest['to']

        territoryData['account_rules'].append(accountRule)

    territoryData['criteria_order'] = requestData['criteria_order']
    territoryData['accounts'] = []

    return territoryData


async def create_territory(requestData: dict) -> dict:
    try:
        requestData = requestData['new_record']

        territoryCount = await collectionTerritory.count_documents({'territory_name': requestData['parent_territory']})
        if territoryCount == 0:
            return {'type': 'error', 'message': 'The parent territory provided is not present in the system.'}

        territoryData = await prepare_territory_dict(requestData)

        logger.debug(territoryData)

        validCriteria = await verify_criteria(territoryData['criteria_order'])
        if not validCriteria:
            return {'type': 'error', 'message': 'Criteria order not in valid format for the territory.'}

        result = await collectionTerritory.insert_one(territoryData)

        return {'message': 'New territory record created.', 'id': str(result.inserted_id)}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def create_root_territory(territoryName: str):
    recordCount = await collectionTerritory.count_documents({'root_territory': True})
    if recordCount:
        logger.debug('A root territory record exists.')
        return

    await collectionTerritory.create_index([('root_territory', 1)], background=True)

    territoryData = {}

    territoryData['territory_name'] = territoryName
    territoryData['root_territory'] = True

    territoryData['territory_manager'] = {}
    territoryData['permissions'] = ''
    territoryData['account_rules'] = []

    result = await collectionTerritory.insert_one(territoryData)
    logger.debug('New root territory record created with id: ' +
                 str(result.inserted_id))


async def get_territory_children(parentTerritory: str) -> list:
    resultList = []
    recordCount = await collectionTerritory.count_documents(
        {'parent_territory': parentTerritory})
    if recordCount == 0:
        return resultList

    children = collectionTerritory.find(
        {'parent_territory': parentTerritory}).sort([('territory_name', 1)])

    async for child in children:
        territory = {}
        territory['territory_name'] = child['territory_name']
        territory['_id'] = str(child['_id'])
        territory['children'] = await get_territory_children(child['territory_name'])
        resultList.append(territory)

    return resultList


async def get_territories_in_tree_structure() -> dict:
    try:
        rootTerritory = await collectionTerritory.find_one({'root_territory': True})

        structuredTerritories = {}
        structuredTerritories['territory_name'] = rootTerritory['territory_name']
        structuredTerritories['_id'] = ''
        structuredTerritories['children'] = await get_territory_children(rootTerritory['territory_name'])

        return {'structured_territories': structuredTerritories, 'message': 'territories retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_territories_list_with_options(start: int, length: int, search: str) -> dict:
    try:
        query = {}
        sortFields = [('territory_name', 1)]
        territoryNames = []

        if len(search) > 0:
            query['territory_name'] = {'$regex': '^' + search, '$options': 'i'}

        recordCount = await collectionTerritory.count_documents(query)
        territories = collectionTerritory.find(query, projection={'territory_name': 1, '_id': 0}).skip(
            start).limit(length).sort(sortFields)

        async for territory in territories:
            territoryNames.append(territory['territory_name'])

        return {'territory_name': territoryNames, 'recordsTotal': recordCount, 'message': 'Territories retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_parent_territories_list_with_options(start: int, length: int, search: str) -> dict:
    try:
        pipeline = []
        territoryNames = []
        recordCount = 0

        pipeline.append({'$sort': {'parent_territory': 1}})
        pipeline.append({'$group': {'_id': '$parent_territory'}})
        pipeline.append({'$match': {'_id': {'$ne': None}}})

        if len(search) > 0:
            pipeline.append(
                {'$match': {'_id': {'$regex': '^' + search, '$options': 'i'}}})

        countPipeline = pipeline.copy()
        countPipeline.append({'$count': 'territory_count'})
        pipeline.append({'$skip': start})
        pipeline.append({'$limit': length})

        countTerritories = collectionTerritory.aggregate(countPipeline)
        territories = collectionTerritory.aggregate(pipeline)

        async for territory in territories:
            territoryNames.append(territory['_id'])

        async for countTerritory in countTerritories:
            recordCount = countTerritory['territory_count']

        return {'territory_name': territoryNames, 'recordsTotal': recordCount, 'message': 'Territories retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_territory(recordId: str) -> dict:
    try:
        resultDoc = await collectionTerritory.find_one({'_id': ObjectId(recordId),
                                                        'root_territory': {'$ne': True}})

        if resultDoc is None:
            return {'type': 'error', 'message': 'No territory found for the provided Id.'}

        resultDoc['_id'] = str(resultDoc['_id'])

        return {'record': resultDoc, 'message': 'A territory record retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def update_territory(recordId: str, requestData: dict) -> dict:
    try:
        resultUserDoc = None
        requestData = requestData['updated_record']

        territoryData = await prepare_territory_dict(requestData)

        existingData = await collectionTerritory.find_one({'_id': ObjectId(recordId),
                                                           'root_territory': {'$ne': True}})

        if existingData is None:
            return {'type': 'error', 'message': 'No matching territory record found to be updated.'}

        territoryCount = await collectionTerritory.count_documents({'territory_name': requestData['parent_territory']})
        if territoryCount == 0:
            return {'type': 'error', 'message': 'The parent territory provided is not present in the system.'}

        if 'accounts' in existingData:
            territoryData['accounts'] = existingData['accounts']

        validCriteria = await verify_criteria(territoryData['criteria_order'])
        if not validCriteria:
            return {'type': 'error', 'message': 'Criteria order not in valid format for the territory.'}

        async with await mongoClient.start_session() as transactionSession:
            async with transactionSession.start_transaction():
                resultDoc = await collectionTerritory.replace_one(
                    {'_id': ObjectId(recordId)}, territoryData,
                    session=transactionSession)

                if resultDoc.matched_count == 0:
                    return {'type': 'error', 'message': 'No matching territory record found to be updated.'}

                if existingData['territory_name'] != requestData['territory_name']:
                    resultUserDoc = await collectionUser.update_many(
                        {'territories': existingData['territory_name']},
                        {'$set': {
                            'territories.$': requestData['territory_name']}},
                        session=transactionSession)

        userUpdateClause = ''
        if resultUserDoc is not None:
            userUpdateClause = ' ' + \
                str(resultUserDoc.modified_count) + ' users updated.'
        return {'message': str(resultDoc.modified_count) + ' territory record updated.' + userUpdateClause}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def verify_criteria(criteriaOrder: str) -> bool:
    if len(criteriaOrder) == 0:
        return True

    operations = {'AND', 'OR'}
    ruleList = []
    bracketOperationList = []
    i = 0

    while i < len(criteriaOrder):
        if criteriaOrder[i] == ' ':
            i += 1
            continue

        elif criteriaOrder[i] == '(':
            bracketOperationList.append(criteriaOrder[i])
            i += 1

        elif criteriaOrder[i] == ')':
            if len(ruleList) > 0 and len(bracketOperationList) > 0 and \
                    bracketOperationList[len(bracketOperationList) - 1] in operations:
                ruleList.pop()
                bracketOperationList.pop()
            if len(bracketOperationList) == 0 or \
                    bracketOperationList[len(bracketOperationList) - 1] != '(':
                return False
            bracketOperationList.pop()
            i += 1

        elif criteriaOrder[i].isdigit():
            ruleNumber = int(criteriaOrder[i])
            while i < len(criteriaOrder) - 1 and criteriaOrder[i + 1].isdigit():
                i += 1
                ruleNumber = (ruleNumber * 10) + int(criteriaOrder[i])
            i += 1

            if len(ruleList) > 0 and len(bracketOperationList) > 0 and \
                    bracketOperationList[len(bracketOperationList) - 1] in operations:
                ruleList.pop()
                bracketOperationList.pop()
            ruleList.append(ruleNumber)

        else:
            operation = criteriaOrder[i]
            while i < len(criteriaOrder) - 1 and criteriaOrder[i + 1] != ' ':
                i += 1
                operation += criteriaOrder[i]
            i += 1

            if operation.upper() in operations:
                bracketOperationList.append(operation.upper())
            else:
                return False

    while len(ruleList) > 1 and len(bracketOperationList) > 0 and bracketOperationList[0] in operations:
        ruleList.pop()
        bracketOperationList.pop()

    if len(ruleList) == 1 and len(bracketOperationList) == 0:
        return True
    return False


async def evaluate_rule(fieldName: str, operator: str, textValue: str, fromText: str, toText: str) -> set:
    query = {}
    newTextValue = None
    newFromText = None
    newToText = None

    if fieldName not in moduleBsonFieldTypes['Account']:
        await load_module_bson_field_types()
        if fieldName not in moduleBsonFieldTypes['Account']:
            return set()

    if moduleBsonFieldTypes['Account'][fieldName] == 'int' or \
            moduleBsonFieldTypes['Account'][fieldName] == 'decimal':
        # int or decimal cannot work with string conditions
        if operator == 'is empty' or \
                operator == 'is not empty' or \
                operator == 'contains' or \
                operator == 'does not contain' or \
                operator == 'starts with' or \
                operator == 'ends with':
            return set()

        # int or decimal cannot work with empty input
        if operator == 'between' or operator == 'not between':
            if len(fromText) == 0 or len(toText) == 0:
                return set()

        else:
            if len(textValue) == 0:
                return set()

    # convert input to int if field type is int
    if moduleBsonFieldTypes['Account'][fieldName] == 'int':
        if operator == 'between' or operator == 'not between':
            newFromText = int(fromText)
            newToText = int(toText)
        else:
            newTextValue = int(textValue)

    # convert input to decimal if field type is decimal
    elif moduleBsonFieldTypes['Account'][fieldName] == 'decimal':
        if operator == 'between' or operator == 'not between':
            newFromText = Decimal128(fromText)
            newToText = Decimal128(toText)
        else:
            newTextValue = Decimal128(textValue)

    # else leave them as string
    else:
        newFromText = fromText
        newToText = toText
        newTextValue = textValue

    # setup query based on the operator
    if operator == '= (equals)':
        query[fieldName] = newTextValue

    elif operator == '! = (does not equal)':
        query[fieldName] = {'$ne': newTextValue}

    elif operator == '< (less than)':
        query[fieldName] = {'$lt': newTextValue}

    elif operator == '<= (less than equal to)':
        query[fieldName] = {'$lte': newTextValue}

    elif operator == '> (greater than)':
        query[fieldName] = {'$gt': newTextValue}

    elif operator == '>= (greater than equal to)':
        query[fieldName] = {'$gte': newTextValue}

    elif operator == 'between':
        query[fieldName] = {'$gte': newFromText, '$lte': newToText}

    elif operator == 'not between':
        query[fieldName] = {'$lte': newFromText, '$gte': newToText}

    elif operator == 'is empty':
        query[fieldName] = ''

    elif operator == 'is not empty':
        query[fieldName] = {'$ne': ''}

    elif operator == 'contains':
        query[fieldName] = {'$regex': newTextValue, '$options': 'i'}

    elif operator == 'does not contain':
        query[fieldName] = {
            '$regex': '^((?!' + newTextValue + ').)*$', '$options': 'i'}

    elif operator == 'starts with':
        query[fieldName] = {'$regex': '^' + newTextValue, '$options': 'i'}

    elif operator == 'ends with':
        query[fieldName] = {'$regex': newTextValue + '$', '$options': 'i'}

    else:
        return set()

    territories = collectionModuleAccount.find(query, projection={'_id': 1})
    resultSet = set()

    # put id's into set as string
    async for territory in territories:
        resultSet.add(str(territory['_id']))

    return resultSet


async def perform_operation(operation: str, set1: set, set2: set) -> set:
    if operation == 'AND':
        return set1 & set2

    elif operation == 'OR':
        return set1 | set2

    else:
        return set()


async def find_rule(ruleNumber: int, accountRules: list) -> dict:
    for accountRule in accountRules:
        if accountRule['rule_number'] == ruleNumber:
            return accountRule

    return {}


async def verify_and_perform_operation(resultList: list, bracketOperationList: list, operations: set):
    # Perform and/or operation if resultlist has atleast 2 sets and operationlist has atleast 1 operation
    if len(resultList) > 1 and len(bracketOperationList) > 0 and \
            bracketOperationList[len(bracketOperationList) - 1] in operations:
        set1 = resultList.pop()
        set2 = resultList.pop()
        operation = bracketOperationList.pop()
        resultSet = await perform_operation(operation, set1, set2)
        resultList.append(resultSet)


async def evaluate_criteria_order(criteriaOrder: str, accountRules: list) -> list:
    if len(criteriaOrder) == 0:
        return []

    operations = {'AND', 'OR'}
    resultList = []
    bracketOperationList = []
    i = 0

    while i < len(criteriaOrder):
        if criteriaOrder[i] == ' ':
            i += 1
            continue

        elif criteriaOrder[i] == '(':
            bracketOperationList.append(criteriaOrder[i])
            i += 1

        elif criteriaOrder[i] == ')':
            await verify_and_perform_operation(
                resultList, bracketOperationList, operations)
            bracketOperationList.pop()
            i += 1

        elif criteriaOrder[i].isdigit():
            ruleNumber = int(criteriaOrder[i])
            while i < len(criteriaOrder) - 1 and criteriaOrder[i + 1].isdigit():
                i += 1
                ruleNumber = (ruleNumber * 10) + int(criteriaOrder[i])
            i += 1

            ruleDict = await find_rule(ruleNumber, accountRules)
            if 'rule_number' not in ruleDict:
                return []

            ruleAccountsSet = await evaluate_rule(ruleDict['field']['field_name'], ruleDict['operator'], ruleDict['textValue'], ruleDict['from'], ruleDict['to'])
            resultList.append(ruleAccountsSet)

            await verify_and_perform_operation(
                resultList, bracketOperationList, operations)

        else:
            operation = criteriaOrder[i]
            while i < len(criteriaOrder) - 1 and criteriaOrder[i + 1] != ' ':
                i += 1
                operation += criteriaOrder[i]
            i += 1

            bracketOperationList.append(operation.upper())

    while len(resultList) > 1 and len(bracketOperationList) > 0:
        await verify_and_perform_operation(resultList, bracketOperationList, operations)

    return list(resultList[0])


async def run_rules() -> dict:
    try:
        territories = collectionTerritory.find(
            {'root_territory': {'$ne': True}})
        territoriesWithInvalidCriteriaOrder = []
        territoriesWithUpdateProblems = []
        territoriesUpdated = []

        clauseInvalidCriteria = ''
        clauseUpdateProblems = ''
        cluaseUpdated = ''

        async for territory in territories:
            validCriteria: bool = await verify_criteria(territory['criteria_order'])
            if not validCriteria:
                territoriesWithInvalidCriteriaOrder.append(
                    territory['territory_name'])
            else:
                accountsList = await evaluate_criteria_order(territory['criteria_order'], territory['account_rules'])
                updatedDoc = await collectionTerritory.update_one(
                    {'_id': territory['_id']}, {'$set': {'accounts': accountsList}})

                if updatedDoc.matched_count == 0:
                    territoriesWithUpdateProblems.append(
                        territory['territory_name'])
                else:
                    territoriesUpdated.append(territory['territory_name'])

        if len(territoriesWithInvalidCriteriaOrder) > 0:
            clauseInvalidCriteria = 'Criteria order not valid for territories: ' + \
                str(territoriesWithInvalidCriteriaOrder) + '. '

        if len(territoriesWithUpdateProblems) > 0:
            clauseUpdateProblems = 'Territories with update problem: ' + \
                str(territoriesWithUpdateProblems) + '. '

        if len(territoriesUpdated) > 0:
            cluaseUpdated = 'Territories successfully updated: ' + \
                str(territoriesUpdated) + '.'

        return {'message': clauseInvalidCriteria + clauseUpdateProblems + cluaseUpdated}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def delete_territory(territoryToDelete: str, territoryToTransfer: str) -> dict:
    try:
        rootTerritory = await collectionTerritory.find_one({'root_territory': True, 'territory_name': territoryToDelete})

        if rootTerritory is not None:
            return {'type': 'error', 'message': 'The root territory cannot be deleted.'}

        if territoryToDelete == territoryToTransfer:
            return {'type': 'error', 'message': 'Both territories cannot be the same.'}

        recordCount = await collectionTerritory.count_documents({'territory_name': territoryToDelete})
        if recordCount == 0:
            return {'type': 'error', 'message': 'The territory to delete is not present in the system.'}

        recordChildCount = await collectionTerritory.count_documents({'parent_territory': territoryToDelete})
        if recordChildCount and len(territoryToTransfer) == 0:
            return {'type': 'error', 'message': 'Children territories exist, but a territory to transfer is not provided.'}

        if recordChildCount:
            recordTransferCount = await collectionTerritory.count_documents({'territory_name': territoryToTransfer})
            if recordTransferCount == 0:
                return {'type': 'error', 'message': 'The territory to transfer is not present in the system.'}

        async with await mongoClient.start_session() as transactionSession:
            async with transactionSession.start_transaction():
                if recordChildCount:
                    resultChildTerritories = await collectionTerritory.update_many(
                        {'parent_territory': territoryToDelete},
                        {'$set': {
                         'parent_territory': territoryToTransfer}},
                        session=transactionSession)

                resultUserDoc = await collectionUser.update_many(
                    {'territories': territoryToDelete},
                    {'$pull': {'territories': territoryToDelete}},
                    session=transactionSession)

                resultDoc = await collectionTerritory.delete_one(
                    {'territory_name': territoryToDelete}, session=transactionSession)

        updateStatement = ''
        if recordChildCount:
            updateStatement = ' Parent of ' + \
                str(resultChildTerritories.modified_count) + \
                ' children territories updated.'

        userUpdateClause = ' ' + \
            str(resultUserDoc.modified_count) + ' users updated.'

        return {'message': str(resultDoc.deleted_count) +
                ' territory deleted.' +
                updateStatement + userUpdateClause}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}
