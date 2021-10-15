import base64
from bson import BSON
from bson.binary import Binary

from ..db import setup_collection
from ..core.config import collectionCompany, logger, mongoClient, collectionTerritory
from .territory import create_root_territory


async def create_one_company_record():
    recordCount = await collectionCompany.count_documents({})
    if recordCount:
        logger.debug('A company record exists.')
        existingCompany = await collectionCompany.find_one({})
        await create_root_territory(existingCompany['company_name'])
        return

    insertDocument = {}
    insertDocument['company_name'] = ''
    insertDocument['logo'] = ''
    insertDocument['logo_content'] = Binary(bytearray())
    insertDocument['email'] = ''
    insertDocument['phone'] = ''
    insertDocument['mobile'] = ''
    insertDocument['website'] = ''
    insertDocument['fax'] = ''

    insertDocument['super_admin'] = {}
    insertDocument['super_admin']['_id'] = ''
    insertDocument['super_admin']['first_name'] = ''
    insertDocument['super_admin']['last_name'] = ''
    insertDocument['super_admin']['email'] = ''
    insertDocument['super_admin']['role'] = ''

    insertDocument['street'] = ''
    insertDocument['city'] = ''
    insertDocument['state'] = ''
    insertDocument['postal_code'] = ''
    insertDocument['hierarchy_structure'] = ''

    result = await collectionCompany.insert_one(insertDocument)
    logger.debug('New company record created with id: ' +
                 str(result.inserted_id))
    await create_root_territory('')


async def create_company_collection():
    await setup_collection.create_collection('company')
    await create_one_company_record()


async def get_company_record() -> dict:
    try:
        resultDoc = await collectionCompany.find_one({})

        if resultDoc is None:
            return {'type': 'error', 'message': 'No company record found.'}

        resultDoc['_id'] = str(resultDoc['_id'])
        if len(resultDoc['logo_content']) > 0:
            resultDoc['logo_content'] = base64.b64encode(
                resultDoc['logo_content'])

        return {'record': resultDoc, 'message': 'The company record retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def update_company_record(requestData: dict) -> dict:
    try:
        resultDoc = await collectionCompany.find_one({})

        if resultDoc is None:
            return {'type': 'error', 'message': 'No company record found to be edited.'}

        requestData = requestData['updated_record']
        updateDocument = {}
        updateDocument['company_name'] = requestData['company_name']
        updateDocument['logo'] = requestData['logo']
        updateDocument['logo_content'] = Binary(
            base64.b64decode(requestData['logo_content']))
        updateDocument['email'] = requestData['email']
        updateDocument['phone'] = requestData['phone']
        updateDocument['mobile'] = requestData['mobile']
        updateDocument['website'] = requestData['website']
        updateDocument['fax'] = requestData['fax']

        updateDocument['super_admin'] = {}
        updateDocument['super_admin']['_id'] = requestData['super_admin']['_id']
        updateDocument['super_admin']['first_name'] = requestData['super_admin']['first_name']
        updateDocument['super_admin']['last_name'] = requestData['super_admin']['last_name']
        updateDocument['super_admin']['email'] = requestData['super_admin']['email']
        updateDocument['super_admin']['role'] = requestData['super_admin']['role']

        updateDocument['street'] = requestData['street']
        updateDocument['city'] = requestData['city']
        updateDocument['state'] = requestData['state']
        updateDocument['postal_code'] = requestData['postal_code']
        updateDocument['hierarchy_structure'] = requestData['hierarchy_structure']

        # logger.debug(updateDocument)

        async with await mongoClient.start_session() as transactionSession:
            async with transactionSession.start_transaction():
                result = await collectionCompany.replace_one(
                    {'_id': resultDoc['_id']}, updateDocument,
                    session=transactionSession)

                if result.matched_count == 0:
                    return {'type': 'error', 'message': 'No matching company record found to be updated.'}

                existingTerritory = await collectionTerritory.find_one({'root_territory': True})
                resultTerritory = await collectionTerritory.update_one({'root_territory': True},
                                                                       {'$set': {
                                                                           'territory_name': requestData['company_name']}},
                                                                       session=transactionSession)
                resultChildTerritories = await collectionTerritory.update_many({'parent_territory': existingTerritory['territory_name']},
                                                                               {'$set': {
                                                                                   'parent_territory': requestData['company_name']}},
                                                                               session=transactionSession)

        return {'message': str(result.modified_count) + ' company record updated. ' +
                str(resultTerritory.modified_count) + ' root territory updated. ' +
                str(resultChildTerritories.modified_count) + ' child territories of root updated.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}
