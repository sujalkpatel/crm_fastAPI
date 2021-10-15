from ..core.config import (logging, logger, collectionProfile,
                           collectionModuleFields, dictProfiles,
                           moduleFieldTypes, moduleBsonFieldTypes,
                           moduleTypeFields, moduleUniqueFields,
                           moduleBsonTypeFields, collectionBsonTypeFields)

# from ..database import crmDB


async def load_logger():
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s:\t{%(pathname)s:%(lineno)d}\t%(message)s', '%Y-%m-%d %H:%M:%S')

    ch.setFormatter(formatter)

    logger.addHandler(ch)


async def load_profiles():
    dictProfiles.clear()

    async for profile in collectionProfile.find():
        profile['_id'] = str(profile['_id'])
        # logger.debug(profile['_id'])
        dictProfiles[profile['profile_name']] = profile

    logger.debug('Profiles loaded.')
    # logger.debug(dictProfiles)


async def load_module_field_types():
    moduleFieldTypes.clear()

    async for module in collectionModuleFields.find({'type': 'module', 'module_name': {'$ne': 'Activity'}}, projection={
        'module_name': 1, 'module_fields.field_name': 1,
            'module_fields.field_type': 1, '_id': 0}):
        fieldTypes = {}

        for moduleField in module['module_fields']:
            fieldTypes[moduleField['field_name']] = moduleField['field_type']
        moduleFieldTypes[module['module_name']] = fieldTypes

    logger.debug('Field->Types loaded.')
    # logger.debug(moduleFieldTypes)


async def load_module_bson_field_types():
    moduleBsonFieldTypes.clear()

    async for module in collectionModuleFields.find({'type': 'module', 'module_name': {'$ne': 'Activity'}}, projection={
        'module_name': 1, 'module_fields.field_name': 1,
            'module_fields.bson_type': 1, '_id': 0}):
        fieldTypes = {}

        for moduleField in module['module_fields']:
            fieldTypes[moduleField['field_name']] = moduleField['bson_type']
        moduleBsonFieldTypes[module['module_name']] = fieldTypes

    logger.debug('Field->BsonTypes loaded.')
    # logger.debug(moduleBsonFieldTypes)


async def load_module_type_fields():
    moduleTypeFields.clear()

    async for module in collectionModuleFields.find({'type': 'module', 'module_name': {'$ne': 'Activity'}}, projection={
        'module_name': 1, 'module_fields.field_name': 1,
            'module_fields.field_type': 1, '_id': 0}):
        typeFields = {}

        for moduleField in module['module_fields']:
            if moduleField['field_type'] in typeFields:
                typeFields[moduleField['field_type']].append(
                    moduleField['field_name'])
            else:
                typeFields[moduleField['field_type']] = [
                    moduleField['field_name']]
        moduleTypeFields[module['module_name']] = typeFields

    logger.debug('Type->Fields loaded.')
    # logger.debug(moduleTypeFields)


async def load_module_unique_fields():
    moduleUniqueFields.clear()

    async for module in collectionModuleFields.find(
            {'type': 'module'}, projection={'module_name': 1, 'unique_fields': 1, '_id': 0}):
        moduleUniqueFields[module['module_name']] = module['unique_fields']

    logger.debug('Module->Unique Fields loaded.')
    # logger.debug(moduleUniqueFields)


async def load_module_bson_type_fields():
    moduleBsonTypeFields.clear()

    async for module in collectionModuleFields.find({'type': 'module', 'module_name': {'$ne': 'Activity'}}, projection={
            '_id': 0}):
        typeFields = {}
        typeFields['id'] = []

        for moduleField in module['module_fields']:
            fieldName = moduleField['field_name']
            if moduleField['bson_type'] in typeFields:
                typeFields[moduleField['bson_type']].append(fieldName)
            else:
                typeFields[moduleField['bson_type']] = [fieldName]

            if moduleField['bson_type'] == 'object' or moduleField['bson_type'] == 'array':
                objectFields = []
                if moduleField['bson_type'] == 'object':
                    objectFields = moduleField['field_object_attribute']
                else:
                    objectFields = moduleField['field_array_element']

                for objectField in objectFields:
                    if objectField['field_name'][-3:] != '_id':
                        if objectField['bson_type'] in typeFields:
                            typeFields[objectField['bson_type']].append(
                                fieldName + '.' + objectField['field_name'])
                        else:
                            typeFields[objectField['bson_type']] = [
                                fieldName + '.' + objectField['field_name']]

                    else:
                        typeFields['id'].append(
                            fieldName + '.' + objectField['field_name'])

        # logger.debug(module['module_name'] + ': ' + str(typeFields))

        moduleBsonTypeFields[module['module_name']] = typeFields
    logger.debug('BsonType->Fields for modules loaded.')


async def load_collection_bson_type_fields():
    collectionBsonTypeFields.clear()

    async for collection in collectionModuleFields.find({'type': 'collection'}, projection={
            '_id': 0}):
        typeFields = {}
        typeFields['id'] = []

        for collectionField in collection['collection_fields']:
            fieldName = collectionField['field_name']
            if collectionField['bson_type'] in typeFields:
                typeFields[collectionField['bson_type']].append(fieldName)
            else:
                typeFields[collectionField['bson_type']] = [fieldName]

            if collectionField['bson_type'] == 'object' or collectionField['bson_type'] == 'array':
                objectFields = []
                if collectionField['bson_type'] == 'object':
                    objectFields = collectionField['field_object_attribute']
                else:
                    objectFields = collectionField['field_array_element']

                for objectField in objectFields:
                    if objectField['field_name'][-3:] != '_id':
                        if objectField['bson_type'] in typeFields:
                            typeFields[objectField['bson_type']].append(
                                fieldName + '.' + objectField['field_name'])
                        else:
                            typeFields[objectField['bson_type']] = [
                                fieldName + '.' + objectField['field_name']]

                    else:
                        typeFields['id'].append(
                            fieldName + '.' + objectField['field_name'])

        # logger.debug(collection['collection_name'] + ': ' + str(typeFields))

        collectionBsonTypeFields[collection['collection_name']] = typeFields
    logger.debug('BsonType->Fields for collections loaded.')


async def load_local_data():
    # await load_logger()
    await load_profiles()
    await load_module_field_types()
    await load_module_bson_field_types()
    await load_module_type_fields()
    await load_module_unique_fields()
    await load_module_bson_type_fields()
    await load_collection_bson_type_fields()
    return
