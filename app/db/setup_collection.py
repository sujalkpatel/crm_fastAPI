from ..core.config import logger, database, collectionModuleFields


async def create_collection_indexes(collectionName: str, collectionFields: list, collectionFieldsUnique: list):
    collection = database.get_collection(collectionName)
    # logger.debug(collectionFieldsUnique)
    uniqueFields = []
    for collectionField in collectionFieldsUnique:
        uniqueFields.append((collectionField, 1))

    # logger.debug(uniqueFields)
    await collection.create_index(uniqueFields, unique=True, background=True)

    uniqueIndexes = await collection.index_information()
    logger.debug(uniqueIndexes)

    # textIndexes = []
    indexes = set()
    for collectionField in collectionFields:
        fieldName = collectionField['field_name']

        if fieldName in indexes:
            continue
        else:
            indexes.add(fieldName)

        if fieldName + '_1' not in uniqueIndexes:
            if collectionField['bson_type'] == 'object' or \
                    (collectionField['bson_type'] == 'array' and
                        len(collectionField['field_array_element']) > 1):
                await collection.create_index(fieldName + '.$**', background=True)

            else:
                order = 1
                if collectionField['bson_type'] == 'date':
                    order = -1
                await collection.create_index([(fieldName, order)], background=True)


async def add_fields(collectionName: str, jsonSchema: dict, jsonProperties: dict, collectionFields: list):
    for collectionField in collectionFields:
        if collectionName == 'module_Activity' and collectionField['field_name'] in jsonProperties:
            continue

        jsonField = {}
        jsonField['bsonType'] = collectionField['bson_type']

        # Handle JSON Object separately
        if collectionField['bson_type'] == 'object':
            jsonObjectProperties = {}

            for objectField in collectionField['field_object_attribute']:
                jsonObjectField = {}
                # logger.debug(objectField)
                jsonObjectField['bsonType'] = objectField['bson_type']

                jsonObjectProperties[objectField['field_name']
                                     ] = jsonObjectField

            jsonField['properties'] = jsonObjectProperties

        # Handle Array separately
        elif collectionField['bson_type'] == 'array':
            jsonItemInArrayField = {}

            # Create an array of objects if more than one field is present
            if len(collectionField['field_array_element']) > 1:
                jsonItemInArrayField['bsonType'] = 'object'

                jsonObjectProperties = {}

                for objectField in collectionField['field_array_element']:
                    jsonObjectField = {}
                    # logger.debug(objectField)
                    jsonObjectField['bsonType'] = objectField['bson_type']

                    if objectField['bson_type'] == 'object':
                        jsonInnerObjectProperties = {}

                        for innerObjectField in objectField['field_object_attribute']:
                            jsonInnerObjectField = {}
                            logger.debug(innerObjectField)

                            jsonInnerObjectField['bsonType'] = innerObjectField['bson_type']

                            jsonInnerObjectProperties[innerObjectField['field_name']
                                                      ] = jsonInnerObjectField

                        jsonObjectField['properties'] = jsonInnerObjectProperties

                    jsonObjectProperties[objectField['field_name']
                                         ] = jsonObjectField

                jsonItemInArrayField['properties'] = jsonObjectProperties

            # Create an array of primitive field if only one field is present
            elif len(collectionField['field_array_element']) == 1:
                # logger.debug(collectionField['field_array_element'][0])
                jsonItemInArrayField['bsonType'] = collectionField['field_array_element'][0]['bson_type']

            jsonField['items'] = jsonItemInArrayField
            jsonField['uniqueItems'] = True

        jsonProperties[collectionField['field_name']] = jsonField

        if(collectionName != 'module_Activity' and collectionField['field_required']):
            if 'required' not in jsonSchema:
                jsonSchema['required'] = []
            jsonSchema['required'].append(
                collectionField['field_name'])


async def create_collection_with_fields(collectionName: str, collectionFields: list, collectionFieldsUnique: list):
    jsonSchema = {}
    jsonSchema['bsonType'] = 'object'

    jsonProperties = {}

    await add_fields(collectionName, jsonSchema, jsonProperties, collectionFields)

    jsonSchema['properties'] = jsonProperties

    validator = {}
    validator['$jsonSchema'] = jsonSchema

    # Create Collection and Apply Validation
    await database.create_collection(collectionName)
    await database.command({'collMod': collectionName, 'validator': validator})

    # Apply Indexes
    await create_collection_indexes(collectionName, collectionFields, collectionFieldsUnique)


async def create_collection(collectionName: str):
    try:
        logger.debug('checking if ' + collectionName + ' is present.')
        collectionNames = await database.list_collection_names()

        if(collectionName not in collectionNames):
            logger.debug(collectionName + ' is not present.')
            logger.debug('creating ' + collectionName + ' collection.')

            collectionDocument = await collectionModuleFields.find_one(
                {'type': 'collection', 'collection_name': collectionName})

            collectionFields = collectionDocument['collection_fields']

            await create_collection_with_fields(collectionName, collectionFields, collectionDocument['unique_fields'])
        else:
            logger.debug(collectionName + ' is present already.')
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))


async def create_module_collections():
    try:
        async for module_document in collectionModuleFields.find({'type': 'module'}):
            # if module_document['module_name'] != 'Activity':
            module_document['_id'] = str(module_document['_id'])
            moduleName = 'module_' + module_document['module_name']

            logger.debug('checking if ' + moduleName + ' is present.')
            moduleNames = await database.list_collection_names()
            # logger.debug(moduleNames)
            if(moduleName not in moduleNames):
                logger.debug(moduleName + ' is not present.')
                logger.debug('creating ' + moduleName + ' collection.')

                moduleFields = module_document['module_fields']

                if moduleName == 'module_Activity':
                    moduleFields = []
                    moduleFields.append(module_document['module_fields'][0])
                    for tField in module_document['module_fields'][1]['activity_fields']['task_fields']:
                        moduleFields.append(tField)
                    for eField in module_document['module_fields'][1]['activity_fields']['event_fields']:
                        moduleFields.append(eField)
                    for cField in module_document['module_fields'][1]['activity_fields']['call_fields']:
                        moduleFields.append(cField)

                await create_collection_with_fields(moduleName, moduleFields, module_document['unique_fields'])

            else:
                logger.debug(moduleName + ' is present already.')

    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
