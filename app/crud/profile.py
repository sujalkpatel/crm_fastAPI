from bson.objectid import ObjectId
import datetime

from ..db import setup_collection
from ..core.config import (
    logger, collectionModuleFields, collectionProfile, collectionUser)

dictModuleOperations = dict()
setModuleImportRecords = set()
setModuleExportRecords = set()
setModuleConvert = set()
setModulePrintView = set()
setUserManagement = {
    "Users",
    "Groups",
    "Profiles",
    "Roles",
    "Data Sharing"
}


async def load_dict_module_operations():
    dictModuleOperations.clear()
    results = await get_data_module_operations()

    for record in results['records']:
        dictModuleOperations[record['module_name']] = set(record['operations'])
    # logger.debug(str(dictModuleOperations))


async def load_set_module_import_records():
    setModuleImportRecords.clear()
    results = await get_data_import_records()

    for record in results['records']:
        setModuleImportRecords.add(record['module_name'])
    # logger.debug(str(setModuleImportRecords))


async def load_set_module_export_records():
    setModuleExportRecords.clear()
    results = await get_data_export_records()

    for record in results['records']:
        setModuleExportRecords.add(record['module_name'])
    # logger.debug(str(setModuleExportRecords))


async def load_set_module_convert():
    setModuleConvert.clear()
    results = await get_data_convert()

    for record in results['records']:
        setModuleConvert.add(record['module_name'])
    # logger.debug(str(setModuleConvert))


async def load_set_module_print_view():
    setModulePrintView.clear()
    results = await get_data_print_view()

    for record in results['records']:
        setModulePrintView.add(record['module_name'])
    # logger.debug(str(setModulePrintView))


async def load_data_structures():
    await load_dict_module_operations()
    await load_set_module_import_records()
    await load_set_module_export_records()
    await load_set_module_convert()
    await load_set_module_print_view()
    logger.debug('Data structures loaded.')


async def create_profile_collection():
    await setup_collection.create_collection('profile')
    await load_data_structures()
    await create_administrator_profile()
    await create_standard_profile()


async def create_administrator_profile():
    recordCount = await collectionProfile.count_documents({'profile_name': 'Administrator'})
    if recordCount:
        logger.debug('The Administrator profile exists.')
        return

    profileData = {}
    profileData['profile_name'] = 'Administrator'
    profileData['profile_description'] = 'Pre-defined Administrator profile.'

    profileData['modules'] = []
    for module in dictModuleOperations:
        profileData['modules'].append(
            {
                'module_name': module,
                'operations': list(dictModuleOperations[module])
            }
        )

    profileData['import_records'] = list(setModuleImportRecords)
    profileData['export_records'] = list(setModuleExportRecords)
    profileData['convert'] = list(setModuleConvert)
    profileData['print_view'] = list(setModulePrintView)
    profileData['user_management'] = list(setUserManagement)

    profileData['created_by'] = 'system_default'
    profileData['created_at'] = datetime.datetime.now()
    profileData['modified_by'] = 'system_default'
    profileData['modified_at'] = datetime.datetime.now()

    result = await collectionProfile.insert_one(profileData)
    logger.debug('New Administrator Profile created with id: ' +
                 str(result.inserted_id))


async def create_standard_profile():
    recordCount = await collectionProfile.count_documents({'profile_name': 'Standard'})
    if recordCount:
        logger.debug('The Standard profile exists.')
        return

    profileData = {}
    profileData['profile_name'] = 'Standard'
    profileData['profile_description'] = 'Pre-defined Standard profile.'

    profileData['modules'] = []
    for module in dictModuleOperations:
        profileData['modules'].append(
            {
                'module_name': module,
                'operations': list(dictModuleOperations[module])
            }
        )

    profileData['import_records'] = list(setModuleImportRecords)
    profileData['export_records'] = list(setModuleExportRecords)
    profileData['convert'] = list(setModuleConvert)
    profileData['print_view'] = list(setModulePrintView)
    profileData['user_management'] = []

    profileData['created_by'] = 'system_default'
    profileData['created_at'] = datetime.datetime.now()
    profileData['modified_by'] = 'system_default'
    profileData['modified_at'] = datetime.datetime.now()

    result = await collectionProfile.insert_one(profileData)
    logger.debug('New Standard Profile created with id: ' +
                 str(result.inserted_id))


async def add_operations_in_activity_module(activityOperation: dict, operations: dict):
    if 'view' in operations and operations['view']:
        activityOperation['operations'].append('view')

    if 'create' in operations and operations['create']:
        activityOperation['operations'].append('create')

    if 'edit' in operations and operations['edit']:
        activityOperation['operations'].append('edit')

    if 'delete' in operations and operations['delete']:
        activityOperation['operations'].append('delete')


async def add_activity_module_operations(resultModules: list, module: dict):
    taskOperation = {
        'module_name': 'Task',
        'module_label': 'Task',
        'module_label_plural': 'Tasks',
        'operations': []
    }
    await add_operations_in_activity_module(
        taskOperation,
        module['options']['Task']['operations']
    )
    resultModules.append(taskOperation)

    eventOperation = {
        'module_name': 'Event',
        'module_label': 'Event',
        'module_label_plural': 'Events',
        'operations': []
    }
    await add_operations_in_activity_module(
        eventOperation,
        module['options']['Event']['operations']
    )
    resultModules.append(eventOperation)

    callOperation = {
        'module_name': 'Call',
        'module_label': 'Call',
        'module_label_plural': 'Calls',
        'operations': []
    }
    await add_operations_in_activity_module(
        callOperation,
        module['options']['Call']['operations']
    )
    resultModules.append(callOperation)


async def get_data_module_operations() -> dict:
    try:
        modules = collectionModuleFields.find(
            {'type': 'module'},
            projection={
                'module_name': 1,
                'module_label': 1,
                'module_label_plural': 1,
                'options.operations': 1,
                'options.Task': 1,
                'options.Event': 1,
                'options.Call': 1,
                '_id': 0
            }
        )

        resultModules = []

        async for module in modules:
            operationDict = {}

            if module['module_name'] == 'Activity':
                await add_activity_module_operations(resultModules, module)
                continue

            operationDict['module_name'] = module['module_name']
            operationDict['module_label'] = module['module_label']
            operationDict['module_label_plural'] = module['module_label_plural']

            operationDict['operations'] = []
            if 'view' in module['options']['operations'] and \
                    module['options']['operations']['view']:
                operationDict['operations'].append('view')

            if 'create' in module['options']['operations'] and \
                    module['options']['operations']['create']:
                operationDict['operations'].append('create')

            if 'edit' in module['options']['operations'] and \
                    module['options']['operations']['edit']:
                operationDict['operations'].append('edit')

            if 'delete' in module['options']['operations'] and \
                    module['options']['operations']['delete']:
                operationDict['operations'].append('delete')

            resultModules.append(operationDict)

        resultModules.append(
            {
                'module_name': 'Attachment',
                'module_label': 'Attachment',
                'module_label_plural': 'Attachments',
                'operations': ['view', 'create', 'delete']
            }
        )

        return {'records': resultModules, 'message': 'Module operations retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def add_import_records_for_activity_module(resultModules: list, module: dict):
    if module['options']['Task']['import_records']:
        resultModules.append(
            {
                'module_name': 'Task',
                'module_label': 'Task',
                'module_label_plural': 'Tasks'
            }
        )

    if module['options']['Event']['import_records']:
        resultModules.append(
            {
                'module_name': 'Event',
                'module_label': 'Event',
                'module_label_plural': 'Events'
            }
        )

    if module['options']['Call']['import_records']:
        resultModules.append(
            {
                'module_name': 'Call',
                'module_label': 'Call',
                'module_label_plural': 'Calls'
            }
        )


async def get_data_import_records() -> dict:
    try:
        modules = modules = collectionModuleFields.find(
            {'type': 'module', '$or': [
                {'options.import_records': True},
                {'module_name': 'Activity'}
            ]},
            projection={
                'module_name': 1,
                'module_label': 1,
                'module_label_plural': 1,
                'options.Task': 1,
                'options.Event': 1,
                'options.Call': 1,
                '_id': 0
            }
        )

        resultModules = []

        async for module in modules:
            if module['module_name'] == 'Activity':
                await add_import_records_for_activity_module(resultModules, module)
                continue

            resultModules.append(
                {
                    'module_name': module['module_name'],
                    'module_label': module['module_label'],
                    'module_label_plural': module['module_label_plural']
                }
            )
        return {'records': resultModules, 'message': 'Import records modules retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def add_export_records_for_activity_module(resultModules: list, module: dict):
    if module['options']['Task']['export']:
        resultModules.append(
            {
                'module_name': 'Task',
                'module_label': 'Task',
                'module_label_plural': 'Tasks'
            }
        )

    if module['options']['Event']['export']:
        resultModules.append(
            {
                'module_name': 'Event',
                'module_label': 'Event',
                'module_label_plural': 'Events'
            }
        )

    if module['options']['Call']['export']:
        resultModules.append(
            {
                'module_name': 'Call',
                'module_label': 'Call',
                'module_label_plural': 'Calls'
            }
        )


async def get_data_export_records() -> dict:
    try:
        modules = modules = collectionModuleFields.find(
            {'type': 'module', '$or': [
                {'options.export': True},
                {'module_name': 'Activity'}
            ]},
            projection={
                'module_name': 1,
                'module_label': 1,
                'module_label_plural': 1,
                'options.Task': 1,
                'options.Event': 1,
                'options.Call': 1,
                '_id': 0
            }
        )

        resultModules = []

        async for module in modules:
            if module['module_name'] == 'Activity':
                await add_export_records_for_activity_module(resultModules, module)
                continue

            resultModules.append(
                {
                    'module_name': module['module_name'],
                    'module_label': module['module_label'],
                    'module_label_plural': module['module_label_plural']
                }
            )
        return {'records': resultModules, 'message': 'Export records modules retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_data_convert() -> dict:
    try:
        modules = modules = collectionModuleFields.find(
            {'type': 'module', 'options.convert': True},
            projection={
                'module_name': 1,
                'module_label': 1,
                'module_label_plural': 1,
                '_id': 0
            }
        )

        resultModules = []

        async for module in modules:
            resultModules.append(
                {
                    'module_name': module['module_name'],
                    'module_label': module['module_label'],
                    'module_label_plural': module['module_label_plural']
                }
            )
        return {'records': resultModules, 'message': 'Convert modules retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_data_print_view() -> dict:
    try:
        modules = modules = collectionModuleFields.find(
            {'type': 'module', 'options.print_view': True},
            projection={
                'module_name': 1,
                'module_label': 1,
                'module_label_plural': 1,
                '_id': 0
            }
        )

        resultModules = []

        async for module in modules:
            resultModules.append(
                {
                    'module_name': module['module_name'],
                    'module_label': module['module_label'],
                    'module_label_plural': module['module_label_plural']
                }
            )
        return {'records': resultModules, 'message': 'Print view modules retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def create_profile(requestData: dict) -> dict:
    try:
        existingProfile = await collectionProfile.find_one({'profile_name': requestData['clone_profile']})

        if existingProfile is None:
            return {'type': 'error', 'message': 'The profile to be cloned does not exist.'}

        profileData = {}
        profileData['profile_name'] = requestData['profile_name']
        profileData['profile_description'] = requestData['profile_description']

        profileData['modules'] = existingProfile['modules']
        profileData['import_records'] = existingProfile['import_records']
        profileData['export_records'] = existingProfile['export_records']
        profileData['convert'] = existingProfile['convert']
        profileData['print_view'] = existingProfile['print_view']
        profileData['user_management'] = existingProfile['user_management']

        profileData['created_by'] = 'system_default'
        profileData['created_at'] = datetime.datetime.now()
        profileData['modified_by'] = 'system_default'
        profileData['modified_at'] = datetime.datetime.now()

        result = await collectionProfile.insert_one(profileData)

        return {'message': 'New profile record created.', 'id': str(result.inserted_id)}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_profiles_list_with_options(start: int, length: int, search: str) -> dict:
    try:
        query = {}
        sortFields = [('profile_name', 1)]
        profileNames = []

        if len(search) > 0:
            query['profile_name'] = {'$regex': '^' + search, '$options': 'i'}

        recordCount = await collectionProfile.count_documents(query)
        profiles = collectionProfile.find(query, projection={
                                          'profile_name': 1, '_id': 0}).skip(
                                              start).limit(length).sort(sortFields)

        async for profile in profiles:
            profileNames.append(profile['profile_name'])

        return {'profile_name': profileNames, 'recordsTotal': recordCount, 'message': 'Profiles retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_profiles_list_for_datatables(start: int, length: int, sortBy: str, sortOrder: str, search: str) -> dict:
    try:
        query = {}
        results = {'records': []}
        sortFields = [(sortBy, -1 if sortOrder == 'desc' else 1)]

        if len(search) > 0:
            query['$or'] = [
                {
                    'profile_name': {
                        '$regex': '^' + search,
                        '$options': 'i'
                    }
                }
            ]
            query['$or'].append(
                {
                    'profile_description': {
                        '$regex': '^' + search,
                        '$options': 'i'
                    }
                }
            )

        recordCount = await collectionProfile.count_documents(query)
        records = collectionProfile.find(
            query,
            projection={
                'profile_name': 1,
                'profile_description': 1,
                'created_by': 1,
                'created_at': 1,
                'modified_by': 1,
                'modified_at': 1
            }
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


async def get_profile(recordId: str) -> dict:
    try:
        resultDoc = await collectionProfile.find_one({'_id': ObjectId(recordId)})

        if resultDoc is None:
            return {'type': 'error', 'message': 'No profile found for the provided Id.'}

        resultDoc['_id'] = str(resultDoc['_id'])

        return {'record': resultDoc, 'message': 'A profile record retrieved successfully.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def add_modules_in_update_profile(profileData: dict, requestData: dict):
    loadStatus = False
    profileData['modules'] = []

    for module in requestData['modules']:
        if module['module_name'] not in dictModuleOperations and not loadStatus:
            await load_dict_module_operations()
            loadStatus = True

        if module['module_name'] not in dictModuleOperations and loadStatus:
            return {
                'type': 'error',
                'message': module['module_name'] + ' is not valid.'
            }

        operationList = []
        for operation in module['operations']:
            if operation not in dictModuleOperations[module['module_name']] and not loadStatus:
                await load_dict_module_operations()
                loadStatus = True

            if operation not in dictModuleOperations[module['module_name']] and loadStatus:
                return {
                    'type': 'error',
                    'message': operation + ' is not valid operation for module ' + module['module_name'] + '.'
                }

            operationList.append(operation)

        profileData['modules'].append(
            {
                'module_name': module['module_name'],
                'operations': operationList
            }
        )


async def add_import_records_for_update_profile(profileData: dict, requestData: dict):
    loadStatus = False
    profileData['import_records'] = []

    for module in requestData['import_records']:
        if module not in setModuleImportRecords and not loadStatus:
            await load_set_module_import_records()
            loadStatus = True

        if module not in setModuleImportRecords and loadStatus:
            return {
                'type': 'error',
                'message': module + ' is not valid for import records.'
            }

        profileData['import_records'].append(module)


async def add_export_records_for_update_profile(profileData: dict, requestData: dict):
    loadStatus = False
    profileData['export_records'] = []

    for module in requestData['export_records']:
        if module not in setModuleExportRecords and not loadStatus:
            await load_set_module_export_records()
            loadStatus = True

        if module not in setModuleExportRecords and loadStatus:
            return {
                'type': 'error',
                'message': module + ' is not valid for export records.'
            }

        profileData['export_records'].append(module)


async def add_convert_for_update_profile(profileData: dict, requestData: dict):
    loadStatus = False
    profileData['convert'] = []

    for module in requestData['convert']:
        if module not in setModuleConvert and not loadStatus:
            await load_set_module_convert()
            loadStatus = True

        if module not in setModuleConvert and loadStatus:
            return {
                'type': 'error',
                'message': module + ' is not valid for convert records.'
            }

        profileData['convert'].append(module)


async def add_print_view_for_update_profile(profileData: dict, requestData: dict):
    loadStatus = False
    profileData['print_view'] = []

    for module in requestData['print_view']:
        if module not in setModulePrintView and not loadStatus:
            await load_set_module_print_view()
            loadStatus = True

        if module not in setModulePrintView and loadStatus:
            return {
                'type': 'error',
                'message': module + ' is not valid for print view records.'
            }

        profileData['print_view'].append(module)


async def add_user_management_for_update_profile(profileData: dict, requestData: dict):
    profileData['user_management'] = []

    for module in requestData['user_management']:
        if module not in setUserManagement:
            return {
                'type': 'error',
                'message': module + ' is not valid for user management.'
            }

        profileData['user_management'].append(module)


async def update_profile(recordId: str, requestData: dict) -> dict:
    try:
        recordCount = await collectionProfile.count_documents({'_id': ObjectId(recordId)})

        if recordCount == 0:
            return {'type': 'error', 'message': 'No matching profile record found to be updated.'}

        requestData = requestData['updated_record']

        profileData = {}
        if 'modules' in requestData:
            result = await add_modules_in_update_profile(profileData, requestData)
            if result is not None:
                return result

        if 'import_records' in requestData:
            result = await add_import_records_for_update_profile(profileData, requestData)
            if result is not None:
                return result

        if 'export_records' in requestData:
            result = await add_export_records_for_update_profile(profileData, requestData)
            if result is not None:
                return result

        if 'convert' in requestData:
            result = await add_convert_for_update_profile(profileData, requestData)
            if result is not None:
                return result

        if 'print_view' in requestData:
            result = await add_print_view_for_update_profile(profileData, requestData)
            if result is not None:
                return result

        if 'user_management' in requestData:
            result = await add_user_management_for_update_profile(profileData, requestData)
            if result is not None:
                return result

        profileData['modified_by'] = 'system_default'
        profileData['modified_at'] = datetime.datetime.now()

        result = await collectionProfile.update_one(
            {'_id': ObjectId(recordId)},
            {'$set': profileData}
        )
        return {'message': str(result.modified_count) + ' profile record updated.'}
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_users_list_by_profile(profileName: str) -> dict:
    try:
        recordCount = await collectionUser.count_documents({'profile': profileName})
        users = collectionUser.find(
            {'profile': profileName},
            projection={
                'first_name': 1,
                'last_name': 1,
                'email': 1,
                'role': 1,
                'profile': 1,
                'status': 1
            }
        )

        records = []
        async for user in users:
            user['_id'] = str(user['_id'])
            records.append(user)

        return {
            'records': records,
            'recordsTotal': recordCount,
            'message': 'Users retrieved successfully.'
        }
    except Exception as e:
        logger.error(str(type(e).__name__) + ': ' + str(e))
        return {'type': 'exception',
                'errorType': str(type(e).__name__),
                'errorMessage': str(e),
                'message': 'An error has occured.'}


async def get_count_by_profile_module_operation(profileName: str, moduleName: str, operation: str, activityList: list = []) -> int:
    if moduleName == 'Activity':
        profileCount = await collectionProfile.count_documents(
            {
                'profile_name': profileName,
                'modules.module_name': {'$in': ['Task', 'Event', 'Call']},
                'modules.operations': operation
            }
        )

        if profileCount:
            profileDoc = await collectionProfile.find_one(
                {
                    'profile_name': profileName,
                    'modules.module_name': {'$in': ['Task', 'Event', 'Call']},
                    'modules.operations': operation
                },
                projection={'modules': 1,
                            'operations': 1}
            )

            for module in profileDoc['modules']:
                if module['module_name'] in {'Task', 'Event', 'Call'} \
                        and operation in module['operations']:
                    activityList.append(module['module_name'])
        return profileCount

    return await collectionProfile.count_documents(
        {
            'profile_name': profileName,
            'modules.module_name': moduleName,
            'modules.operations': operation
        }
    )
