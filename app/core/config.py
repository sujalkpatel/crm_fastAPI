import logging
from motor import motor_asyncio

secretKey = '0a88205576749e27eb28708de73e355c228c527acf35281b67648c3829ece041'

mongoDB_connect_string = "mongodb://crmBackendApp:crm%401234abcd@localhost:27017/crm_db"

mongoClient = motor_asyncio.AsyncIOMotorClient(mongoDB_connect_string)

database = mongoClient.crm_db

gridFs = motor_asyncio.AsyncIOMotorGridFSBucket(database)

collectionModuleFields = database.get_collection('module-fields')
collectionModuleAccount = database.get_collection('module_Account')
collectionProfile = database.get_collection('profile')
collectionUser = database.get_collection('user')
collectionCompany = database.get_collection('company')
collectionRole = database.get_collection('role')
collectionTerritory = database.get_collection('territory')
collectionGroup = database.get_collection('group')
collectionSharingRule = database.get_collection('sharing_rule')
collectionDefaultPermission = database.get_collection('default_permission')
collectionData = database.get_collection('data')

logger = logging.getLogger('crmLogger')

dictProfiles = dict()
moduleFieldTypes = dict()
moduleBsonFieldTypes = dict()
moduleTypeFields = dict()
moduleBsonTypeFields = dict()
moduleUniqueFields = dict()
activityFieldTypes = dict()
activityTypeFields = dict()
collectionBsonTypeFields = dict()
