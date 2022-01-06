
import uuid,re
# import sqlalchemy.sql.default_comparator # Must include for pyinstaller
import sqlalchemy.sql.default_comparator
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from Local.config_local import mysql_credentials as dbCredentials
from datetime import datetime, timedelta
from Service.utility import string2Time, convertObjectArrayToDict


def findTableObj(tableName):
    """
    Find the table object in Base
    :param tableName:
    :return: tableObj
    """
    for tableObj in Base.classes:
        tableNameinBase = str(tableObj.__table__.fullname)
        if tableNameinBase == tableName:
            return tableObj
    return None

def getStatsRecord(fullSourceName):
    """
    Find & return the record in data_raw_data_stats table. If not existing, creat a new one
    :param fullSourceName:
    :param listDate:
    :return:
    """
    listDate = datetime.now().strftime("%Y-%m-%d")
    record = session.query(data_raw_data_stats).filter(data_raw_data_stats.listDate == listDate). \
        filter(data_raw_data_stats.source == fullSourceName).one_or_none()
    if record is None:
        record = data_raw_data_stats()
        record.fullSourceName = fullSourceName
        record.listDate = listDate
        session.add(record)
    return record

class Stats:
    def __init__(self, source):
        self.listDate = datetime.now().strftime("%Y-%m-%d")
        self.source = source
        self.listRecord =0
        self.detailRecord = 0
        self.archivedRecord =0
        self.unarchivedRecord = 0
        self.makeupRecord = 0
        self.valid = 0

    def add(self):
        dsStatsRecord = data_raw_data_stats()
        for attrName in self.__dict__:
            setattr(dsStatsRecord, attrName, getattr(self, attrName))
        session.add(dsStatsRecord)

    def update(self, dsStatsRecord):
        for attrName in self.__dict__:
            setattr(dsStatsRecord, attrName, getattr(self, attrName))

class Contacts:
    def __init__(self, source, company, name, contactChannel, contactDetails, lastUsedtime):
        self.uuid = str(uuid.uuid4())
        self.parentId = self.uuid
        self.name = name
        self.lastUsedName = ''
        self.fullName = ''
        self.fullNameEn = ''
        self.companyId = ''
        self.companyName = company
        self.lastCompanyName = ''
        self.contactChannel = contactChannel
        self.contactDetails = contactDetails
        self.usedSource = source
        self.lastUsedSource = ''
        self.lastUsedTime = lastUsedtime
        self.acqStage = 0
        self.acqStageUpdatedAt = None
        self.acqSenderUuid = ""
        self.optout = 0
        self.optoutDate = None
        self.comment = ''
        self.contactVerified = 0
        self.contactVerifiedAt = None

        if self.contactChannel == "Mobile":
            self.canWhatsapp = 1
        else:
            self.canWhatsapp = 0

    def add(self):
        contactRecord = data_contact()
        for attrName in self.__dict__:
            setattr(contactRecord, attrName, getattr(self, attrName))
        session.add(contactRecord)
        # session.add(data_contact(uuid=self.uuid,
        #                          parentId=self.parentId,
        #                          name=self.name,
        #                          lastUsedName=self.lastUsedName,
        #                          fullName=self.fullName,
        #                          fullNameEn=self.fullNameEn,
        #                          companyId=self.companyId,
        #                          companyName=self.companyName,
        #                          lastCompanyName=self.lastCompanyName,
        #                          contactChannel=self.contactChannel,
        #                          contactDetails=self.contactDetails,
        #                          usedSource=self.usedSource,
        #                          lastUsedSource=self.lastUsedSource,
        #                          lastUsedTime=self.lastUsedTime,
        #                          acqStage=self.acqStage,
        #                          acqStageUpdatedAt = self.acqStageUpdatedAt,
        #                          acqSenderUuid = self.acqSenderUuid,
        #                          optout=self.optout,
        #                          optoutDate=self.optoutDate,
        #                          comment=self.comment,
        #                          canWhatsapp=self.canWhatsapp
        #                          )
        #             )
        return self.uuid

class BigTableRecord:

    def __init__(self):
        self.id = str(uuid.uuid4())
        self.source = ""
        self.sourceId = ""
        self.sourceVehicleId = ""
        self.sourceURL = ""
        self.contactPersonId = ""
        self.title = ""
        self.brandId = 0
        self.brandCn = ""
        self.brandEn = ""
        self.modelId = 0
        self.modelName = ""
        self.modelDerivedName = ""
        self.year = 0
        self.carAge = 0
        self.priceNow = 0
        self.lastPrice = 0
        self.lastChangedPrice = 0
        self.priceOriginal = 0
        self.priceTag = ""
        self.lowestNewCarPrice = 0
        self.highestNewCarPrice = 0
        self.doors = 0
        self.numberOfSeat = ""
        self.engineVolume = ""
        self.maxOutput = ""
        self.transmissionType = ""
        self.miles = 0
        self.color = ""
        self.colorInterior = ""
        self.vehicleType = ""
        self.bodyType = ""
        self.additionalNote = ""
        self.otherNote = ""
        self.sourceImageURL1 = ""
        self.sourceImageURL2 = ""
        self.sourceImageURL3 = ""
        self.sourceImageURL4 = ""
        self.sourceImageURL5 = ""
        self.sourceVideoURL1 = ""
        self.previousOwners = 0
        self.isDirectOwner = 0
        self.warranty = ""
        self.underRegistration = ""
        self.licenseFee = ""
        self.licensePeriod = ""
        self.lastMaintenance = ""
        self.sold = 0 # 0 = not sold 1 = sold
        self.soldTime = None
        self.status = ""
        self.sourceCreateTime = None
        self.sourceUpdateTime = None
        self.firstSourceUpdateTime = None
        self.scanTime = None
        self.firstScanTime = None
        self.createdAt = None
        self.updatedAt = None
        self.archive = 0
        self.archiveDate = None
        self.invalidate = 0
        self.invalidateDate = None
        self.invalidateReason = ""
        self.mapped = 0
        self.inAvailableList = 0
        self.inPriceCheck = 0
        self.pending = 0
        self.pendingReason = ""
        self.pendingSetDate =None
        self.duplicate = 0
        self.duplicateId = ""
        self.updatedByAdmin = 0 # 0 = not updated by Admin; 1 = updated by Admin
        self.updatedByAdminUserId = ""
        self.possibleUnmatchedContactSkipped = 0
        self.firstAcqMesgGen = 0
        self.firstAcqMesgGenAt = None

    def assignValueFromRaw(self, source, rawTableRecord, rawRecordCategory):
        self.__assignInitialValue(source, rawTableRecord, rawRecordCategory)
        self.__setPrice(rawTableRecord, rawRecordCategory)
        self.__setTypeSubType(rawTableRecord)
        self.__setDistrictSuburb(rawTableRecord)
        self.__setBuildingYear()
        self.__setSellerType(rawTableRecord)
        self.__setContacts(rawTableRecord)
        self.__updateFirstScanTime()

    def addANewRecord(self):
        bigTableRecord = data_bigTable()
        for attrName in self.__dict__:
            setattr(bigTableRecord, attrName, getattr(self, attrName))

        session.add(bigTableRecord)

    def updateABigTableRecord(self, foundBigTableRecord):
        # Notes:
        # 1) uuid is not updated.
        # 2) The fields for invalidate are not updated. i.e. once a self is invalided in big table,
        #    this self will be invalidated forever until the admin updates it
        # 3) We may need to look at pending cases later

        excludedAttributes = ["uuid", "category", "firstScanTime",
                             "price_buy", "price_buy_org", "price_buy_int", "price_buy_org_int",
                             "price_rent", "price_rent_org", "price_rent_int", "price_rent_org_int"]

        # 1) Update the all attributes except the ones in excludedAttribute, which would be updated in step 2)
        for attrName in self.__dict__:
            if attrName not in excludedAttributes:
                setattr(foundBigTableRecord, attrName, getattr(self, attrName))

        # 2) Update the priceChanged
        if self.price_buy_int == foundBigTableRecord.price_buy_int and self.price_rent_org_int == foundBigTableRecord.price_rent_int:
            foundBigTableRecord.priceChanged = 0
        else:
            foundBigTableRecord.priceChanged = 1

        # 2) update category (0=buy, 1=rent, 2=both) & price info:

        if self.category == 0:
            foundBigTableRecord.price_buy = self.price_buy
            foundBigTableRecord.price_buy_org = self.price_buy_org
            if self.price_buy_org_int > 0:
                foundBigTableRecord.price_buy_org_int = self.price_buy_org_int
            elif foundBigTableRecord.price_buy_int != self.price_buy_int:
                foundBigTableRecord.price_buy_org_int = foundBigTableRecord.price_buy_int
            foundBigTableRecord.price_buy_int = self.price_buy_int

            if foundBigTableRecord.category == 1:
                foundBigTableRecord.category = 2
        else:
            foundBigTableRecord.price_rent = self.price_rent
            foundBigTableRecord.price_rent_org = self.price_rent_org

            if self.price_rent_org_int > 0:
                foundBigTableRecord.price_rent_org_int = self.price_rent_org_int
            elif foundBigTableRecord.price_rent_int != self.price_rent_int:
                foundBigTableRecord.price_rent_org_int = foundBigTableRecord.price_rent_int
            foundBigTableRecord.price_rent_int = self.price_rent_int

            if foundBigTableRecord.category == 0:
                foundBigTableRecord.category = 2

        # 3) Update the firstscantime in case that the raw data loading process a later raw bigTableRecord first
        if foundBigTableRecord.firstScanTime > self.firstScanTime:
            foundBigTableRecord.firstScanTime = self.firstScanTim

    def __assignInitialValue(self, source, rawTableRecord, rawRecordCategory):
        """
        Format the string values of rawTableRecord and assign them to self
        :param rawTableRecord: a rawTableRecord from DB
        :return:
        """
        self.source = source
        self.sourceId = rawTableRecord.sourceId.strip()
        self.propertyId = rawTableRecord.propertyID.strip()
        self.url = rawTableRecord.url.strip()
        self.title = rawTableRecord.title.strip()
        self.descr = rawTableRecord.descr.strip()
        self.mgtFee = rawTableRecord.mgtFee.strip()
        self.size_s = rawTableRecord.size_s.strip()
        self.size_g = rawTableRecord.size_g.strip()
        self.rooms = rawTableRecord.rooms.strip()
        self.years = rawTableRecord.years.strip()
        self.floor = rawTableRecord.floor.strip()
        self.district = rawTableRecord.district.strip()
        self.suburb = rawTableRecord.suburb.strip()
        self.estate = rawTableRecord.estate.strip()
        self.addr1 = rawTableRecord.addr1.strip()
        self.addr2 = rawTableRecord.addr2.strip()
        self.addr3 = rawTableRecord.addr3.strip()
        self.agent = rawTableRecord.agent.strip()
        self.img1 = rawTableRecord.img1.strip()
        self.img2 = rawTableRecord.img2.strip()
        self.img3 = rawTableRecord.img3.strip()
        self.img4 = rawTableRecord.img4.strip()
        self.img5 = rawTableRecord.img5.strip()
        self.img6 = rawTableRecord.img6.strip()
        self.img7 = rawTableRecord.img7.strip()
        self.img8 = rawTableRecord.img8.strip()
        self.img9 = rawTableRecord.img9.strip()
        self.img10 = rawTableRecord.img10.strip()
        self.scanTime = rawTableRecord.scanTime
        self.sourceCreatedAt = string2Time(rawTableRecord.sourceCreatedAt)
        self.sourceUpdatedAt = string2Time(rawTableRecord.sourceUpdatedAt)
        self.firstScanTime = rawTableRecord.scanTime

        self.sold = rawTableRecord.sold
        if rawTableRecord.sold == 1:
            self.soldTime = rawTableRecord.scanTime

        # 0: buy-only, 1: rent-only, 2: both
        if rawRecordCategory.lower() == "buy":
            self.category = 0
        else:
            self.category = 1

    def __setPrice(self, rawTableRecord, rawRecordCategory):
        from Service.utility import getPrice

        priceStr = rawTableRecord.price.strip()
        priceOrgStr = rawTableRecord.price_org.strip()
        priceInt = getPrice(priceStr)
        priceOrgInt = getPrice(priceOrgStr)

        if priceInt >= priceOrgInt * (1 + 0.2) and priceOrgInt > 0:
            # print("Found a issue self",self.source,self.sourceId)
            # print(priceStr, priceOrgStr, priceInt, priceOrgInt)
            # It could happens for Ricacorp Buy which mixes up with the 居屋 的補地價和未補地價的價錢
            priceOrgInt = 0
            priceOrgStr = ""

        if rawRecordCategory.lower() == "buy":
            self.price_buy = priceStr
            self.price_buy_org = priceOrgStr
            if priceOrgInt > 0:
                self.price_buy_org_int = priceOrgInt
            else:
                if self.price_buy_int != priceInt:
                    self.price_buy_org_int = self.price_buy_int
            self.price_buy_int = priceInt
        else:
            self.price_rent = priceStr
            self.price_rent_org = priceOrgStr
            if priceOrgInt > 0:
                self.price_rent_org_int = priceOrgInt
            else:
                if self.price_rent_int != priceInt:
                    self.price_rent_org_int = self.price_rent_int
            self.price_rent_int = priceInt

        # print(priceOrgNum, price_org)

    def __setContacts(self, rawTableRecord):

        def phoneContact(name, phone):
            """
            Return a self (list) of contact consists of phone sender, contact channel type (either Mobile or Fixed), and the contact name
            :param name:
            :param phone:
            :return:
            """
            mobileRegex = "^852(4[3-7][0-9]{6}|5[1-9][0-9]{6}|6[0-9]{7}|7[0-3][0-9]{6}|8[1-46-9][0-9]{6}|85[0-13-9][0-9]{5}|9[1-9][0-9]{6}|90[1-9][0-9]{5})$"  # HK Fixed-line sender patterns
            fixedRegex = "^852((2|3)[1-9][0-9]{6})$"  # HK Fixed-line sender patterns

            countryCode="852"
            phoneStr = countryCode+str(phone)
            if re.search(mobileRegex, phoneStr):
                return [name, "Mobile", phoneStr]
            elif re.search(fixedRegex, phoneStr):
                return [name, "Fixed", phoneStr]
            else:
                return []  # Invalid phone sender

        def emailContact(name, email):

            #Handle the strange case, such as "edwardl%40century21-goodwin.com%2C%20luckwong@century21-goodwin.com"
            # from house101
            import urllib
            email = urllib.parse.unquote(email)
            email = email.split(',')[0]
            email = email.split(' ')[0]

            emailRegex = "(\w+(?:[-+.]\w+)*@\w+(?:[-.]\w+)*\.\w+(?:[-.]\w+)*)"
            if re.search(emailRegex, email):
                return [name, "Email", email]
            else:
                return []  # Invalid email

        contacts = []
        phones = {rawTableRecord.phone1, rawTableRecord.whatsapp1}  # remove duplicated numbers
        for phone in phones:
            contact = phoneContact(rawTableRecord.contact1.strip(), phone)
            if contact:
                contacts.append(contact)
        phones2 = {rawTableRecord.phone2, rawTableRecord.whatsapp2}
        phones2.difference_update(phones)
        for phone in phones2:
            contact = phoneContact(rawTableRecord.contact2.strip(), phone)
            if contact:
                contacts.append(contact)
        phones.union(phones2)
        phones3 = {rawTableRecord.phone3, rawTableRecord.whatsapp3}
        phones3.difference_update(phones)
        for phone in phones3:
            contact = phoneContact(rawTableRecord.contact3.strip(), phone)
            if contact:
                contacts.append(contact)
        phones.union(phones3)
        phones4 = {rawTableRecord.phone4, rawTableRecord.whatsapp4}
        phones4.difference_update(phones)
        for phone in phones4:
            contact = phoneContact(rawTableRecord.contact4.strip(), phone)
            if contact:
                contacts.append(contact)
        phones.union(phones4)
        phones5 = {rawTableRecord.phone5, rawTableRecord.whatsapp5}
        phones5.difference_update(phones)
        for phone in phones5:
            contact = phoneContact(rawTableRecord.contact5.strip(), phone)
            if contact:
                contacts.append(contact)
        phones.union(phones5)
        contact = emailContact(rawTableRecord.contact1.strip(), rawTableRecord.email.strip())
        if contact:
            contacts.append(contact)

        contactsID = storeContacts(self.source, self.agent.strip(), self.scanTime, contacts)
        # Store only up to 5 contacts
        if len(contactsID) >= 1:
            self.contactId01 = contactsID[0]
        if len(contactsID) >= 2:
            self.contactId02 = contactsID[1]
        if len(contactsID) >= 3:
            self.contactId03 = contactsID[2]
        if len(contactsID) >= 4:
            self.contactId04 = contactsID[3]
        if len(contactsID) >= 5:
            self.contactId05 = contactsID[4]

    def __setSourceCreatedAt(self, rawTableRecord):
        time = string2Time(rawTableRecord.sourceCreatedAt)
        if self.sourceCreatedAt == None or self.sourceCreatedAt > time:
            # get an older sourceCreatedAt time
            self.sourceCreatedAt = time

    def __setSourceUpdatedAt(self, rawTableRecord):
        time = string2Time(rawTableRecord.sourceUpdatedAt)
        if self.sourceUpdatedAt is None or self.sourceUpdatedAt < time:
            # get a newer sourceUpdatedAt time
            self.sourceUpdatedAt = time

    def __updateFirstScanTime(self):
        if self.firstScanTime is None or self.firstScanTime < self.scanTime:
            self.firstScanTime = self.scanTime

    def setInvalidate(self, reason):
        self.invalidate = 1
        self.invalidateReason = reason
        self.invalidateAt = datetime.now()

    def setValidate(self, reason):
        self.invalidate = 0
        self.invalidateAt = None

    def setArchive(self):
        self.archive = 1
        self.archiveAt = datetime.now()

    def setUnArchive(self):
        self.archive = 0
        self.archiveAt = None

    def setPending(self, reason):
        self.pending = 1
        self.pendingReason = reason

    def setUnPending(self):
        self.pending = 0
        self.pendingUpdatedByAdmin = 1
        self.pendingUpdatedAt = datetime.now()

class AlertEvent():
    def __init__(self, alertSubscriptionUuid, bigtableUuid, new_or_updated):
        self.uuid = str(uuid.uuid4())
        self.alertSubscriptionUuid = alertSubscriptionUuid
        self.bigtableUuid = bigtableUuid
        self.new_or_updated = new_or_updated
        self.sent = 0
        self.sentAt = None
        self.visitCount = 0
        self.firstVisitAt = None
        self.lastVisitAt = None
        self.error = ""
        self.failedCount = 0
        self.retry = None

    def add(self):
        alertEvent = data_alert_event()
        for attrName in self.__dict__:
            setattr(alertEvent, attrName, getattr(self, attrName))
        session.add(alertEvent)

    def setSent(self):
        self.sent = 1
        self.sentAt = datetime.now()

    def haveVisit(self):
        self.visitCount += 1
        if self.firstVisitAt is None:
            self.lasVisitAt = datetime.now()
            self.firstVisitAt = datetime.now()
        else:
            self.lasVisitAt = self.firstVisitAt
            self.firstVisitAt = datetime.now()

    def haveError(self, error):
        self.error = error
        self.retry += 1

class DeliveryRecord:
    def __init__(self, initiator, initiatorUuid, sender, senderId, recipient, recipientId, channel):
        self.initiator = initiator
        self.initiatorUuid = initiatorUuid
        self.sender = sender
        self.senderId = senderId
        self.senderStatus = 1
        self.recipient = recipient
        self.recipientId = recipientId
        self.recipientStatus = 1
        self.channel = channel
        self.type1 = 0
        self.mesgPart1 = ""
        self.type2 = 0
        self.mesgPart2 = ""
        self.type3 = 0
        self.mesgPart3 = ""
        self.type4 = 0
        self.mesgPart4 = ""
        self.type5 = 0
        self.mesgPart5 = ""
        self.purpose = ""
        self.sent = 0
        self.sentAt = None
        self.retry = 0

    def add(self):
        deliveryRecord = data_delivery_queue()
        for attrName in self.__dict__:
         setattr(deliveryRecord, attrName, getattr(self, attrName))
        session.add(deliveryRecord)

def storeContacts(source, company, lastUsedtime, contacts):
    contactIDs = []
    for contact in contacts:
        name = contact[0]
        channel = contact[1]
        detail = contact[2]
        foundContact = session.query(data_contact).filter(data_contact.contactChannel == channel).filter(
            data_contact.contactDetails == detail).one_or_none()
        if foundContact is None:
            contactRecord = Contacts(source, company, name, channel, detail, lastUsedtime)
            contactID = contactRecord.add()
            contactIDs.append(contactID)
            # print(contact, "-->", contactID)
        else:
            contactIDs.append(foundContact.uuid)
            if foundContact.companyName.replace(" ", "").lower() != company.replace(" ", "").lower() and company > "":
                foundContact.lastCompanyName = foundContact.companyName
                foundContact.companyName = company
            if foundContact.name.replace(" ", "").lower() != name.replace(" ", "").lower() and name > "":
                foundContact.lastUsedName = foundContact.name
                foundContact.name = name
            if foundContact.lastUsedTime < lastUsedtime:
                foundContact.lastUsedTime = lastUsedtime
                if foundContact.usedSource != source:
                    foundContact.lastUsedSource = foundContact.usedSource
                    foundContact.usedSource = source
    return contactIDs

def readActiveDataSources(DataSources):
    """Returns a list of all active data sources from the DB
    """
    return session.query(DataSources).filter(DataSources.status == 1).order_by(DataSources.id).all()

# Connect to DB using Sqlalchemy
Base = automap_base()
engineName = 'mysql://' + dbCredentials['user'] + ':' + dbCredentials['password'] + '@' + \
             dbCredentials['host'] + '/' + dbCredentials['db'] + '?charset=utf8mb4'
engine = create_engine(engineName, echo=False)

# reflect the tables
Base.prepare(engine, reflect=True)
session = Session(engine)

# mapped classes are now created with names by default matching that of the table name.
# All table names and field names are case sensitive.

config_datasource = Base.classes.carguide_data_sources
# config_delivery_fb = Base.classes.config_delivery_fb
config_delivery_mobile = Base.classes.config_delivery_mobile

data_alert_event = Base.classes.carguide_car_alert_notification
data_alert_subscription = Base.classes.carguide_car_alert
data_bigTable = Base.classes.vehicle_data_external_source
data_contact = Base.classes.contact_person
data_user = Base.classes.carguide_user
data_raw_data_stats = Base.classes.vehicle_active_raw_data_stats
data_delivery_queue = Base.classes.data_delivery_queue


# Datasources
activeDSRecords = readActiveDataSources(config_datasource)
mappedDS = convertObjectArrayToDict(activeDSRecords, ["source"])

# config_delivery_mobile
deliveryMobilesCore = session.query(config_delivery_mobile).filter(config_delivery_mobile.type == 1).\
    filter(config_delivery_mobile.status == 1).all()
mappedDeliveryMobileCore = convertObjectArrayToDict(deliveryMobilesCore,["uuid"])

deliveryMobilesSat = session.query(config_delivery_mobile).filter(config_delivery_mobile.type == 2).\
    filter(config_delivery_mobile.status == 1).all()
mappedDeliveryMobileSat = convertObjectArrayToDict(deliveryMobilesSat,["uuid"])

# config_delivery_fb
# deliveryFBCore = session.query(config_delivery_fb).filter(config_delivery_fb.type == 1).\
#     filter(config_delivery_fb.status == 1).all()
# mappedDeliveryFBCore = convertObjectArrayToDict(deliveryFBCore, ["uuid"])
#
# deliveryFBSat = session.query(config_delivery_fb).filter(config_delivery_fb.type == 2).\
#     filter(config_delivery_fb.status == 1).all()
# mappedDeliveryFBSat = convertObjectArrayToDict(deliveryFBSat, ["uuid"])