from requests.auth import HTTPBasicAuth  # or HTTPDigestAuth, or OAuth1, etc.
from requests import Session
from zeep import Client
import json
from flask import Flask,request,jsonify
from zeep.transports import Transport
from zeep import Client
import zeep
import sys
import time
from zeep.wsse.username import UsernameToken

app = Flask(__name__)

import pyodbc
server = 'dbserveranasight.database.windows.net'
database = 'warehouse'
username = 'anasight@dbserveranasight'
password = 'Root@12345'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()


session = Session()
session.auth = HTTPBasicAuth('veten1', 'bshyWs')
client = Client('https://sthservices.ozsoft.com.au/ANS_API/VT_API.asmx?wsdl',
            transport=Transport(session=session))
# print(client.service.ValidateClient('veten1', 'bshyWs'))
# sys.exit()
# print(client.service.GetDivisions('\'\'7%.8"!2()(36\'0=;<4'))
# print(client.service.GetClientAVDetails("&-7%(9,!3((+;6'2?344",'VT00152'))
# sys.exit()
tokenContainer = []

# def get_token():
token = client.service.ValidateClient('veten1', 'bshyWs').Token
tokenContainer.clear()
tokenContainer.append(token.encode('ascii'))


#Get Locations
@app.route("/GetLocations",methods = ['POST', 'GET'])
def GetLocations():
    if request.method == 'GET':
        # get API response
        get_locations = client.service.GetLocations(f"""{tokenContainer[0].decode('ascii')}""")
        table_name = 'vettrak_get_locations'
        # Generate QUERY for each dictionary inside data list

        if(get_locations.LocaList.TLoca):
            for query in get_locations.LocaList.TLoca:   #Query is used to contain a location data at every iteration
                compareWith = "Loca_Code"
                compare_field_val = str(query.Loca_Code)
                sql_count_query = "SELECT COUNT(Loca_Code)from " + table_name + " where " + compareWith + " = '" + compare_field_val + "'"
                cursor.execute(sql_count_query)  # execute query
                record = cursor.fetchone() # Storing the value of count like 1,2,3...
                if(record[0] >0):
                    update_query_builder = "UPDATE " + table_name + " SET Loca_Code = '" + str(
                        query.Loca_Code) + "', Loca_Name = '" + str(query.Loca_Name) + "', Loca_Desc = '" + str(
                        query.Loca_Desc) + "', Stat_ShortName = '" + str(
                        query.Stat_ShortName) + "', Loca_PCode = '" + str(
                        query.Loca_PCode) + "', Loca_Suburb = '" + str(query.Loca_Suburb) + "', Load_Loading = '" + str(
                        query.Load_Loading) + "', Coun_Name = '" + str(query.Coun_Name) + "', Loca_Active = '" + str(
                        query.Loca_Active) + "', address_building = '" + str(
                        query.Address.Building) + "', address_UnitDetails = '" + str(
                        query.Address.UnitDetails) + "', address_StreetNumber = '" + str(query.Address.StreetNumber) + "', address_StreetName = '" + str(
                        query.Address.StreetName) + "', address_POBox = '" + str(query.Address.POBox) + "', address_City = '" + str(
                        query.Address.City) + "', address_State = '" + str(
                        query.Address.State) + "', address_Postcode = '" + str(
                        query.Address.Postcode) + "' WHERE " + compareWith + " = '" + compare_field_val + "'"
                    cursor.execute(update_query_builder)
                    cnxn.commit()
                    print('duplicate==>',record[0])

                else:
                    # SQL Insert query
                    queryBuilder = "INSERT INTO " + table_name + " (Loca_Code, Loca_Name, Loca_Desc, Stat_ShortName, Loca_PCode, Loca_Suburb," \
                                   "Load_Loading, Coun_Name, Loca_Active, address_building, address_UnitDetails, address_StreetNumber, " \
                                   "address_StreetName,address_POBox, address_City, address_State, address_Postcode) VALUES ('" + str(
                        query.Loca_Code) + "','" + str(query.Loca_Name) + "','" + str(query.Loca_Desc) + "','" + str(
                        query.Stat_ShortName) + "','" + str(query.Loca_PCode) + "','" + str(
                        query.Loca_Suburb) + "','" + str(query.Load_Loading) + "','" + str(query.Coun_Name) + "','" + str(
                        query.Loca_Active) + "','" + str(query.Address.Building) + "','" + str(
                        query.Address.UnitDetails) + "','" + str(query.Address.StreetNumber) + "','" + str(
                        query.Address.StreetName) + "','" + str(query.Address.POBox) + "','" + str(
                        query.Address.City) + "','" + str(query.Address.State) + "','" + str(query.Address.Postcode) + "')"

                    cursor.execute(queryBuilder)
                    cnxn.commit() # To establish Connection/Mapping with Database
            return jsonify({'type': 'Get Divisions', 'responseData': str(get_locations.LocaList.TLoca), 'status': 1})
        else:
          return jsonify({'type': 'Get Divisions', 'responseData':'No Record Found', 'status': 1})
    else:
        return jsonify({'type': 'request','message': 'Sorry! Method Not Allowed','status': '0'})

# Get Employment Category List
@app.route("/GetEmploymentCategoryList",methods = ['POST', 'GET'])
def GetEmploymentCategoryList():
    if request.method == 'GET':
        get_employment_category_list = client.service.GetEmploymentCategoryList()
        table_name = 'vettrak_get_employment_category_list'
        if(get_employment_category_list.EmcaList.TEmca):
            for query in get_employment_category_list.EmcaList.TEmca:
                compareWith = 'Emca_Code'  # Field name which you use in COUNT() and in WHERE condition
                compare_field_val = str(query.Emca_Code)  # This is the value of the field used in WHERE condition

                sql_count_query = "SELECT COUNT(" + compareWith + ")from " + table_name + " where " + compareWith + " = '" + compare_field_val + "'"
                cursor.execute(sql_count_query)  # execute query
                record = cursor.fetchone()
                if (record[0] > 0):
                    generate_update_query(table_name, query, cursor, compareWith, compare_field_val)

                else:
                    generate_insert_query(table_name, query, cursor)
            return jsonify({'type': 'Get Divisions', 'responseData': str(get_employment_category_list.EmcaList.TEmca), 'status': 1})
        else:
          return jsonify({'type': 'Get Divisions', 'responseData':'No Record Found', 'status': 1})
    else:
        return jsonify({'type': 'request','message': 'Sorry! Method Not Allowed','status': '0'})


# Search for Employer
@app.route("/SearchForEmployer", methods=['POST', 'GET'])
def SearchForEmployer():
    if request.method == 'GET':
        search_for_employer = client.service.SearchForEmployer(f"""{tokenContainer[0].decode('ascii')}""", '', 1)
        table_name = 'vettrak_search_for_employer'
        # Generate QUERY for each dictionary inside data list

        if (search_for_employer.EmplList.TEmpl):
            for query in search_for_employer.EmplList.TEmpl:  # Query is used to contain a location data at every iteration
                compare_field_name = 'Empl_Identifier'
                compare_field_val = str(query.Empl_Identifier)
                sql_count_query = "SELECT COUNT(" + compare_field_name + ")from " + table_name + " where " + compare_field_name + " = '" + compare_field_val + "'"
                cursor.execute(sql_count_query)  # execute query
                record = cursor.fetchone()  # Storing the value of count like 1,2,3...
                if (record[0] > 0):
                    update_query_builder = "UPDATE " + table_name + " SET Empl_Identifier = '" + str(
                        query.Empl_Identifier) + "', Emty_Code = '" + str(query.Emty_Code) + "', Anzs_Code = '" + str(
                        query.Anzs_Code) + "', Empl_Name = '" + str(
                        query.Empl_Name) + "', Empl_LegalName = '" + str(
                        query.Empl_LegalName) + "', Empl_EmployerSize = '" + str(
                        query.Empl_EmployerSize) + "', Empl_BAddr = '" + str(
                        query.Empl_BAddr) + "', Empl_BCity = '" + str(query.Empl_BCity) + "', Empl_BPCode = '" + str(
                        query.Empl_BPCode) + "', BusinessState = '" + str(
                        query.BusinessState) + "', Empl_PAddr = '" + str(
                        query.Empl_PAddr) + "', Empl_PCity = '" + str(query.Empl_PCity) + "', Empl_PPCode = '" + str(
                        query.Empl_PPCode) + "', PostalState = '" + str(query.PostalState) + "', Empl_Phone = '" + str(
                        query.Empl_Phone) + "', Empl_Fax = '" + str(
                        query.Empl_Fax) + "', Empl_Mobile = '" + str(
                        query.Empl_Mobile) + "', Empl_Email = '" + str(query.Empl_Email) + "', Empl_ACN = '" + str(
                        query.Empl_ACN) + "', Empl_ABN = '" + str(
                        query.Empl_ABN) + "', Empl_Active = '" + str(
                        query.Empl_Active) + "', Empl_Parent = '" + str(query.Empl_Parent) + "', ParentName = '" + str(
                        query.ParentName) + "', Empl_Type = '" + str(query.Empl_Type) + "', Empl_Code = '" + str(
                        query.Empl_Code) + "', URL = '" + str(
                        query.URL) + "', Business_Address_Building = '" + str(
                        query.BusinessAddress.Building) + "', Business_Address_UnitDetails = '" + str(
                        query.BusinessAddress.UnitDetails) + "', Business_Address_StreetNumber = '" + str(
                        query.BusinessAddress.StreetNumber) + "', Business_Address_StreetName = '" + str(
                        query.BusinessAddress.StreetName) + "', Business_Address_POBox = '" + str(
                        query.BusinessAddress.POBox) + "', Business_Address_City = '" + str(
                        query.BusinessAddress.City) + "', Business_Address_State = '" + str(
                        query.BusinessAddress.State) + "', Business_Address_Postcode = '" + str(
                        query.BusinessAddress.Postcode) + "', Postal_Adress_Building = '" + str(
                        query.PostalAddress.Building) + "', Postal_Adress_UnitDetails = '" + str(
                        query.PostalAddress.UnitDetails) + "', Postal_Adress_StreetNumber = '" + str(
                        query.PostalAddress.StreetNumber) + "', Postal_Adress_StreetName = '" + str(
                        query.PostalAddress.StreetName) + "', Postal_Adress_POBox = '" + str(
                        query.PostalAddress.POBox) + "', Postal_Adress_City = '" + str(
                        query.PostalAddress.City) + "', Postal_Adress_State = '" + str(
                        query.BusinessAddress.State) + "', Postal_Adress_Postcode = '" + str(
                        query.PostalAddress.Postcode) + "' WHERE " + compare_field_name + " = '" + compare_field_val + "'"
                    cursor.execute(update_query_builder)
                    cnxn.commit()

                else:
                    # SQL Insert query
                    queryBuilder = "INSERT INTO " + table_name + " (Empl_Identifier,Emty_Code,Anzs_Code,Empl_Name,Empl_LegalName,Empl_EmployerSize,Empl_BAddr,Empl_BCity,Empl_BPCode,BusinessState,Empl_PAddr,Empl_PCity,Empl_PPCode,PostalState,Empl_Phone,Empl_Fax,Empl_Mobile,Empl_Email,Empl_ACN,Empl_ABN,Empl_Active,Empl_Parent,ParentName,Empl_Type,Empl_Code,URL,Business_Address_Building,Business_Address_UnitDetails,Business_Address_StreetNumber,Business_Address_StreetName,Business_Address_POBox,Business_Address_City,Business_Address_State,Business_Address_Postcode,Postal_Adress_Building,Postal_Adress_UnitDetails,Postal_Adress_StreetNumber,Postal_Adress_StreetName,Postal_Adress_POBox,Postal_Adress_City,Postal_Adress_State,Postal_Adress_Postcode) VALUES ('" + str(
                        query.Empl_Identifier) + "','" + str(query.Emty_Code) + "','" + str(
                        query.Anzs_Code) + "','" + str(
                        query.Empl_Name) + "','" + str(query.Empl_LegalName) + "','" + str(
                        query.Empl_EmployerSize) + "','" + str(query.Empl_BAddr) + "','" + str(
                        query.Empl_BCity) + "','" + str(
                        query.Empl_BPCode) + "','" + str(query.BusinessState) + "','" + str(
                        query.Empl_PAddr) + "','" + str(query.Empl_PCity) + "','" + str(
                        query.Empl_PPCode) + "','" + str(query.PostalState) + "','" + str(
                        query.Empl_Phone) + "','" + str(query.Empl_Fax) + "','" + str(query.Empl_Mobile) + "', '" + str(
                        query.Empl_Email) + "','" + str(query.Empl_ACN) + "','" + str(
                        query.Empl_ABN) + "','" + str(
                        query.Empl_Active) + "','" + str(query.Empl_Parent) + "','" + str(
                        query.ParentName) + "','" + str(query.Empl_Type) + "','" + str(
                        query.Empl_Code) + "','" + str(
                        query.URL) + "','" + str(query.BusinessAddress.Building) + "','" + str(
                        query.BusinessAddress.UnitDetails) + "','" + str(
                        query.BusinessAddress.StreetNumber) + "','" + str(
                        query.BusinessAddress.StreetName) + "','" + str(query.BusinessAddress.POBox) + "','" + str(
                        query.BusinessAddress.City) + "','" + str(query.BusinessAddress.State) + "','" + str(
                        query.BusinessAddress.Postcode) + "', '" + str(
                        query.PostalAddress.Building) + "','" + str(
                        query.PostalAddress.UnitDetails) + "','" + str(
                        query.PostalAddress.StreetNumber) + "','" + str(
                        query.PostalAddress.StreetName) + "','" + str(query.PostalAddress.POBox) + "','" + str(
                        query.PostalAddress.City) + "','" + str(query.PostalAddress.State) + "','" + str(
                        query.PostalAddress.Postcode) + "')"

                    cursor.execute(queryBuilder)
                    cnxn.commit()  # To establish Connection/Mapping with Database
            return jsonify(
                {'type': 'Get Divisions', 'responseData': str(search_for_employer.EmplList.TEmpl), 'status': 1})
        else:
            return jsonify({'type': 'Get Divisions', 'responseData': 'No Record Found', 'status': 1})
    else:
        return jsonify({'type': 'request', 'message': 'Sorry! Method Not Allowed', 'status': '0'})


# Get Organisations
@app.route("/GetOrganisations",methods = ['POST', 'GET'])
def GetOrganisations():
    if request.method == 'GET':
        # get API response
        get_organisations = client.service.GetOrganisations(f"""{tokenContainer[0].decode('ascii')}""")
        table_name = 'vettrak_get_organisations'
        # Generate QUERY for each dictionary inside data list

        if(get_organisations.OrgaList.TOrga):
            for query in get_organisations.OrgaList.TOrga:   #Query is used to contain a location data at every iteration
                compareWith = 'OrganisationId'
                compare_field_val = str(query.OrganisationId)
                sql_count_query = "SELECT COUNT(OrganisationId)from " + table_name + " where " + compareWith + " = '" + compare_field_val + "'"
                cursor.execute(sql_count_query)  # execute query
                record = cursor.fetchone() # Storing the value of count like 1,2,3...
                if(record[0] >0):
                    generate_update_query(table_name, query, cursor, compareWith, compare_field_val)
                    #print('duplicate==>',record[0])

                else:
                    generate_insert_query(table_name, query, cursor)

            return jsonify({'type': 'Get Divisions', 'responseData': str(get_organisations.OrgaList.TOrga), 'status': 1})
        else:
          return jsonify({'type': 'Get Divisions', 'responseData':'No Record Found', 'status': 1})
    else:
        return jsonify({'type': 'request','message': 'Sorry! Method Not Allowed','status': '0'})


# Get Referral Sources
@app.route("/GetReferralSources", methods=['POST', 'GET'])
def GetReferralSources():
    if request.method == 'GET':
        get_referral_sources = client.service.GetReferralSources(f"""{tokenContainer[0].decode('ascii')}""")
        table_name = 'vettrak_get_referral_sources'
        # Generate QUERY for each dictionary inside data list

        if (get_referral_sources.ReferralTypeList.TReferral):
            for query in get_referral_sources.ReferralTypeList.TReferral:  # Query is used to contain a location data at every iteration
                compare_field_name = 'ReferralId'
                compare_field_val = str(query.ReferralId)
                sql_count_query = "SELECT COUNT(" + compare_field_name + ")from " + table_name + " where " + compare_field_name + " = '" + compare_field_val + "'"
                cursor.execute(sql_count_query)  # execute query
                record = cursor.fetchone()  # Storing the value of count like 1,2,3...
                if (record[0] > 0):
                    generate_update_query(table_name, query, cursor, compare_field_name,
                                          compare_field_val)  # Calling Update query function

                else:
                    # SQL Insert query
                    generate_insert_query(table_name, query, cursor)  # Calling insert query function

            return jsonify(
                {'type': 'Get Divisions', 'responseData': str(get_referral_sources.ReferralTypeList.TReferral),
                 'status': 1})
        else:
            return jsonify({'type': 'Get Divisions', 'responseData': 'No Record Found', 'status': 1})
    else:
        return jsonify({'type': 'request', 'message': 'Sorry! Method Not Allowed', 'status': '0'})

# Get Enrollments for client
@app.route("/GetEnrolmentsForClient",methods = ['POST', 'GET'])
def GetEnrolmentsForClient():
    if request.method == 'GET':
        # get API response
        search_for_employer = client.service.GetEnrolmentsForClient(f"""{tokenContainer[0].decode('ascii')}""", 'VT00152')
        table_name = 'vettrak_get_enrolments_for_client'

        # Generate QUERY for each dictionary inside data list

        if(search_for_employer.ClieEnroList.TClieEnro):
            for query in search_for_employer.ClieEnroList.TClieEnro:   #Query is used to contain a location data at every iteration
                compare_field_name = 'Enrol_ID'
                compare_field_val = str(query.ID)

                # Query to count Loca_Code from table
                sql_count_query = "SELECT COUNT(" + compare_field_name + ")from " + table_name + " where " + compare_field_name + " = '" + compare_field_val + "'"
                cursor.execute(sql_count_query)  # execute query
                record = cursor.fetchone() # Storing the value of count like 1,2,3...
                if(record[0] >0):

                    input_dict = zeep.helpers.serialize_object(query.DeliveryModes)
                    update_query_builder = "UPDATE " + table_name + " SET Enrol_ID = '" + str(
                        query.ID) + "', Code = '" + str(query.Code) + "', StartDate = '" + str(
                        query.StartDate) + "', EndDate = '" + str(
                        query.EndDate) + "', Status = '" + str(
                        query.Status) + "', StatusType = '" + str(
                        query.StatusType) + "', DateOfEffect = '" + str(
                        query.DateOfEffect) + "', Qual_Code = '" + str(query.Qual_Code) + "', Qual_Name = '" + str(
                        query.Qual_Name) + "', OrganisationId = '" + str(
                        query.OrganisationId) + "', Description = '" + str(
                        query.Description) + "', Loca_Code = '" + str(query.Loca_Code) + "', DeliveryType = '" + str(
                        query.DeliveryType) + "', DeliveryModes = '" + (json.dumps(input_dict)) + "', EmployerIdentifier = '" + str(
                        query.EmployerIdentifier) + "', EmployerName = '" + str(
                        query.EmployerName) + "', EmployerContactCode = '" + str(
                        query.EmployerContactCode) + "', Amount = '" + str(query.Amount) + "', GST = '" + str(
                        query.GST) + "', AmountPaid = '" + str(
                        query.AmountPaid) + "', AmountCredited = '" + str(
                        query.AmountCredited) + "', DivisionId = '" + str(query.DivisionId) + "', EnrolmentType = '" + str(
                        query.EnrolmentType) + "', EnrolmentTypeCode = '" + str(query.EnrolmentTypeCode) + "', Clie_Code = '" + str(
                        query.Clie_Code) + "', GivenName = '" + str(
                        query.GivenName) + "', Surname = '" + str(
                        query.Surname) + "' WHERE " + compare_field_name + " = '" + compare_field_val + "'"
                    print("update_query_builder=>", update_query_builder)
                    cursor.execute(update_query_builder)
                    cnxn.commit()

                else:
                    # SQL Insert query
                    input_dict = zeep.helpers.serialize_object(query.DeliveryModes)
                    queryBuilder = "INSERT INTO " + table_name + " (Enrol_ID,Code, StartDate, EndDate," \
                                                                 " Status, StatusType, DateOfEffect, Qual_Code, Qual_Name, " \
                                                                 "OrganisationId, Description, Loca_Code, DeliveryType, DeliveryModes, " \
                                                                 "EmployerIdentifier, EmployerName, EmployerContactCode, Amount, " \
                                                                 "GST, AmountPaid, AmountCredited, DivisionId, EnrolmentType, " \
                                                                 "EnrolmentTypeCode, Clie_Code, GivenName, Surname) VALUES ('" + str(
                        query.ID) + "','" + str(query.Code) + "','" + str(query.StartDate) + "','" + str(
                        query.EndDate) + "','" + str(query.Status) + "','" + str(
                        query.StatusType) + "','" + str(query.DateOfEffect) + "','" + str(query.Qual_Code) + "','" + str(
                        query.Qual_Name) + "','" + str(query.OrganisationId) + "','" + str(
                        query.Description) + "','" + str(query.Loca_Code) + "','" + str(
                        query.DeliveryType) + "','" + (json.dumps(input_dict)) + "','" + str(
                        query.EmployerIdentifier) + "','" + str(query.EmployerName) + "','" + str(query.EmployerContactCode) + "', '" +str(
                        query.Amount) + "','" + str(query.GST) + "','" + str(
                        query.AmountPaid) + "','" + str(
                        query.AmountCredited) + "','" + str(query.DivisionId) + "','" + str(
                        query.EnrolmentType) + "','" + str(query.EnrolmentTypeCode) + "','" + str(
                        query.Clie_Code) + "','" + str(
                        query.GivenName) + "','" + str(query.Surname) + "')"
                    print("insertqueryBuilder=>", queryBuilder)
                    cursor.execute(queryBuilder)
                    cnxn.commit() # To establish Connection/Mapping with Database
            return jsonify({'type': 'Get Divisions', 'responseData': str(search_for_employer.ClieEnroList.TClieEnro), 'status': 1})
        else:
          return jsonify({'type': 'Get Divisions', 'responseData':'No Record Found', 'status': 1})
    else:
        return jsonify({'type': 'request','message': 'Sorry! Method Not Allowed','status': '0'})


# Get Contracts Or Enrolments For Client
@app.route("/GetContractsOrEnrolmentsForClient", methods=['POST', 'GET'])
def GetContractsOrEnrolmentsForClient():
    if request.method == 'GET':
        # get API response
        search_for_employer = client.service.GetContractsOrEnrolmentsForClient(f"""{tokenContainer[0].decode('ascii')}""", 'VT00152')
        table_name = 'vettrak_get_contracts_or_enrolments_for_client'

        # Generate QUERY for each dictionary inside data list

        if (search_for_employer.ContEnroList.TContEnro):
            for query in search_for_employer.ContEnroList.TContEnro:  # Query is used to contain a location data at every iteration
                compare_field_name = 'Enrol_ID'
                compare_field_val = str(query.ID)
                sql_count_query = "SELECT COUNT(" + compare_field_name + ")from " + table_name + " where " + compare_field_name + " = '" + compare_field_val + "'"
                cursor.execute(sql_count_query)  # execute query
                record = cursor.fetchone()  # Storing the value of count like 1,2,3...
                if (record[0] > 0):
                    update_query_builder = "UPDATE " + table_name + " SET Enrol_ID = '" + str(
                        query.ID) + "', Code = '" + str(query.Code) + "', StartDate = '" + str(
                        query.StartDate) + "', EndDate = '" + str(
                        query.EndDate) + "', Status = '" + str(
                        query.Status) + "', DateOfEffect = '" + str(
                        query.DateOfEffect) + "', Qual_Code = '" + str(query.Qual_Code) + "', Qual_Name = '" + str(
                        query.Qual_Name) + "', Empl_Identifier = '" + str(
                        query.Empl_Identifier) + "', Empl_Name = '" + str(
                        query.Empl_Name) + "', ContractType = '" + str(
                        query.ContractType) + "', OrganisationId = '" + str(
                        query.OrganisationId) + "', Description = '" + str(
                        query.Description) + "', StateShortName = '" + str(
                        query.StateShortName) + "', EmployerContactCode = '" + str(
                        query.EmployerContactCode) + "', Rec_Type = '" + str(
                        query.Rec_Type) + "', Loca_Code = '" + str(
                        query.Loca_Code) + "' WHERE " + compare_field_name + " = '" + compare_field_val + "'"
                    cursor.execute(update_query_builder)
                    cnxn.commit()

                else:
                    # SQL Insert query
                    queryBuilder = "INSERT INTO " + table_name + " (Enrol_ID," \
                                                                 "Code,StartDate,EndDate,Status,DateOfEffect,Qual_Code,Qual_Name,Empl_Identifier," \
                                                                 "Empl_Name,ContractType,ContractTypeCode,OrganisationId,Description,StateShortName," \
                                                                 "EmployerContactCode,Rec_Type,Loca_Code) VALUES ('" + str(
                        query.ID) + "','" + str(query.Code) + "','" + str(query.StartDate) + "','" + str(
                        query.EndDate) + "','" + str(query.Status) + "','" + str(query.DateOfEffect) + "','" + str(
                        query.Qual_Code) + "','" + str(
                        query.Qual_Name) + "','" + str(query.Empl_Identifier) + "','" + str(
                        query.Empl_Name) + "','" + str(query.ContractType) + "','" + str(
                        query.ContractTypeCode) + "','" + str(
                        query.OrganisationId) + "','" + str(query.Description) + "','" + str(
                        query.StateShortName) + "', '" + str(
                        query.EmployerContactCode) + "','" + str(query.Rec_Type) + "','" + str(
                        query.Loca_Code) + "')"
                    cursor.execute(queryBuilder)
                    cnxn.commit()  # To establish Connection/Mapping with Database
            return jsonify(
                {'type': 'Get Divisions', 'responseData': str(search_for_employer.ContEnroList.TContEnro), 'status': 1})
        else:
            return jsonify({'type': 'Get Divisions', 'responseData': 'No Record Found', 'status': 1})
    else:
        return jsonify({'type': 'request', 'message': 'Sorry! Method Not Allowed', 'status': '0'})


# Get Client AV Details
@app.route("/GetClientAVDetails", methods=['POST', 'GET'])
def GetClientAVDetails():
    if request.method == 'GET':
        # get API response
        search_for_employer = client.service.GetClientAVDetails(f"""{tokenContainer[0].decode('ascii')}""", 'VT00152')
        table_name = 'vettrak_get_client_AVDetails'
        query = search_for_employer.ClieAVETMISS
        # print("Clie code ===> ",query.Clie_Code)
        if (search_for_employer.ClieAVETMISS):
            compare_field_name = 'Clie_Code'
            compare_field_val = str(query.Clie_Code)
            sql_count_query = "SELECT COUNT(" + compare_field_name + ")from " + table_name + " where " + compare_field_name + " = '" + compare_field_val + "'"

            cursor.execute(sql_count_query)  # execute query
            record = cursor.fetchone()  # Storing the value of count like 1,2,3...
            if (record[0] > 0):
                # SQL Update Query
                generate_update_query(table_name, query, cursor, compare_field_name,
                                      compare_field_val)  # Calling Update query function

            else:
                # SQL Insert Query
                generate_insert_query(table_name, query, cursor)  # Calling insert query function

            return jsonify(
                {'type': 'Get Divisions', 'responseData': str(search_for_employer.ClieAVETMISS), 'status': 1})
        else:
            return jsonify({'type': 'Get Divisions', 'responseData': 'No Record Found', 'status': 1})
    else:
        return jsonify({'type': 'request', 'message': 'Sorry! Method Not Allowed', 'status': '0'})

# Get Client Details
@app.route("/GetClientDetails",methods = ['POST', 'GET'])
def GetClientDetails():
    if request.method == 'GET':
        # get API response
        search_for_employer = client.service.GetClientDetails(f"""{tokenContainer[0].decode('ascii')}""", 'VT00152')
        table_name = 'vettrak_get_client_details'
        # Generate QUERY for each dictionary inside data list
        query = search_for_employer.ClieDetail
        if(search_for_employer.ClieDetail):
            compare_field_name = 'Clie_Code'
            compare_field_val = str(query.Clie_Code)
            sql_count_query = "SELECT COUNT(" + compare_field_name + ")from " + table_name + " where " + compare_field_name + " = '" + compare_field_val + "'"

            cursor.execute(sql_count_query)  # execute query
            record = cursor.fetchone() # Storing the value of count like 1,2,3...
            if(record[0] >0):
                update_query_builder = "UPDATE " + table_name + " SET Clie_Surname = '" + str(
                        query.Clie_Surname) + "', Clie_Given = '" + str(query.Clie_Given) + "', Clie_Code = '" + str(
                        query.Clie_Code) + "', Empl_Identifier = '" + str(
                        query.Empl_Identifier) + "', Clie_Other = '" + str(
                        query.Clie_Other) + "', Clie_Username = '" + str(query.Clie_Username) + "', Clie_DOB = '" + str(
                        query.Clie_DOB) + "', Clie_RAddr = '" + str(query.Clie_RAddr) + "', Clie_RCity = '" + str(
                        query.Clie_RCity) + "', Clie_RPCode = '" + str(
                        query.Clie_RPCode) + "', Stat_RShortName = '" + str(
                        query.Stat_RShortName) + "', Clie_PAddr = '" + str(
                        query.Clie_PAddr) + "', Clie_PCity = '" + str(
                        query.Clie_PCity) + "', Clie_PPCode = '" + str(
                        query.Clie_PPCode) + "', Stat_PShortName = '" + str(
                        query.Stat_PShortName) + "', Clie_Address_Type = '" + str(
                        query.Clie_Address_Type) + "', Clie_Email = '" + str(
                        query.Clie_Email) + "', Clie_FaxPhone = '" + str(query.Clie_FaxPhone) + "', Clie_HomePhone = '" + str(
                        query.Clie_HomePhone) + "', Clie_WorkPhone = '" + str(
                        query.Clie_WorkPhone) + "', Clie_MobilePhone = '" + str(
                        query.Clie_MobilePhone) + "', Clie_EmergencyName = '" + str(query.Clie_EmergencyName) + "', Clie_EmergencyPhone = '" + str(
                        query.Clie_EmergencyPhone) + "', Clie_Title = '" + str(query.Clie_Title) + "', Gender = '" + str(
                        query.Gender) + "', PositionCode = '" + str(
                        query.PositionCode) + "', Clie_Passport = '" + str(
                        query.Clie_Passport) + "', Clie_Visa = '" + str(
                        query.Clie_Visa) + "', Vity_Name = '" + str(
                        query.Vity_Name) + "', Clie_VisaExpiry = '" + str(
                        query.Clie_VisaExpiry) + "', Clie_DateLastAssessed = '" + str(
                        query.Clie_DateLastAssessed) + "', Clie_Commenced = '" + str(
                        query.Clie_Commenced) + "', TerminationDate = '" + str(
                        query.TerminationDate) + "', Clie_Salary = '" + str(
                        query.Clie_Salary) + "', DivisionId = '" + str(
                        query.DivisionId) + "', UsualAddress = '" + str(
                        query.UsualAddress) + "', PostalAddress = '" + str(
                        query.PostalAddress) + "', USI = '" + str(
                        query.USI) + "', USIExempt = '" + str(
                        query.USIExempt) + "' WHERE " + compare_field_name + " = '" + compare_field_val + "'"
                cursor.execute(update_query_builder)
                cnxn.commit()

            else:
                # SQL Insert query
                # generate_insert_query(table_name, query, cursor)  # Calling insert query function
                queryBuilder = "INSERT INTO vettrak_get_client_details (Clie_Surname, Clie_Given, Clie_Code, Empl_Identifier, " \
                               "Clie_Other, Clie_Username, Clie_DOB, Clie_RAddr, Clie_RCity, Clie_RPCode, Stat_RShortName, Clie_PAddr, " \
                               "Clie_PCity, Clie_PPCode, Stat_PShortName, Clie_Address_Type, Clie_Email, Clie_FaxPhone, Clie_HomePhone, " \
                               "Clie_WorkPhone, Clie_MobilePhone, Clie_EmergencyName, Clie_EmergencyPhone, Clie_Title, Gender, PositionCode, " \
                               "Clie_Passport, Clie_Visa, Vity_Name, Clie_VisaExpiry, Clie_DateLastAssessed, Clie_Commenced, TerminationDate, " \
                               "Clie_Salary, DivisionId, UsualAddress, PostalAddress, USI, USIExempt) VALUES ('" + str(
                query.Clie_Surname) + "','" + str(query.Clie_Given) + "','" + str(query.Clie_Code) + "','" + str(
                query.Empl_Identifier) + "','" + str(query.Clie_Other) + "','" + str(query.Clie_Username) + "','" + str(query.Clie_DOB) + "','" + str(
                query.Clie_RAddr) + "','" + str(query.Clie_RCity) + "','" + str(
                query.Clie_RPCode) + "','" + str(query.Stat_RShortName) + "','" + str(
                query.Clie_PAddr) + "','" + str(
                query.Clie_PCity) + "','" + str(query.Clie_PPCode) + "','" + str(query.Stat_PShortName) + "', '" +str(
                query.Clie_Address_Type) + "','" + str(query.Clie_Email) + "','" + str(query.Clie_FaxPhone) + "','" + str(
                query.Clie_HomePhone) + "','" + str(query.Clie_WorkPhone) + "','" + str(query.Clie_MobilePhone) + "','" + str(query.Clie_EmergencyName) + "','" + str(
                query.Clie_EmergencyPhone) + "','" + str(query.Clie_Title) + "','" + str(
                query.Gender) + "','" + str(query.PositionCode) + "','" + str(
                query.Clie_Passport) + "','" + str(
                query.Clie_Visa) + "','" + str(query.Vity_Name) + "','" + str(query.Clie_VisaExpiry) + "', '" +str(
                query.Clie_DateLastAssessed) + "','" + str(query.Clie_Commenced) + "','" + str(
                query.TerminationDate) + "','" + str(query.Clie_Salary) + "','" + str(
                query.DivisionId) + "','" + str(
                query.UsualAddress) + "','" + str(query.PostalAddress) + "','" + str(query.USI) + "', '" +str(
                query.USIExempt) + "')"
                cursor.execute(queryBuilder)
                cnxn.commit() # To establish Connection/Mapping with Database
            return jsonify({'type': 'Get Divisions', 'responseData': str(search_for_employer.ClieDetail), 'status': 1})
        else:
          return jsonify({'type': 'Get Divisions', 'responseData':'No Record Found', 'status': 1})
    else:
        return jsonify({'type': 'request','message': 'Sorry! Method Not Allowed','status': '0'})



def generate_insert_query(table, dictionary, cursor):

    retry_flag = True
    retry_count = 0
    while retry_flag and retry_count < 5:
        try:
            input_dict = zeep.helpers.serialize_object(dictionary)
            output_dict = json.loads(json.dumps(input_dict))

            # Get all "keys" inside "values" key of dictionary (column names)
            columns = ', '.join([key for key, value in output_dict.items()])
            values = ', '.join(
                ["'" + str(value) + "'" if value is None or type(value) is not str else "'" + value.replace("'","''") + "'"
                 for key, value in output_dict.items()])  # if the type of value is None or other than string then if else is used in List Comprehension
            insert_query_builder = "INSERT INTO " + table + " (" + columns + ") VALUES (" + values + ")"
            cursor.execute(insert_query_builder)
            cnxn.commit()
            retry_flag = False
        except Exception as inst:
            print('Insert Query Exception==>>>', inst)
            print("Retry after 1 sec")
            retry_count = retry_count + 1
            time.sleep(1)

def generate_update_query(table, dictionary, cursor, compareColumn, columnValue):

    retry_flag = True
    retry_count = 0
    while retry_flag and retry_count < 5:
        try:
            input_dict = zeep.helpers.serialize_object(dictionary)
            output_dict = json.loads(json.dumps(input_dict))
            values = ','.join(
                [f"{key} = '{value}'" if "'" not in str(value) else f"""{key} = '{value.replace("'", "''")}'""" for
                 key, value in output_dict.items()])

            update_query_builder = "UPDATE " + table + " SET " + values + " WHERE " + compareColumn + " = '" + columnValue + "'"
            cursor.execute(update_query_builder)
            cnxn.commit()
            retry_flag = False
        except Exception as inst:
            print('Update Query Exception==>>>',inst)
            print("Retry after 1 sec")
            retry_count = retry_count + 1
            time.sleep(1)

app.run(host='10.1.1.86',port='8081',debug=False)
