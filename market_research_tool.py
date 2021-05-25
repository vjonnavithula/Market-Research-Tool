# -*- coding: utf-8 -*-
"""
The purpose of this script is to pull data from FPDS.
Please change From email Address and SMTP Server details

@author: Venu Jonnavithula
"""

#%% Imports
import pandas as pd
import pyodbc
import datetime
import calendar

from collections import OrderedDict
import xmltodict
import requests
import json
import warnings

# Mail import
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
## File to send and its path
filename = 'data.xlsx'
SourcePathName='/u/homes/venjonna/python/'+filename
#%% Creation of Contracts class wrapper for FPDS API
warnings.filterwarnings('ignore')

field_map = {

    'piid': 'PIID',
    'idv_piid': 'REF_IDV_PIID',
    'idv_agency_id': 'REF_IDV_AGENCY_ID',
    'modification_number': 'MODIFICATION_NUMBER',

    'contracting_agency_id': 'CONTRACTING_AGENCY_ID',
    'contracting_agency_name': 'CONTRACTING_AGENCY_NAME',
    'contracting_office_id': 'CONTRACTING_OFFICE_ID',
    'contracting_office_name': 'CONTRACTING_OFFICE_NAME',
    'funding_agency_id': 'FUNDING_AGENCY_ID',
    'funding_office_id': 'FUNDING_OFFICE_ID',
    'funding_office_name': 'FUNDING_OFFICE_NAME',
    'agency_code': 'AGENCY_CODE',
    'agency_name': 'AGENCY_NAME',
    'department_id': 'DEPARTMENT_ID',
    'department_name': 'DEPARTMENT_NAME',

    'last_modified_date': 'LAST_MOD_DATE',
    'last_modified_by': 'LAST_MODIFIED_BY',
    'award_completion_date': 'AWARD_COMPLETION_DATE',
    'created_on': 'CREATED_DATE',
    'date_signed': 'SIGNED_DATE',
    'effective_date': 'EFFECTIVE_DATE',
    'estimated_completion_date': 'ESTIMATED_COMPLETION_DATE',

    'obligated_amount': 'OBLIGATED_AMOUNT',
    'ultimate_contract_value': 'ULTIMATE_CONTRACT_VALUE',
 'contract_pricing_type': 'TYPE_OF_CONTRACT_PRICING',

    'award_status': 'AWARD_STATUS',
    'contract_type': 'CONTRACT_TYPE',
    'created_by': 'CREATED_BY',
    'description': 'DESCRIPTION_OF_REQUIREMENT',
    'modification_reason': 'REASON_FOR_MODIFICATION',
    'legislative_mandates': 'LEGISLATIVE_MANDATES',
    'local_area_setaside': 'LOCAL_AREA_SET_ASIDE',
    'socioeconomic_indicators': 'SOCIO_ECONOMIC_INDICATORS',
    'multiyear_contract': 'MULTIYEAR_CONTRACT',
    'national_interest_code': 'NATIONAL_INTEREST_CODE',
    'national_interest_description': 'NATIONAL_INTEREST_DESCRIPTION',

    'naics_code': 'PRINCIPAL_NAICS_CODE',
    'naics_description': 'NAICS_DESCRIPTION',
    'product_or_service_code': 'PRODUCT_OR_SERVICE_CODE',
    'product_or_service_description': 'PRODUCT_OR_SERVICE_DESCRIPTION',

    'place_of_performance_district': 'POP_CONGRESS_DISTRICT_CODE',
    'place_of_performance_country': 'POP_CONGRESS_COUNTRY',
    'place_of_performance_state': 'POP_STATE_NAME',

    'vendor_city': 'VENDOR_ADDRESS_CITY',
    'vendor_district': 'VENDOR_CONGRESS_DISTRICT_CODE',
    'vendor_country_code': 'VENDOR_ADDRESS_COUNTRY_CODE',
    'vendor_country_name': 'VENDOR_ADDRESS_COUNTRY_NAME',
    'vendor_duns': 'VENDOR_DUNS_NUMBER',
    'vendor_dba_name': 'VENDOR_DOING_BUSINESS_AS_NAME',
    'vendor_name': 'VENDOR_NAME',
    'vendor_state_code': 'VENDOR_ADDRESS_STATE_CODE',
    'vendor_state_name': 'VENDOR_ADDRESS_STATE_NAME',
    'vendor_zip': 'VENDOR_ADDRESS_ZIP_CODE',

}

boolean_map = {
    True: 'Y',
    False: 'N',
}


class Contracts():

    feed_url = "https://www.fpds.gov/ezsearch/FEEDS/ATOM?FEEDNAME=PUBLIC&q="
    feed_size = 10
    query_url = ''

    def __init__(self, logger=None):
        #point logger to a log function, print by default
        if logger:
            self.log = logger
        else:
            self.log = print

    def pretty_print(self, data):
        self.log(json.dumps(data, indent=4))
 def convert_params(self, params):

        new_params = {}
        for k,v in params.items():
            new_params[field_map[k]] = v
        return new_params

    def combine_params(self, params):
        return " ".join("%s:%s" % (k,v) for k,v in params.items())

    def process_data(self, data):
        #todo
        if isinstance(data, dict):
            #make a list so it's consistent
            data = [data,]
        return data

    def get(self, num_records='all', order='desc', **kwargs):

        params = self.combine_params(self.convert_params(kwargs))

        data = []
        i = 0
        #for n in range(0, num_records, 10):
        while num_records == "all" or i < num_records:

            self.log("querying {0}{1}&start={2}".format(self.feed_url, params, i))
            resp = requests.get(self.feed_url + params + '&start={0}'.format(i), timeout=60, verify = False)
            self.query_url = resp.url
            self.log("finished querying {0}".format(resp.url))
            resp_data = xmltodict.parse(resp.text, process_namespaces=True, namespaces={'http://www.fpdsng.com/FPDS': None, 'http://www.w3.org/2005/Atom': None})
            try:
                processed_data = self.process_data(resp_data['feed']['entry'])
                for pd in processed_data:
                    data.append(pd)
                    i += 1

                #if data contains less than 10 records, break out of loop
                if  len(processed_data) < 10:
                    break

            except KeyError as e:
                #no results
                self.log("No results for query")
                break

        return data


#%% Get date range for compare

date  = datetime.datetime.now()
today = datetime.date.today()
#first_day = date.today().replace(day=1)
#first_day = first_day.strftime('%Y/%m/%d')
start_date=input('Enter Start Date YYYY-MM-DD : ')
year,month,day=map(int, start_date.split('-'))
first_day=datetime.date(year,month,day)
first_day = first_day.strftime('%Y/%m/%d')

last_day = date.replace(day = calendar.monthrange(date.year, date.month)[1])
last_day = last_day.strftime('%Y/%m/%d')
naic_code=input('Enter NAIC Code: ')
to_email=input('Enter Email Address: ')

#%% Import data from FPDS using Contracts class. Limit to date range (current month)
c = Contracts()

query_range = '['+first_day+','+last_day+']'
records = c.get(naics_code=naic_code,last_modified_date = query_range)

piid_list = []
mod_list = []
pop_end_list = []
idv_piid_val_list = []
oblg_amt_list = []
last_mod_date_list = []
contact_officer_list =[]
global_dun_list=[]
vendor_name_list=[]
contract_office_size_list=[]
count = len(records)

for i in range(0,count):
    try:
        piid_val = records[i]['content']['https://www.fpds.gov/FPDS:award']['https://www.fpds.gov/FPDS:awardID']['https://www.fpds.gov/FPDS:awardContractID']['https://www.fpds.gov/FPDS:PIID']
    except KeyError:
        piid_val = ''
    try:
        mod_num = records[i]['content']['https://www.fpds.gov/FPDS:award']['https://www.fpds.gov/FPDS:awardID']['https://www.fpds.gov/FPDS:awardContractID']['https://www.fpds.gov/FPDS:modNumber']
    except KeyError:
        mod_num = ''
    try:
        idv_piid_val = records[i]['content']['https://www.fpds.gov/FPDS:award']['https://www.fpds.gov/FPDS:awardID']['https://www.fpds.gov/FPDS:referencedIDVID']['https://www.fpds.gov/FPDS:PIID']
    except KeyError:
        idv_piid_val = ''
    try:
        pop_end = records[i]['content']['https://www.fpds.gov/FPDS:award']['https://www.fpds.gov/FPDS:relevantContractDates']['https://www.fpds.gov/FPDS:ultimateCompletionDate']
    except KeyError:
        pop_end = ''
    try:
        oblg_amt = records[i]['content']['https://www.fpds.gov/FPDS:award']['https://www.fpds.gov/FPDS:dollarValues']['https://www.fpds.gov/FPDS:obligatedAmount']
    except KeyError:
        oblg_amt = ''
    try:
        last_modified_date = records[i]['modified']
    except KeyError:
        last_modified_date = ''
    try:
        contact_officer_val = records[i]['content']['https://www.fpds.gov/FPDS:award']['https://www.fpds.gov/FPDS:purchaserInformation']['https://www.fpds.gov/FPDS:contractingOfficeAgencyID']
        contract_officer_dict=dict(contact_officer_val)
        contact_officer_val=contract_officer_dict.get('@name')
    except KeyError:
 try:
        vendor_name_val = records[i]['content']['https://www.fpds.gov/FPDS:award']['https://www.fpds.gov/FPDS:vendor']['https://www.fpds.gov/FPDS:vendorHeader']['https://www.fpds.gov/FPDS:vendorName']
    except KeyError:
        vendor_name_val = ''
    try:
        contract_office_size_val = records[i]['content']['https://www.fpds.gov/FPDS:award']['https://www.fpds.gov/FPDS:vendor']['https://www.fpds.gov/FPDS:contractingOfficerBusinessSizeDetermination']
        contract_office_size_dict = dict(contract_office_size_val)
        contract_office_size_val=contract_office_size_dict.get('@description')
    except KeyError:
        contract_office_size_val = ''
    try:
        global_dun_val = records[i]['content']['https://www.fpds.gov/FPDS:award']['https://www.fpds.gov/FPDS:vendor']['https://www.fpds.gov/FPDS:vendorSiteDetails']['https://www.fpds.gov/FPDS:vendorDUNSInformation']['https://www.fpds.gov/FPDS:globalParentDUNSNumber']
    except KeyError:
        global_dun_val = ''

    piid_list.append(piid_val)
    mod_list.append(mod_num)
    pop_end_list.append(pop_end)
    idv_piid_val_list.append(idv_piid_val)
    oblg_amt_list.append(oblg_amt)
    last_mod_date_list.append(last_modified_date)
    contact_officer_list.append(contact_officer_val)
    global_dun_list.append(global_dun_val)
    vendor_name_list.append(vendor_name_val)
    contract_office_size_list.append(contract_office_size_val)

df = pd.DataFrame(list(zip(piid_list,mod_list,pop_end_list,idv_piid_val_list,oblg_amt_list,last_mod_date_list,contact_officer_list,global_dun_list,vendor_name_list,contract_office_size_list)),
              columns=['piid','mod_num','pop_end','idv_piid','oblg_amt','last_mod_date','contact_officer','global_duns','vendor_name','contract_office_size'])

#Exclude NULL PIIDs or cases where the action was $0
df = df.loc[df['piid'] != '']
df = df.loc[df['oblg_amt'] > '0.00']


#%%Break out Partial PIID

#df['oblg_id'] = df['piid'].str.split(r'(NSFDACS)|(NSFDAS)|(NSFAST)|(491004)|(491006)|(NSF)',
 # expand=True)[7]

df['oblg_amt'] = df['oblg_amt'].astype('float')
df.to_excel('data.xlsx')
#Send email
msg = MIMEMultipart()
msg['From'] = <From Email Address>
msg['To'] =  to_email
msg['Subject'] = 'Report Data'
body = 'Attachment is the report for NAIC code '+naic_code+' for the Date Range from '+first_day + ' to '+last_day
msg.attach(MIMEText(body, 'plain'))
## ATTACHMENT PART OF THE CODE IS HERE
attachment = open(SourcePathName, 'rb')
part = MIMEBase('application', "octet-stream")
part.set_payload((attachment).read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
msg.attach(part)
server = smtplib.SMTP(<mailServer>, 25)  ### put your relevant SMTP here
server.ehlo()
#server.starttls()
server.ehlo()
#server.login('from@domain.com', 'password_here')  ### if applicable
server.send_message(msg)
server.quit()