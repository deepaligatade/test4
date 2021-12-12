import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow,Flow
from google.auth.transport.requests import Request
import os
import pickle
import streamlit as st
import datetime
## Required for writing to goolge sheet
from pprint import pprint
from googleapiclient import discovery


## Google Sheet Credentials
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1A2spx394hFZEhArmSc0rxfgYddoQjs97QW-EiCF0U_4'

## Function to open a sheet and load the data in dataframe
def main(SAMPLE_SPREADSHEET_ID_input,SAMPLE_RANGE_NAME):
    #values_input, service
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'google.json', SCOPES) # here enter the name of your downloaded JSON file
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result_input = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID_input,
                                range=SAMPLE_RANGE_NAME).execute()
    values_input = result_input.get('values', [])
   
    if not values_input :
        print('No data found.')
        return -1
    return values_input
#############################################

## Faculty sheet Load
SHEET_NAME ='Faculty'
SAMPLE_RANGE = 'Faculty'
values_input = main(SPREADSHEET_ID,SAMPLE_RANGE)
df_Faculty=pd.DataFrame(values_input[1:], columns=values_input[0])
# st.write(df_Faculty)
#############################################

## Schedule sheet Load

values_input = main(SPREADSHEET_ID,'Schedule')
df_Schedule=pd.DataFrame(values_input[1:], columns=values_input[0])
#st.write(df_Schedule)

## Converting "Start Date" and "end Date" columns to DateTime datatype 

df_Schedule['Start Date'] = pd.to_datetime(df_Schedule['Start Date'])
df_Schedule['End Date'] = pd.to_datetime(df_Schedule['End Date'])
#st.write(df_Schedule)
##############################################

## Calendar sheet Load

values_input = main(SPREADSHEET_ID,'Calendar')
df_Calendar=pd.DataFrame(values_input[1:], columns=values_input[0])

## Converting Date column to DateTime datatype 

df_Calendar.Date = pd.to_datetime(df_Calendar.Date)
#st.write(df_Calendar)
#####################################################################

## Function to convert time to numeric type

def time_convert(times):
    cnt=0
    comp = times.split(':')
    cnt+=int(comp[0])
    if comp[1]=='00':
        cnt+=0
    elif comp[1]=='30':
        cnt+=0.5
    if times[-2:] == 'PM':
        cnt+=12
    return cnt  

# Convert "Sart time" and "End time" functions to numeric types for calendar and schedule sheet

df_Schedule['Start Time'] = df_Schedule['Start Time'].apply(time_convert)
df_Schedule['End Time'] = df_Schedule['End Time'].apply(time_convert)
df_Calendar['Start_Time'] = df_Calendar['Start_Time'].apply(time_convert)
df_Calendar['End_Time'] = df_Calendar['End_Time'].apply(time_convert)


#st.write(df_Calendar)
### Reading Ratings ########### 
######## Ratings fetched from Metabase--> Copy paste mannually

values_input = main(SPREADSHEET_ID,'Rating')

df_Rating=pd.DataFrame(values_input[1:], columns=values_input[0])
#### Reading batch data ###########
values_input = main(SPREADSHEET_ID,'Batch')

df_Batch=pd.DataFrame(values_input[1:], columns=values_input[0])
df_Batch['Latest_Scheduled_Date'] = pd.to_datetime(df_Batch['Latest_Scheduled_Date'])

####### Reading Module sequence #########
values_input = main(SPREADSHEET_ID,'Modules')

df_Modules=pd.DataFrame(values_input[1:], columns=values_input[0])

## Reading Weight matrix ############
values_input = main(SPREADSHEET_ID,'Weight')

df_Weight=pd.DataFrame(values_input[1:], columns=values_input[0])
##########################################################

values_input = main(SPREADSHEET_ID,'Schedule_online')

df_Online_Schedule=pd.DataFrame(values_input[1:], columns=values_input[0])
########### Function to write to Google sheet #########

###
def write_to_sheet(place,data,sheet_id):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = None

    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'google.json', SCOPES) # here enter the name of your downloaded JSON file
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)
#service = discovery.build('sheets', 'v4', credentials=credentials)
    spreadsheet_id = sheet_id  # TODO: Update placeholder value.

# The A1 notation of a range to search for a logical table of data.
# Values will be appended after the last row of the table.
    range_ = place  # TODO: Update placeholder value.

# How the input data should be interpreted.
    value_input_option = 'RAW'  # TODO: Update placeholder value.

# How the input data should be inserted.
    insert_data_option = 'INSERT_ROWS'  # TODO: Update placeholder value.

    value_range_body = {  "values": [data]}
    # TODO: Add desired entries to the request body.

    request = service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, range=range_, valueInputOption=value_input_option, insertDataOption=insert_data_option, body=value_range_body)
    response = request.execute()

# TODO: Change code below to process the `response` dict:
    pprint(response)
###########################################################################

################# Logic Functions #######################
## Function to fetch active batches for the location 
def all_active(loc):
    loc_batches = df_Batch[df_Batch['Location']==loc]
    active_batches = loc_batches[loc_batches['Is_active']=='YES']
    return active_batches.loc[:,['Batch']]
###########################################################
## Function to fetch the schedule status for the given batch
def next_module_date(batch,location):
    latest_module = df_Batch[(df_Batch['Batch']==batch) & (df_Batch['Location']==location)]['Latest_scheduled_Module'].values[0]
    latest_module_index = df_Modules[df_Modules['Module Name']==latest_module]['Sequence'].values[0]
    next_module_index = str(int(latest_module_index)+1)
    last_module_index = df_Modules.tail(1)['Sequence'].values[0]
    latest_date = df_Batch[(df_Batch['Batch']==batch) & (df_Batch['Location']==location)]['Latest_Scheduled_Date'].values[0]
    date_list = pd.date_range(start=latest_date, periods=8, freq = 'D')
    if next_module_index != last_module_index:
        next_module = df_Modules[df_Modules['Sequence']==next_module_index]['Module Name'].values[0]
        next_date = date_list[-1] 
        return(latest_module,latest_date,next_module,next_date)
    return(latest_module,latest_date)
############################################################################################
### The function to fetch searching the available faculty for given module at the location
def search_faculty(module_name,location):
    df1 = df_Faculty[df_Faculty[module_name]=='Yes']
    list_faculty = list(df1['Faculty Name'])
    print(list_faculty)
    dict_faculty={}
    list_fac =[]
    list_loc =[]
    list_wei =[]
    list_int =[]
    for f in list_faculty:
        dict_faculty.update({f:0})
        list_fac.append(f)
        #print("f",f,df1[df1['Faculty Name']==f]['Internal'])
        inter = df1[df1['Faculty Name']==f]['Internal'].values[0]
        loc = df1[df1['Faculty Name']==f]['Location'].values[0]
        list_loc.append(loc)
        list_int.append(inter)
        if inter.lower() == 'yes':
            w = int(df_Weight[df_Weight['Criteria']=='Internal']['Weight'].values[0])
            w1 = dict_faculty[f] 
            dict_faculty[f] = w+w1
        else:
            w = int(df_Weight[df_Weight['Criteria']=='External']['Weight'].values[0])
            w1 = dict_faculty[f] 
            dict_faculty[f] = w+w1           
        if loc.lower() == location.lower():
            w = int(df_Weight[df_Weight['Criteria']=='Location']['Weight'].values[0])
            w1 = dict_faculty[f] 
            dict_faculty[f] = w+w1
        print(dict_faculty,inter,sep="|")
    
    for x in dict_faculty.values():
        list_wei.append(x)
    d=dict(Name=list_fac,Location=list_loc,Internal=list_int,Weight=list_wei)
    print(d)
    df_try = pd.DataFrame.from_dict(d)
    print(df_try)
    return df_try
############################################################################
### Function to check availability of the Faculty

def check_availability(faculty,start_date, end_date, start_time, end_time,resi_type):
    dates=pd.date_range(start= start_date, end=end_date)
    st = time_convert(start_time)
    et = time_convert(end_time)
  
    if resi_type == 'FT':

    #print(dates)
        df1 = df_Calendar[df_Calendar['Faculty']==faculty]
        if df1.shape[0]==0:
            return True
        for d in dates:
            df1 = df1[df1['Date']==d]
            #print(df1)
            if df1.shape[0] != 0:
                df1 = df1[(df1['Start_Time']< et) |  (df1['Start_Time']> st)]
                if df1.shape[0] !=0:
                    return False
                else:
                    flag = True
            else:
                flag = True
        else:
            if flag == True:
                return True
###########################################################################
#######################Function to read schedule of specific batch ###########
def read_schedule(location,batch,sdate,ldate):
    #print(type(sdate))
    sdate = pd.to_datetime(sdate)
    ldate = pd.to_datetime(ldate)
    #print(type(ldate))
    df_batch_schedule = df_Schedule[(df_Schedule['Location']==location) & (df_Schedule['Batch']==batch)]
    df_batch_schedule = df_batch_schedule[(df_batch_schedule['Start Date'].dt.date>=sdate) & (df_batch_schedule['Start Date'].dt.date<=ldate)]
    return df_batch_schedule
################################################################################
################## Function to read the schedule for faculty #############
def read_faculty_schedule(faculty,sdate,ldate):
        #print(type(sdate))
    sdate = pd.to_datetime(sdate)
    ldate = pd.to_datetime(ldate)
    #print(type(ldate))
    df_faculty_schedule = df_Schedule[df_Calendar['Faculty']==faculty]
    df_faculty_schedule = df_faculty_schedule[(df_faculty_schedule['Start Date'].dt.date>=sdate) & (df_faculty_schedule['Start Date'].dt.date<=ldate)]
    return df_faculty_schedule
################## Function to update the sheet ########################
# TODO: Change placeholder below to generate authentication credentials. See
# https://developers.google.com/sheets/quickstart/python#step_3_set_up_the_sample
#
# Authorize using one of the following scopes:
#     'https://www.googleapis.com/auth/drive'
#     'https://www.googleapis.com/auth/drive.file'
#     'https://www.googleapis.com/auth/spreadsheets'
def update_sheet(place,data,sheet_id):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = None

    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'google.json', SCOPES) # here enter the name of your downloaded JSON file
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)
#service = discovery.build('sheets', 'v4', credentials=credentials)
    spreadsheet_id = sheet_id  # TODO: Update placeholder value.

# The A1 notation of a range to search for a logical table of data.
# Values will be appended after the last row of the table.
    range_ = place  # TODO: Update placeholder value.

# How the input data should be interpreted.
    value_input_option = 'RAW'  # TODO: Update placeholder value.

# How the input data should be inserted.
    insert_data_option = 'INSERT_ROWS'  # TODO: Update placeholder value.

    value_range_body = {  "values": [data]}
    # TODO: Add desired entries to the request body.
    request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=range_, valueInputOption=value_input_option, body=value_range_body)
    response = request.execute()
##################################################################
########### Function to update the batch detailes ######################
def set_batch(batch,location,latest_date,latest_module):
    index = df_Batch.index
    condition = (df_Batch['Batch']==batch) & (df_Batch['Location']==location)
    number = index[condition]
    #print(number.tolist())
    #condition =number.tolist()[0]
    s_date = df_Batch[(df_Batch['Batch']==batch) & (df_Batch['Location']==location)]['Start_date'][0]
    owner = df_Batch[(df_Batch['Batch']==batch) & (df_Batch['Location']==location)]['Batch_Owner'][0]
    if latest_module == 'Case Study':
        active = 'NO'
    else:
        active = 'Yes'
    week = int(df_Batch[(df_Batch['Batch']==batch) & (df_Batch['Location']==location)]['Week_of_Year'][0])+1
    print(week)
    number = index[condition]
    row = number.tolist()[0]+2
    print(row)
    ranges = 'Batch!A'+str(row)
    print(ranges)
    update_sheet(ranges,list([batch, location, s_date,active, owner,latest_module,latest_date,week]),SPREADSHEET_ID)
###################################################################################################

###################### Function to set the batch inactive ####################
def set_inactive(batch,location):
    index = df_Batch.index
    condition = (df_Batch['Batch']==batch) & (df_Batch['Location']==location)
    number = index[condition]
    #print(number.tolist())
    #condition =number.tolist()[0]
    number = index[condition]
    row = number.tolist()[0]+2
    print(row)
    ranges = 'Batch!D'+str(row)
    print(ranges)
    update_sheet(ranges,list(['NO']),SPREADSHEET_ID)
################################################################
################# Function to get feedback #####################
def get_ratings(faculty,module):
    df_Rating['Faculty']=df_Rating['Faculty'].str.lower()
    df_Rating['Topic'] = df_Rating['Topic'].str.lower()
    faculty = faculty.lower()
    df_fac = pd.DataFrame(df_Rating[df_Rating['Faculty'].str.contains(faculty)])
    if module == 'SQL 1' or module == 'SQL 2':
        module = 'DBMS'
    elif module == 'Statistics':
        module = 'Stat'
    module = module.lower()
    df_fac = pd.DataFrame(df_fac[df_fac['Topic'].str.contains(module)])
    if df_fac.shape[0]==0:
        return '0'
    else:
        df_fac.sort_values(by='Session Date',ascending=False,inplace=True)
        df_fac = df_fac['Avg Ratings'].head(1)
        return df_fac.iloc[0]

######### GUI Code #####################
### Choosing Between Full time and Online Schedule
st.header("Faculty Planner")
application = st.sidebar.radio("Choose Application",('View Existing','Create New'))
if application=='Create New':
    batch_type = st.sidebar.radio( "Type of Batch", ("Full-Time", "Online"))

    print(batch_type)
    if batch_type == 'Full-Time':
        location = st.selectbox('City',('Banglore','Chennai','Gurgaon','Hyderabad','Mumbai','Pune'))
        df_active = all_active(location)
        #print(list(df_active.Batch))
        batch = st.selectbox('Select Batch',list(df_active.Batch))
        status = next_module_date(batch,location)
        if len(status)==4:
            st.write('Batch Selected : ',location,batch)
            st.write('Schedule available: Module:',status[0],'Week:', str(status[1])[:10])
            st.write('Upcoming Schedule: Module:',status[2],'Week',str(status[3])[:10])
            choice = st.radio('Do you want to schedule?',('Yes','No'))
            if choice == 'Yes':
                module = st.selectbox('Module Name',['ITP','NPV','Python LI','SQL 1','SQL 2','SQL LI','EDA','Statistics',
                'EDA_STAT LI','SLR','SLR LI','SLC','USL','SLC_USL LI','Case Study'])
                df_all = search_faculty(module,location)
                df_all.sort_values('Weight', axis=0, ascending=False, inplace=True )
                start_date = st.date_input('Start Date',min_value=datetime.date(2021, 1, 1))
                end_date = st.date_input('End Date',min_value=datetime.date(2021, 1, 1))
                av=[]
                ratings=[]
                for fac in df_all['Name']:
                    ratings.append(get_ratings(fac,module))
                    av.append(check_availability(fac,start_date, end_date, '9:30:00 AM', '5:00:00 PM','FT'))
                df_all['Available'] = av
                df_all['Ratings']=ratings

                df_all.sort_values(['Available','Weight'], axis=0, ascending=False, inplace=True )
                st.write(df_all)
                st.write('Proceed to schedule')
                faculty = st.selectbox('Faculty',df_all['Name'])
                available = check_availability(faculty,start_date, end_date, '9:30:00 AM', '5:00:00 PM','FT')
                if available:
                    st.write('Faculty is available')
                    st.write('Press Submit to confirm the schedule')
                    save = st.button('Submit')
                    if save:
                        ac = df_Batch[(df_Batch['Batch']==batch)&(df_Batch['Location']==location)]['Batch_Owner'].iloc[0]
                        print(ac)
                        write_to_sheet('Schedule',list(['DSE-FT',location,str(start_date), str(end_date),batch,module,faculty,ac,'9:30:00 AM', '5:00:00 PM','Residency',7,7]),SPREADSHEET_ID) 
                        dates=pd.date_range(start= start_date, end=end_date)
                        for d in dates:
                            write_to_sheet('Calendar',list([faculty,str(d),'9:30:00 AM', '5:00:00 PM']),SPREADSHEET_ID)
                        set_batch(batch,location,start_date,module)
                    
                    #final=st.radio('Do you want confirm?',('Yes','No'))
                    #if final == 'Yes':
                        ## write the code to write to sheet calendar
                        #ac = df_Batch[(df_Batch['Batch']==batch)&(df_Batch['Location']==location)]['Batch_Owner']
                        print()
                        #write_to_sheet('Schedule',list(['DSE-FT',location,str(start_date), str(end_date),batch,status[2],faculty,faculty,'9:30:00 AM', '5:00:00 PM','Residency',7,7]),SPREADSHEET_ID) 
                        #dates=pd.date_range(start= start_date, end=end_date)
                        #for d in dates:
                            #write_to_sheet('Calendar',list([faculty,str(d),'9:30:00 AM', '5:00:00 PM']),SPREADSHEET_ID)'''
                else:
                    st.write('Faculty not avaialable')
        else:
            st.write('The latest scheduled module for',location,batch, 'is',status[0],'for the week of', str(status[1])[:10])
            st.write('The batch has finished all modules')
            final = st.radio('Do you want to set it inactive',('Yes','No'))
            if final=='Yes':
                set_inactive(batch,location)
                
            
    else:
        batch = st.selectbox('Batch',['Online Jan 21','Online Feb 21','Online March 21'])
        module = st.selectbox('Module Name',['ITP','NPV','Python LI','SQL 1','SQL 2','SQL LI','EDA','Statistics',
                'EDA_STAT LI','SLR','SLR LI','SLC','USL','SLC_USL LI','Case Study'])
        start_date = st.date_input('Start Date',min_value=datetime.date(2021, 1, 1))
        end_date = st.date_input('End Date',min_value=datetime.date(2021, 1, 1))
        type_session = st.radio("Type of Session",['Residency','Lab'])
        st.write("Select days of week")
        days = st.multiselect('Week Days',['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'])
if application=='View Existing':
    view =st.sidebar.radio("Select View",("Faculty View","Batch View"))
    if view=='Batch View':
        location = st.selectbox('City',('Banglore','Chennai','Gurgaon','Hyderabad','Mumbai','Pune'))
        df_active = all_active(location)
        #print(list(df_active.Batch))
        batch = st.selectbox('Select Batch',list(df_active.Batch))
        from_date = st.date_input("From Date")
        to_date = st.date_input("To Date")
        df = read_schedule(location,batch,from_date,to_date)
        st.write(df)
    if view=='Faculty View':
        names = list(df_Faculty['Faculty Name'])
        fac = st.selectbox("Faculty",names)
        from_date = st.date_input("From Date")
        to_date = st.date_input("To Date")
        df = read_faculty_schedule(fac,from_date,to_date)
        st.write(df)