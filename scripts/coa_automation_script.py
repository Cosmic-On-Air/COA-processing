# -*- coding: utf-8 -*-
"""
Created on Fri Jan  9 09:32:09 2026

@author: aidan
"""

"""
Name: coa_automation_script.py
Description:
    *   This code automates processing of google form data submissions using the google cloud API
    *   The first half of the code provides various functions for interacting with the google API
    *   Specifically it uses the google drive, google sheets, and Gmail APIs.
    *   The second half of the code is a script to process responses, email citizens their results,
        and send weekly summaries to the CoA team.
    *   The Script relies on both the cosmic_on_air_db and cosmic_on_air classes and functions to 
        perform its tasks. 
    *   It also requires OAuth keys for the google API, and naturally a reliable internet connection.

Cosmic On Air (cosmic-on-air.org; cosmiconair@gmail.com)

Version: 7 Mar 2026

Contributors:
A. Gebbie, Department of Physics, University of Cape Town, South Africa
"""

#TODO use logging library, its especially useful since google API already integrates it
##########################################################################################
# import required modules
import cosmic_on_air as ca
import cosmic_on_air_db as ca_db

import traceback

from datetime import datetime
import time
import tempfile
import os
import io
import socket
socket.setdefaulttimeout(None) #ensure that there is no default network request timeout

import base64
from email import encoders
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

################################################################################################
# define program constants

# debug flag. When debug==True, no emails are sent to citizens
debug = False

# If modifying these scopes, delete the file token.json.
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets", # read/write forms spreadsheet
    "https://www.googleapis.com/auth/drive", # read/write google drive files/folders
    "https://www.googleapis.com/auth/gmail.send", # sending automatic emails
]

# maximum number of days passed since submission for automatic email to be sent.
max_delay = 7
    
# The ID and range of a submission spreadsheet.
coa_db_id = "1BZdB4rI0tTv-XQso3f_l-g8tKOecjjFe"
db_folder_id = "1SzYj5EtJFghee3Tt2Ct7MX6N0Roc31bs"
form_sheet_id = "1CxkzhD3_av7tC_aTwajOspJ4aKAq3HuClFsxBYx6MiQ"
submission_range = "Form responses 1!A2:K"
# cell updating is done with f"Form responses 1!I{idx+2}"
summary_sheet_id = "1IMXxyc_c7Bys8t5n10XhM1ewyuzAL2FIAwgeX5zjvA0"
summary_folder_id = "1XpAHcxDHfVQzoAyi2q_XzC8NUqrjAXs4"
summary_week = "Sheet1!B1"
summary_range = "Sheet1!A3:G"

# email of the sender, use "Display Name <email address>" format
sender_email = "Cosmic On Air <cosmiconairuct@gmail.com>"

# email list of COA team to send weekly summaries to
summary_recipients = "Cosmic On Air UCT <cosmiconairuct@gmail.com>"
summary_recipients += ", Aidan Gebbie <gbbaid001@myuct.ac.za>"
#summary_recipients += ", Cosmic On Air Team <cosmiconair@gmail.com>"

no_kml_recipients = "Cosmic On Air UCT <cosmiconairuct@gmail.com>"
no_kml_recipients += ", Aidan Gebbie <gbbaid001@myuct.ac.za>"

# path to OAuth credentials
credentials_path = os.path.join(BASE_DIR, "credentials", "google_credentials.json")
token_path = os.path.join(BASE_DIR, "credentials", "token.json")

def extract_drive_id(url):
    """
    Function to extract the file id from a google drive share link

    Parameters
    ----------
    url : string of the link to the google drive file/folder

    Returns
    -------
    drive_id : string of the drive id of the file/folder

    """
    
    drive_id = url
    
    if "id=" in url: # one url style
        idx = url.rfind("=") + 1
        drive_id = url[idx:]
    elif "file/d" in url: # other url style
        start_idx = url.rfind("file/d/") + 7
        stop_idx = url.rfind("/view")
        drive_id = url[start_idx:stop_idx]
    
    return drive_id

def email_with_name(email, display_name):
    """
    Function to combine email and display_name into a string.
    
    Parameters
    ----------
    email : string of the email.
    
    display_name : string of the display name.
    
    Returns
    -------
    combined_email : string in format "display_name <email>"
    """
    
    return f"{display_name} <{email}>"

def get_creds():
    """
    Function to fetch the google OAuth credentials from the folder using the global
    constant for the path to the credentials.
    
    Parameters
    ----------
    None
    
    Returns
    -------
    creds : object of the google OAuth credentials.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists(token_path):
        print("found token file")
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_path, "w") as token:
            token.write(creds.to_json())
    
    return creds

def is_internet(host="8.8.8.8", port=53, timeout=3):
    """
    Function to check that there is an active network connection.
    It tests this by pinging the google public dns
    
    Source - https://stackoverflow.com/a
    Posted by 7h3rAm, modified by community. See post 'Timeline' for change history
    Retrieved 2026-01-12, License - CC BY-SA 4.0
    
    Host: 8.8.8.8 (google-public-dns-a.google.com)
    OpenPort: 53/tcp
    Service: domain (DNS/TCP)
    
    Parameters
    ----------
    None
    
    Returns
    -------
    state : boolean value, True if a successful ping was made, otherwise False.
    """
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        socket.setdefaulttimeout(None)
        return True
    except socket.error:
        return False

def safe_execute(request, num_retries=3, quota_sleep=60):
    """
    Function to safely execute a google API operation by attempting it a number of times
    and handling quota limit exceptions.
    
    Parameters
    ----------
    request : object of the specific API request being made
    
    num_retries : (default=3) positive integer number of retries to attempt, this is passed
        to the request.execute() function.
        
    quota_sleep : (default=60) number of seconds to sleep in a quota limit is reached.
    
    Returns
    -------
    result : result object returned by request.execute()
    """
    
    try:
        result = request.execute(num_retries=num_retries)
    except HttpError as e:
        if e.resp.status == 429: # quota limit exceeded
            #TODO add this to logging
            print(f"Quota exceeded, sleeping {quota_sleep} seconds...") 
            time.sleep(quota_sleep)
            result = request.execute(num_retries=num_retries)
        else:
            raise
    return result

def get_file(creds, file_id, path="", num_retries=3, quota_sleep=60):
    """
    Download a file from the google drive to the desired path.
    
    Parameters
    ----------
    creds : google API credentials object to use in request.
    
    file_id : google drive id of the file to download
    
    path : path to download the file to 
        (default="", file download to current working directory)
    
    num_retries : number of retry attempts to download file
        (default=3) (see downloader.next_chunk() function for details)
        
    quota_sleep : (default=60) number of seconds to sleep in a quota limit is reached.
    
    Returns
    -------
    filename : string of final absolute path to the file (including the file name).
    """
    
    # create drive api client
    service = build("drive", "v3", credentials=creds)

    # get filename
    filename = service.files().get(fileId=file_id, fields="name").execute()["name"]
    filename = os.path.join(path, filename)
    
    # create file download request and downloader object
    request = service.files().get_media(fileId=file_id)
    file = io.FileIO(filename, "wb") # open local write-binary file
    downloader = MediaIoBaseDownload(file, request)
    
    # download file in chunks (with timeout error handling)
    done = False
    while not done:
        try:
            status, done = downloader.next_chunk(num_retries=num_retries)
        except HttpError as e:
            if e.resp.status == 429: # quota limit exceeded
                #TODO add this to logging
                print(f"Quota exceeded, sleeping {quota_sleep} seconds...") 
                time.sleep(quota_sleep)
                status, done = downloader.next_chunk(num_retries=num_retries)
            else:
                raise
        
    return filename

def create_folder(creds, folder_name, parent_id):
    """
    Create a folder on the google drive.
    
    Parameters
    ----------
    creds : google API credentials object to use in request.
    
    folder_name : string for the name of the folder to create.
    
    parent_id : string google drive folder id of the parent folder to create the
        folder in.
    
    Returns
    -------
    folder_id : string of the google drive id of the new folder that was created.
    """
    
    # create the folder metadata
    folder_metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder", # google drive folder
        "parents": [parent_id]
    }
    
    # create and execute drive API call
    service = build("drive", "v3", credentials=creds)
    folder = safe_execute(service.files().create(body=folder_metadata, fields="id"))
        
    return folder["id"]

def upload_file(creds, local_path, parent_id):
    """
    Upload a file to the google drive.
    
    Parameters
    ----------
    creds : google API credentials object to use in request.
    
    local_path : path of the local file to upload to the google drive.
    
    parent_id : google drive id of the parent folder to upload the file to.
    
    Returns
    -------
    folder_id : string of the google drive id of the new file that was created.
    """
    # get filename of file
    filename = os.path.basename(local_path)
    
    # Wrap the local file for upload
    media = MediaFileUpload(local_path, resumable=True)
    
    
    file_metadata = {
        "name": filename,
        "parents": [parent_id]
    }
    
    # create and execute google drive API call
    service = build("drive", "v3", credentials=creds)
    uploaded_file = safe_execute(service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ))
        
    return uploaded_file["id"]

def update_file(creds, local_path, file_id):
    """
    Update an existing file on the google drive.
    
    Parameters
    ----------
    creds : google API credentials object to use in request.
    
    local_path : path of the local file to use to update the drive file.
    
    file_id : google drive id of the file to update.
    
    Returns
    -------
    result : result obtained from request.execute() API call.
    """
    
    # Wrap the local file for upload
    media = MediaFileUpload(local_path, resumable=True)
    
    # create and execute drive API call
    service = build("drive", "v3", credentials=creds)
    result = safe_execute(service.files().update(
        fileId=file_id,
        media_body=media,
    ))
    
    return result
        
def delete_file(creds, file_id):    
    """
    Delete a file on the google drive.
    
    Parameters
    ----------
    creds : google API credentials object to use in request.
    
    file_id : google drive id of the file to delete.
    
    Returns
    -------
    result : result obtained from request.execute() API call.
    """
    
    # create and execute drive API call
    service = build("drive", "v3", credentials = creds)
    result = safe_execute(service.files().delete(fileId=file_id))
    
    return result
        

def get_spreadsheet_data(creds, sheet_id, sheet_range):
    """
    Get a range of data from a google drive spreadsheet.
    
    Parameters
    ----------
    creds : google API credentials object to use in request.
    
    sheet_id : string google drive file id of the spreadsheet to read.
    
    sheet_range : string of range of data to read from (in sheets API range format)
    
    Returns
    -------
    values : python 2D list of values read from that range.
    """

    service = build("sheets", "v4", credentials=creds)

    # Call the Sheets API
    result = safe_execute(service.spreadsheets().values().get(
        spreadsheetId=sheet_id, 
        range=sheet_range,
        fields="values"
    ))
    values = result.get("values", []) # returns [] if there are no values
        
    return values

def update_cell(creds, sheet_id, sheet_range, value):
    """
    Update a single cell in the spreadsheet to a specific value.
    
    Parameters
    ----------
    creds : google API credentials object to use in request.
    
    sheet_id : string google drive file id of the spreadsheet to read.
    
    sheet_range : string of range of data to read from (in sheets API range format)
    
    value : string value to write (raw) to cell.
    
    Returns
    -------
    result : result of request.execute() API call
    """    
    
    values = [[value]]
    body = {'values': values}
    
    service = build('sheets', 'v4', credentials=creds)
    
    result = safe_execute(service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=sheet_range,
        valueInputOption="RAW",
        body=body
    ))

    return result
        
def add_summary(creds, sheet_id, submission, data, img_id):
    """
    Function to add an entry to the weekly summary spreadsheet.
    
    Parameters
    ----------
    creds : google API credentials object to use in request.
    
    sheet_id : string google drive file id of the spreadsheet to read.
    
    submission : the submission data read from the submission spreadsheet
    
    data : processed data from cosmic_on_air function.
    
    img_id : string google drive file id of the data image.
    
    Returns
    -------
    result of request.execute() API call    
    """
    # formulate list to write to spreadsheet
    values = [[
        str(submission[0]), # submission timestamp
        submission[1], # submission email
        submission[9], # optional comment
        data['flight_number'], 
        str(data['date']), 
        data['detector'] + " " + data['detector_serial'], 
        img_id # image id for image of data summary
    ]]
    
    body = {'values': values}
    
    # execute AP call
    service = build('sheets', 'v4', credentials=creds)
    result = safe_execute(service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range="Sheet1!A1", # API uses this to figure out where the end of the data block is
        valueInputOption="RAW",
        body=body
    ))

    return result
        
def clear_range(creds, sheet_id, sheet_range):
    """
    Clear a range of data from the google spreadsheet.
    
    Parameters
    ----------
    creds : google API credentials object to use in request.
    
    sheet_id : string google drive file id of the spreadsheet to read.
    
    sheet_range : string of range of data to delete (in sheets API range format)
    
    Returns
    -------
    result of request.execute() API call  
    """
    
    service = build('sheets', 'v4', credentials=creds)
    
    result = safe_execute(service.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range=sheet_range
    ))
    
    return result

def gmail_send_message(creds, raw_msg):
    """
    Send a Gmail email message.
    The message id is printed to console and returned.
    
    Parameters
    ----------
    creds : google API credentials object to use in request.
    
    raw_msg : Gmail API encoded raw message to send.
    
    Returns
    -------
    message_id : id of the email that was produced and sent.
    """
    
    # create service for gmail API
    service = build("gmail", "v1", credentials=creds)
    
    
    # have no retries to minimize risk of double sending emails
    result = safe_execute(
        service.users()
        .messages()
        .send(userId="me", body=raw_msg)
    , num_retries=0) # 'me' indicates that the sender is the authenticated user
    
    #TODO log message id
    print(f'Message Id: {result["id"]}')
    return result["id"]
        
def error_email(sender, contact, error, traceback, extra):
    """
    Function to create an error email object.

    Parameters
    ----------
    sender : string email of sender.
    
    contact : string email of person to send error email to.
    
    error : string of the error that occured.
    
    traceback : string of the error traceback.
    
    extra : string of extra information of error.

    Returns
    -------
    raw_msg : Gmail API encoded raw message to send.
    """
    
    message = EmailMessage()
    message.set_content(error + "\n" + extra + "\n" + traceback)
    
    message["To"] = contact
    message["From"] = sender
    message["Subject"] = "An error occured in the coa test script"
    
    # encode message
    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    
    return {"raw": encoded_message}
        
def result_email(sender, submission, image_path, html_path):
    """
    Function to create a result email to send to citizens after their data has been processed.
    
    Parameters
    ----------
    sender : string email of sender.
    
    submission : the submission data read from the submission spreadsheet.
    
    image_path : path of the image to embed into the email.
    
    html_path : path of the html file to attach to the email.
    
    Returns
    -------
    raw_msg : Gmail API encoded raw message to send.
    """
    
    timestamp = submission[0]
    to = email_with_name(submission[1], submission[2])
    name = submission[2]

    subject = f"Cosmic On Air – Your Flight Radiation Data [{timestamp}]" # timestamp

    body_msg = f"""
    <p>Hello {name},</p>

    <p>We have now completed processing your submission to the Cosmic On Air Google Form.</p>

    <p>Below is an embedded image summarising the radiation dose data collected during your flight.  
    Additionally, an interactive HTML file of your results is provided. 
    You can open the html file in your preferred web browser. A desktop browser is recommended 
    for optimal scaling and reliability, as mobile browsers may not fully support interactive features.
    The file features an interactive world map and graphs.</p>

    <p>Please note: this is an automated email.<br> 
    If you included comments in your form submission, our team will review and respond within 14 days.</p>

    <p>Kind regards,<br>
    Cosmic On Air Team</p>
    """
    # Root message
    message = MIMEMultipart('related')
    message['To'] = to
    message['From'] = sender
    message['Subject'] = subject

    # Alternative part (HTML body)
    msg_alternative = MIMEMultipart('alternative')
    message.attach(msg_alternative)

    # HTML body: normal text first, then image
    html_body = f"""
    <html><body>
    {body_msg}
    <img src="cid:image1">
    </body></html>
    """
    msg_alternative.attach(MIMEText(html_body, 'html'))

    # embed image
    with open(image_path, 'rb') as f:
        img = MIMEImage(f.read())
        img.add_header('Content-ID', '<image1>')
        message.attach(img)
    
    # html file attachment
    filename = "results.html"
    with open(html_path, "rb") as f:
        mime_part = MIMEBase('text', 'html')
        mime_part.set_payload(f.read())

    # Encode the payload in base64
    encoders.encode_base64(mime_part)
    mime_part.add_header('Content-Disposition', 'attachment', filename=filename)
    message.attach(mime_part)

        
    # Encode email for Gmail API
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return {'raw': raw}
        
def summary_email(sender, to, date, values, images):
    """
    Function to create a weekly summary email to send to coa team.
    
    Parameters
    ----------
    sender : string email of sender.
    
    to : string email of person to send email to.
    
    date : string of date of weekly summary email.
    
    values : 2D array of values from weekly summary spreadsheet
    
    images : list/tuple of image path strings for the images to attach to email.
    
    Returns
    -------
    raw_msg : Gmail API encoded raw message to send.
    """
    
    message = EmailMessage()
    
    message["To"] = to
    message["From"] = sender
    message["Subject"] = f"Weekly summmary for {date}"
    
    # if there are no new submissions, send appropriate email
    if len(values) == 0:
        body = ("Dear Cosmic On Air Team,"
                + f"\n\nThere are no new data submissions for {date}."
                + "\n\nRegards,\nAutomated Cosmic on Air Script")
        
        message.set_content(body)
        
    # else if there are new submissions, send email summarising submissions
    else:
        body = (
            "Dear Cosmic On Air Team,"
            + f"\n\nBelow is a list of the {len(values)} new submissions for {date}:"
            + "\nAdditionally, images of the data summary for each submission are attached."
        )
        
        # include summary blurb for each submission
        for idx, value in enumerate(values):
            row = f"\n\n\t{idx+1}. {value[0]}, {value[1]}, \n\t\t{value[3]} {value[4]}, {value[5]},"
            row += f'\n\t\tOptional comment: "{value[2]}".'
            
            body += row
        
        body += "\n\nRegards,\nAutomated Cosmic on Air Script"
        
        message.set_content(body)
        
        # attach images to email
        for image in images:            
            # Attach an image
            with open(image, "rb") as f:
                file_data = f.read()
            
            # EmailMessage automatically handles MIME type if you give maintype/subtype
            message.add_attachment(file_data,
                               maintype="image",
                               subtype="png",
                               filename=os.path.basename(image))
    
    # encode message
    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    
    return {"raw": encoded_message}

def no_kml_email(sender, to, indeces, submissions):
    """
    Function to create an alert email for submissions without kml files.
    
    Parameters
    ----------
    sender : string email of sender.
    
    to : string email of person to send email to.
    
    indeces : list of indeces of submissions without kml files
    
    submissions : 2D array of submissions kml files
    
    Returns
    -------
    raw_msg : Gmail API encoded raw message to send.
    """
    
    message = EmailMessage()
    
    message["To"] = to
    message["From"] = sender
    message["Subject"] = "Submissions without kml files"
    
    if len(indeces) == 0:
        return False
        
    # if there are new submissions, send email summarising submissions
    body = (
        "Dear Cosmic On Air Team,"
        + "\n\nThe following submissions don't have a flight kml file, please upload"
        + " a flight kml file to the google drive and paste its link in the submissions"
        + " spreadsheet for each submission affected."
        + "\n\nHere is the link to the spreadsheet: "
        + "https://docs.google.com/spreadsheets/d/1CxkzhD3_av7tC_aTwajOspJ4aKAq3HuClFsxBYx6MiQ/"
        + "\nHere is the link to the folder to upload kml files to: "
        + "https://drive.google.com/drive/folders/13kjYFl1lvHeuM3sfNMxMNVo9VRUu7-Dv3-oLV5Yn-rrvmVtBZ0ZSwrZO3b3cIfse063h7vQI"
        + "\n"
    )
    
    # include summary blurb for each submission
    for idx in indeces:
        value = submissions[idx]
        row = f"\nSpreadsheet Row {idx+2}: {value[5]} {value[6]} (Submission {value[0]}, {value[1]}),"
        
        body += row
    
    body += "\n\nRegards,\nAutomated Cosmic on Air Script"
    
    message.set_content(body)

    # encode message
    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    
    return {"raw": encoded_message}

##################################################################################################
# Script to process results

# Get google OAuth credentials
creds = get_creds()

errors = []
tracebacks = []

# create a temporary directory to handle all files in
with tempfile.TemporaryDirectory() as tmpdirname:
    # fetch latest list of values in submission spreadsheet
    values = get_spreadsheet_data(creds, form_sheet_id, submission_range)
    
    # adjust list to ensure correct dimensions and determine if there is new data
    new = False
    for row in values:
        # google sheets doesn't guarentee full A:I range, it trims off empty cells
        while len(row) < 11:
            row.append("")
        
        if row[0] == "":
            continue
        
            
        if row[10] != "y" and row[10] != "Y": # if the row is marked as processed
            new = True
            row[0] = datetime.strptime(row[0], "%m/%d/%Y %H:%M:%S")
            row[6] = datetime.strptime(row[6], "%m/%d/%Y").date()
            
        
    # if there are new submissions process these
    if new:
        # download coa.db file from google drive and create database object to interact
        # with existing database
        database_path = os.path.join(tmpdirname, "data_archive")
        os.makedirs(database_path)
        database_file = get_file(creds, coa_db_id, database_path) # download file
        database_file = os.path.join(database_path, database_file)
        db = ca_db.CoaDatabase(database_path, show_figures=False, show_progress=False)
        
        no_flight_kml = []
        
        # process each new response
        for idx, row in enumerate(values):
            # skip already processed responses
            if row[10] == "y" or row[10] == "Y":
                continue
            
            if row[8] == "":
                no_flight_kml.append(idx)
                print(f"No kml file in submission {row[1]} ({row[0]})")
                continue
                
            
            # TODO: add code to log errors with logging library
            # wrap process in try block to handle logging
            try:
                # update file cell to 'n' to indicate currently processing
                update_cell(creds, form_sheet_id, f"Form responses 1!K{idx+2}", "n")
                                
                print("Processing response: " + row[1] + " (" + row[0].strftime("%Y-%m-%d %H:%M:%S") + ")")
                
                # download raw log and flight file
                data_file = get_file(creds, extract_drive_id(row[7]), tmpdirname)
                flight_file = get_file(creds, extract_drive_id(row[8]), tmpdirname)
                
                # process/add data to database
                citizen_id = email_with_name(row[1], row[2])
                
                detector_serial = row[4]
                if detector_serial == "": detector_serial = "UNKNOWN"
                flight, data = db.add(data_file, flight_file, detector_id=row[3], detector_serial=detector_serial, citizen_id=citizen_id, submission_date=row[0])
                
                # get data_id and new log file of data
                entry_id, processed_file = flight[0], flight[8]
                processed_file = os.path.join(database_path, processed_file)
                
                # create html figure of plot
                fig = ca.plotly_plot(data)
                
                # create path names of image and html of figure to create
                img_path = os.path.join(tmpdirname, f"{entry_id}.png")
                html_path = os.path.join(tmpdirname, "html attachment.html")
                
                # create image and html of plotly figure
                fig.write_html(html_path, include_plotlyjs="cdn") # use 'cdn' to minimize html file size
                # don't forget. it requires you to install kaleido and pio.get_chrome()C:/Users/aidan/OneDrive - University of Cape Town/Cosmic On Air/Processed_data_12251023.log
                fig.write_image(img_path, width=1300, height=600)
                                
                msg = result_email(sender_email, row, img_path, html_path)
                print("response message created.")
                
                # change cell to 'y' for processed
                # Note that the submission is labelled as processed before any other API interactions
                # to avoid automatic reprocessing of submission incase of an error 
                # (if an error occured it should definitely be processed manuaully/supervised)
                update_cell(creds, form_sheet_id, f"Form responses 1!K{idx+2}", "y")
                
                # email the citizen
                if debug: # don't email in debug mode
                    print("In debug, response email sending disabled")
                else:
                    # only email if the submission is recent (within now - max_delay days).
                    if (datetime.now() - row[0]).days > max_delay:
                        errors.append(Exception(f"Warning: submission older than 7 days: {row}"))
                        print("Warning: submission older than 7 days, not sending email")
                    else:
                        gmail_send_message(creds, msg)
                
                # update coa.db file
                update_file(creds, database_file, coa_db_id)
                # create google drive folders for new database entry
                entry_folder_id = create_folder(creds, entry_id, db_folder_id)
                backup_folder_id = create_folder(creds, "backup", entry_folder_id)
                # upload processed log, raw log and flight kml
                upload_file(creds, data_file, backup_folder_id)
                upload_file(creds, flight_file, backup_folder_id)
                upload_file(creds, processed_file, entry_folder_id)
                
                # add to weekly summary list
                img_id = upload_file(creds, img_path, summary_folder_id)
                add_summary(creds, summary_sheet_id, row, data, img_id)
                
                # delete local files
                os.remove(data_file)
                os.remove(flight_file)
                os.remove(html_path)
                os.remove(img_path)
                
                # mark cell as completely processed
                update_cell(creds, form_sheet_id, f"Form responses 1!K{idx+2}", "Y")
                
                print("Finished processing response.")
                
            # TODO create appropriate logging module error
            # add errors to tracebacks and errors lists to raise at end
            # this is done so that an error in one submission doesn't delay
            # processing other submissions.
            except Exception as e:
                tb_str = traceback.format_exc()
                tracebacks.append(tb_str)
                errors.append(e)
                
        # email team for submissions without flight kml
        if len(no_flight_kml) > 0:
            msg = no_kml_email(sender_email, no_kml_recipients, no_flight_kml, values)
            gmail_send_message(creds, msg)
            
    else:
        print("no new submissions")
    
    # now handle weekly summary
    
    # retrieve week number of most recent summary email
    values = get_spreadsheet_data(creds, summary_sheet_id, summary_week)
    sheet_week_number = int(values[0][0])
        
    # get current week number
    iso_calender = datetime.now().isocalendar()
    week, year = iso_calender.week, iso_calender.year
    
    # if the numbers disagree, a new weekly summary email is due
    if sheet_week_number != week:
        print("Creating weekly summary.")
        
        # handle edge case of week number being greater than now, hence year overflow
        if sheet_week_number > week:
            year -= 1
        
        date_str = f"Week {sheet_week_number} of {year}"
        
        # get the 2D list of values from the weekly summary spreadsheet
        values = get_spreadsheet_data(creds, summary_sheet_id, summary_range)
        
        # download all the images to include in the summart email
        images = []
        for row in values:
            images.append(get_file(creds, row[6], tmpdirname))
        
        msg = summary_email(sender_email, summary_recipients, date_str, values, images)
    
        gmail_send_message(creds, msg)
        
        
        # reset weekly summary spreadsheet and delete images
        update_cell(creds, summary_sheet_id, summary_week, str(week))
        clear_range(creds, summary_sheet_id, summary_range)
        for row in values:
            delete_file(creds, row[6]) # delete image on drive
        
        print("Weekly summary sent.")

if errors:
    print()
    for tb in tracebacks:
        print(tb)
        
    raise Exception(f"{len(errors)} errors occured: {errors}")
else:
    print("Script ended without errors")