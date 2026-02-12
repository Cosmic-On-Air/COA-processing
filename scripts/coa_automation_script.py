# -*- coding: utf-8 -*-
"""
Created on Fri Jan  9 09:32:09 2026

@author: aidan
"""

#TODO use logging library, its especially useful since google API already integrates it
import cosmic_on_air as ca
import cosmic_on_air_db as ca_db

import traceback

from datetime import datetime
import time
import tempfile
import os
import io
from zipfile import ZipFile, ZIP_DEFLATED # to compress the html attachment
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

# If modifying these scopes, delete the file token.json.
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets", # read and write forms spreadsheet
    "https://www.googleapis.com/auth/drive", # TODO try make stricter access of drive
    "https://www.googleapis.com/auth/gmail.send", # sending automatic emails
]

def extract_drive_id(url):
    drive_id = url
    
    if "id=" in url:
        idx = url.rfind("=") + 1
        drive_id = url[idx:]
    if "file/d" in url:
        start_idx = url.rfind("file/d/") + 7
        stop_idx = url.rfind("/view")
        drive_id = url[start_idx:stop_idx]
    
    return drive_id

def get_creds():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("google_credentials.json", SCOPES)
            creds = flow.run_local_server(port=0) # only localhost:8080 is permitted
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    
    return creds

# Source - https://stackoverflow.com/a
# Posted by 7h3rAm, modified by community. See post 'Timeline' for change history
# Retrieved 2026-01-12, License - CC BY-SA 4.0

def is_internet(host="8.8.8.8", port=53, timeout=3):
    """
    Host: 8.8.8.8 (google-public-dns-a.google.com)
    OpenPort: 53/tcp
    Service: domain (DNS/TCP)
    """
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        socket.setdefaulttimeout(None)
        return True
    except socket.error:
        return False

def safe_execute(request, num_retries=3, quota_sleep=60):
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
    """
    
    # create drive api client
    service = build("drive", "v3", credentials=creds)

    # get filename
    filename = service.files().get(fileId=file_id, fields="name").execute()["name"]
    filename = os.path.join(path, filename)

    # pylint: disable=maybe-no-member
    request = service.files().get_media(fileId=file_id)
    file = io.FileIO(filename, "wb") # open local write-binary file
    downloader = MediaIoBaseDownload(file, request)
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

def get_spreadsheet_data(creds, sheet_id, sheet_range):
    """
    Get the a range of data from a spreadsheet.
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

def update_cell(creds, spreadsheet_id, sheet_range, value):
    """
    Update a single cell in the spreadsheet
    """    
    
    values = [[value]]
    body = {'values': values}
    
    service = build('sheets', 'v4', credentials=creds)
    
    result = safe_execute(service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=sheet_range,
        valueInputOption="RAW",
        body=body
    ))

    return result
        
def add_summary(creds, sheet_id, submission, data, img_id):
    """
    Add an entry to the weekly summary spreadsheet
    """
    values = [[
        submission[0], # submission timestamp
        submission[1], # submission email
        submission[7], # optional comment
        data['flight_number'], 
        str(data['date']), 
        data['device_id'], 
        img_id # image id for image of data summary
    ]]
    
    body = {'values': values}
    
    service = build('sheets', 'v4', credentials=creds)
    
    result = safe_execute(service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range="Sheet1!A1", # API uses this to figure out where the end of the data block is
        valueInputOption="RAW",
        body=body
    ))

    return result
        
def clear_range(creds, spreadsheet_id, sheet_range):
    """
    Clear a range of data from the spreadsheet.
    """
    
    service = build('sheets', 'v4', credentials=creds)
    
    result = safe_execute(service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=sheet_range
    ))
    
    return result

def create_folder(creds, folder_name, parent_id):
    """
    Create a folder on the google drive.
    """
    folder_metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder", # google drive folder
        "parents": [parent_id]
    }
    
    # create drive api client
    service = build("drive", "v3", credentials=creds)
    
    folder = safe_execute(service.files().create(body=folder_metadata, fields="id"))
        
    return folder["id"]
    
def upload_file(creds, local_path, parent_id):
    filename = os.path.basename(local_path)
    
    # Wrap the local file for upload
    media = MediaFileUpload(local_path, resumable=True)
    
    file_metadata = {
        "name": filename,
        "parents": [parent_id]
    }
    
    # create drive api client
    service = build("drive", "v3", credentials=creds)
    
    # Create the file in Drive
    uploaded_file = safe_execute(service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ))
        
    return uploaded_file["id"]

def update_file(creds, local_path, file_id):
    # Wrap the local file for upload
    media = MediaFileUpload(local_path, resumable=True)
    
    # create drive api client
    service = build("drive", "v3", credentials=creds)
    
    result = safe_execute(service.files().update(
        fileId=file_id,
        media_body=media,
    ))
    
    return result
        
def delete_file(creds, file_id):    
    service = build("drive", "v3", credentials = creds)
    
    result = safe_execute(service.files().delete(fileId=file_id))
    
    return result
        
def gmail_send_message(creds, raw_msg):
    """Create and send an email message
    Print the returned  message id
    Returns: Message object, including message id
    
    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """
    
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
        
def error_email(sender, error, traceback, extra):
    message = EmailMessage()
    message.set_content(error + "\n" + extra + "\n" + traceback)
    
    message["To"] = sender
    message["From"] = sender
    message["Subject"] = "An error occured in the coa test script"
    
    # encode message
    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    
    return {"raw": encoded_message}
        
def result_email(sender, form_submission, image_path, zip_path):
    #TODO revert back to no zip file now that file size is smaller
    timestamp = form_submission[0]
    to = form_submission[1]
    name = form_submission[2]

    # TODO in final version, the names must be replaced with Cosmic On Air
    subject = f"CoA Test Automation – Your Flight Radiation Data [{timestamp}]" # timestamp

    body_msg = f"""
    <p>Hello {name},</p>

    <p>We have now completed processing your submission to the Cosmic On Air Google Form.</p>

    <p>Below is an embedded image summarising the radiation dose data collected during your flight.  
    Additionally, an interactive HTML file of your results is provided inside a ZIP archive to reduce size. 
    Please download and unzip the file before opening it in your browser. A desktop browser is recommended 
    for optimal scaling and reliability, as mobile browsers may not fully support interactive features.
    The file features interactive graphs and a world map.</p>

    <p>Please note: this is an automated email.<br> 
    If you included comments in your form submission, our team will review and respond within 7 days.</p>

    <p>Kind regards,<br>
    Aidan's Test Program</p>
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

    # Attach image
    with open(image_path, 'rb') as f:
        img = MIMEImage(f.read())
        img.add_header('Content-ID', '<image1>')
        message.attach(img)
    
    # zip file attachment
    filename = "results.zip"
    with open(zip_path, "rb") as f:
        mime_part = MIMEBase('application', 'zip')
        mime_part.set_payload(f.read())

    encoders.encode_base64(mime_part)
    mime_part.add_header('Content-Disposition', 'attachment', filename=filename)
    message.attach(mime_part)

        
    # Encode for Gmail API
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return {'raw': raw}
        
def summary_email(sender, to, date, values, images):
    
    message = EmailMessage()
    
    message["To"] = to
    message["From"] = sender
    message["Subject"] = f"Weekly summmary for {date}"
    
    if len(values) == 0:
        body = ("Dear Aidan (testing program),"
                + f"\n\nThere are no new data submissions for {date}."
                + "\n\nRegards,\nAutomated Cosmic on Air script")
        
        message.set_content(body)
    else:
        # TODO address to team "Cosmic on Air Team"
        body = (
            "Dear Aidan (testing program),"
            + f"\n\nBelow is a list of the {len(values)} new submissions for {date}:"
            + "\nAdditionally, images of the data summary for each submission are attached."
        )
        
        for idx, value in enumerate(values):
            row = f"\n\n\t{idx+1}. {value[0]}, {value[1]}, \n\t\t{value[3]} {value[4]}, {value[5]},"
            row += f'\n\t\tOptional comment: "{value[2]}".'
            
            body += row
        
        body += "\n\nRegards,\nAutomated Cosmic on Air script"
        
        message.set_content(body)
        
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

debug = False

# maximum number of days passed since submission for automatic email to be sent.
max_delay = 7
    
# The ID and range of a submission spreadsheet.
coa_db_id = "1WUb_ZS_hhQAxNYrgg5ezhvpKNur350h_"
db_folder_id = "1NX8IytEoF2lHTqxEb4WYUU_w6PRLgf6r"
form_sheet = "1S9xGNofAsEmzuFlBKZUvNI6nmVUQZiAHnqZkjqADRlQ"
data_range = "Form responses 1!A2:I"
# cell updating is done with f"Form responses 1!I{idx+2}"
summary_sheet_id = "1ZGi_ePmfJW-pEhAIiY-qoVwBBbN-9trxvkp7OkP2-ls"
summary_week = "Sheet1!B1"
summary_range = "Sheet1!A3:G"
summary_folder_id = "1k-952PvNLrhmRENUxgaxFOB_upCCTQAD"

# email of the sender, use "Display Name <email address>" format
sender_email = "Aidan Gebbie <aidan.rw.gebbie@gmail.com>"

creds = get_creds()

with tempfile.TemporaryDirectory() as tmpdirname:
    values = get_spreadsheet_data(creds, form_sheet, data_range)
    currated_values = []
    
    new = False
    
    for row in values:
        if row[0] == "":
            continue
        
        # google sheets doesn't guarentee full A:I range, it trims off empty cells
        while len(row) < 9: # google sheets doesn't guarentee full A:I range, it trims off empty cells
            row.append("")
            
        if row[8] != "y" and row[8] != "Y": # if the row is marked as processed
            new = True
        
        currated_values.append(row)
        
    if new:
        database_path = os.path.join(tmpdirname, "data_archive")
        os.makedirs(database_path)
        
        database_file = get_file(creds, coa_db_id, database_path)
        database_file = os.path.join(database_path, database_file)
        
        db = ca_db.CoaDatabase(database_path, show_figures=False)
            
        for idx, row in enumerate(currated_values):
            if row[8] == "y" or row[8] == "Y": # if the row is marked as processed
                continue # already processed
            
            try:
                update_cell(creds, form_sheet, f"Form responses 1!I{idx+2}", "n")
                
                print("Processing response: " + row[1] + " (" + row[0] + ")")
                data_file = get_file(creds, extract_drive_id(row[5]), tmpdirname)
                flight_file = get_file(creds, extract_drive_id(row[6]), tmpdirname)
                
                flight, data = db.add(data_file, flight_file, citizen_id=row[1])
                
                entry_id, processed_file = flight[0], flight[8]
                processed_file = os.path.join(database_path, processed_file)
                
                entry_folder_id = create_folder(creds, entry_id, db_folder_id)
                backup_folder_id = create_folder(creds, "backup", entry_folder_id)
                
                fig = ca.plotly_plot(data)
                
                img_path = os.path.join(tmpdirname, f"{data['flight_number']} {data['date']} {data['device_id']}.png")
                html_path = os.path.join(tmpdirname, "html attachment.html")
                zip_path = os.path.join(tmpdirname, "results.zip")
                
                fig.write_html(html_path, include_plotlyjs="cdn")
                # don't forget. it requires you to install kaleido and pio.get_chrome()C:/Users/aidan/OneDrive - University of Cape Town/Cosmic On Air/Processed_data_12251023.log
                fig.write_image(img_path, width=1300, height=600)
                
                # writing files to a zipfile
                with ZipFile(zip_path,'w', ZIP_DEFLATED) as zip:
                    zip.write(html_path, arcname="results.html")
                
                msg = result_email(sender_email, row, img_path, zip_path)
                
                print("response message created.")
                
                update_cell(creds, form_sheet, f"Form responses 1!I{idx+2}", "y")
                
                if debug:
                    print("In debug, response email sending disabled")
                else:
                    # extra condition: if the submission is older than 7 days, don't send email.
                    timestamp = datetime.strptime(row[0], "%d/%m/%Y %H:%M:%S")
                    if (datetime.now() - timestamp).days > max_delay:
                        # TODO send logging warning to developer instead
                        print("Warning: submission older than 7 days, not sending email")
                    else:
                        gmail_send_message(creds, msg)
                
                update_file(creds, database_file, coa_db_id)
                upload_file(creds, data_file, backup_folder_id)
                upload_file(creds, flight_file, backup_folder_id)
                upload_file(creds, processed_file, entry_folder_id)
                
                # add to weekly summary list
                img_id = upload_file(creds, img_path, summary_folder_id)
                add_summary(creds, summary_sheet_id, row, data, img_id)
                
                os.remove(data_file)
                os.remove(flight_file)
                os.remove(html_path)
                os.remove(img_path)
                os.remove(zip_path)
                
                print("Finished processing response.")
            except Exception as e:
                tb_str = traceback.format_exc()
                #gmail_send_message(error_email(sender_email, str(e), tb_str, str(row)))
                raise
                
    #handle weekly summary
    values = get_spreadsheet_data(creds, summary_sheet_id, summary_week)
    sheet_week_number = int(values[0][0])
        
    iso_calender = datetime.now().isocalendar()
    week, year = iso_calender.week, iso_calender.year
    
    # if the numbers disagree, a new weekly summary email is due
    if sheet_week_number != week:
        print("Creating weekly summary.")
        if sheet_week_number > week:
            year -= 1
        date_str = f"Week {sheet_week_number} of {year}"
            
        values = get_spreadsheet_data(creds, summary_sheet_id, summary_range)
        images = []
        
        # download all the images for the summary
        for row in values:
            images.append(get_file(creds, row[6], tmpdirname))
        
        msg = summary_email(sender_email, "aidan.gebbie@outlook.com", date_str, values, images)
        
        print("Email created.")
        
        gmail_send_message(creds, msg)
        
        print("Email sent.")
        
        update_cell(creds, summary_sheet_id, summary_week, str(week))
        clear_range(creds, summary_sheet_id, summary_range)
        
        #TODO consider turning this into a batch request https://github.com/googleapis/google-api-python-client/blob/main/docs/batch.md
        for row in values:
            delete_file(creds, row[6]) # delete image on drive
        