# Procedure as proposed (can be improved):

Users complete a google form on the COA website (info such as flight number, date, city, etc) and upload their detector file. The .kml file from Flightaware should also be manually uploaded (free for 7 days from flight). Otherwise the Administrator can do this later. We can try to sort out API access for flight data later.

The google form is kept on the Cosmic on Air google account. All submission data is stored in a linked spreadsheet while file attachments are stored in a folder on a google drive; both are kept on the COA google drive. The data is later accessed by a python script running on a local computer to process data and send reply emails.

Once a day, a computer retrieves the submissions from the google drive using google APIs. Once processed, a result summary graph is produced and emailed to the user. The processed data is added to a database of flights that is stored on the google drive. Additionally, once a week, a weekly summary email is sent to the Cosmic on Air Administrator with new flight graphs as well as flight details.

Both the raw data, flight data, and processed data are stored in an organised data archive on the Cosmic On Air google drive. The archive includes an SQL file which is used to make the archive easily searchable. Keywords, including flight date, flight number, and detector ID can be used to search the archive. An archive interaction script, streamlines functionality so a user can search and plot the summary graph of a particular flight or add/reprocess/delete flight data.

The functionality of all the automation described above is divided into 3 well documented python scripts: cosmic_on_air provides all functions used in handling data, cosmic_on_air_db provides a class to handle and streamline interactions with the data archive, coa_automation_script automates interactions with the google drive to retrieve and reply to submissions. 

# Note:

When using the cosmic_on_air functions, make sure to have the CARI_7A_DVD folder in the current working directory.
