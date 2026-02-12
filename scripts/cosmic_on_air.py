"""
Name: cosmic_on_air.py
Description:
    *   This code provides a library of functions to process data from radiation devices in flight.
    *   It supports reading files from the safecast, GMC, and Radiacode detectors,
    *   And can read flight ADS-B data from .kml and .csv formats
    *   If features automated interaction with the CARI-7A software to obtain reference radiation
        data for a flight.
    *   And includes an elaborate data plotting function using plotly which creates multiple 
        subplots using to summarise the data visually.
    *   The finally it includes functions to save and retrieve processed data.
    
    *   To ensure full use of features, download the CARI-7A software and extract it into the
        same folder as this script.
        https://www.faa.gov/data_research/research/med_humanfacs/aeromedical/radiobiology/cari7
    
Cosmic On Air (cosmic-on-air.org; cosmiconair@gmail.com)

Version: 10 Feb 2026

Contributors:
C. Briand, Laboratory for Space Studies and Instrumentation in Astrophysics, Observatoire de Paris, France
J. Trickett, Department of Physics, University of Cape Town, South Africa
A. Gebbie, Department of Physics, University of Cape Town, South Africa
"""

version = "v1" # version of script is indicated in processed log files

####################################################################################################
#Imports the modules we need
#Make sure you have these installed prior to running this code
import os
import tempfile
import shutil
from datetime import datetime, timedelta
import time
import numpy as np
import subprocess
from pykml import parser
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from matplotlib import pyplot as plt
import cartopy.crs as ccrs
import airportsdata
#from scipy.stats import t, f

####################################################################################################
def data_id(data):
    """
    Function to generate the unique data id string from the data
        id = flight_number + " " + date (YYYY-MM-DD) + " " + device_id
    
    Parameters
    data : data dictionary with 'flight_number', 'device_id' and 'date' keys
        date must be a datetime
    """
    return data['flight_number'] + " " + data['date'].strftime("%Y-%m-%d") + " " + data['device_id']

####################################################################################################
def read_raw_log(log_filename, flight_filename="", citizen_id="UNKNOWN", device_gps=False, parallel=6, time_delta=-1, disable_cari_weather=True):
    """
    Function to read and parse the log file and KML file and sort the data into a variety of categories including:
    device ID, datetime, count rate  and GPS coordinates.
    
    The function also checks if an existing processed data file exists, and uses that instead if it is found (this
    can be disabled by setting fore_reprocess=True)
    
    Parameters
    ----------
    log_filename : path and name of the device .log file to open
    
    flight_filename : path and name of the ADS-B flight data file to open
        (if not provided, program will attempt to use device gps data regardless to try recover data)
        
    citizen_id : string identifier of the individual who collected the data.
    
    device_gps : Boolean value to select device GPS instead of interpolating flightAware data
    
    parallel : number of parallel instances of CARI to run. If parallel <= 0, then it skips CARI software
        
    time_delta : default=-1. If greater than 0, the software will attempt to recover corrupted timestamps in data
        the value it is set to will be the delta time used between measurements if the end timestamp is corrupted
        
    
    disable_cari_weather : boolean value to disable the Geomagnetic storm and Furbush effect
        correction in the CARI-7A software

    Returns
    -------
    data : python dictioanry of arrays which includes:
        device_id, flight_number, origin, destination, origin ICAO, destination ICAO,
        date, takeoff, landing, time, cnt_1mn, cnt5sc, gps_lat, gps_lon, gps_alt,
        
        if CARI software is available, it also includes cari_total, total-neutron, R2 (fit of measurement to reference)
        
        data is trimmed to takeoff and landing
    
    """
    
    data = {'device_id' : "", 'flight_number' : "", 'citizen_id' : "",
            'origin' : "", 'destination' : "",
            'origin ICAO' : "", 'destination ICAO' : "",
            'date' : None, 'takeoff' : None, 'landing' : None, 
            'time_offset' : "", 'R2' : "", 'scaling_factor': 0.0,
            'timestamps' : "",
            'cnt_1mn' : [], 'cnt_5sc' : [], 
            'time' : [], 'lat' : [], 'lon' : [], 'alt' : []}
    
    data['citizen_id'] = citizen_id
    
    ##############################################################
    #Device Data processing
    
    _, ext = os.path.splitext(os.path.basename(log_filename))
    ext = ext.lower()
    if ext == ".log":
        device_data = read_safecast_log(log_filename)
    elif ext == ".csv":
        device_data = read_otherdata_csv(log_filename)
    elif ext == ".txt":
        device_data = read_uct_data(log_filename)
    else:
        raise ValueError("Only data .log or .csv files are accepted")
        
    fixed_times = fix_times(device_data['time'], time_delta)
    
    if fixed_times is device_data['time']:
        data['timestamps'] = "original"
    else:
        data['timestamps'] = "repaired"
        device_data['time'] = fixed_times
    
    ##############################################################
    # flight data processing
    _, ext = os.path.splitext(os.path.basename(flight_filename))
    if ext == ".kml":
        flight_data = read_flight_kml(flight_filename)
    elif ext == ".csv":
        flight_data = read_flight_csv(flight_filename)
    else: # try extract date and flight number from flight string, else prompt console
        try:
            rows = flight_filename.split(",")
            # Try to use device gps data anyways since no flight data is available
            takeoff = datetime.strptime(rows[0].strip(), "%Y-%m-%d %H:%M:%S")
            landing = datetime.strptime(rows[1].strip(), "%Y-%m-%d %H:%M:%S")
            
            flight_number = rows[2].strip()
        except:
            # Try to use device gps data anyways since no flight data is available
            takeoff = datetime.strptime(input("What is the takeoff time? YYYY-MM-DDThh:mm:ssZ\n"), "%Y-%m-%dT%H:%M:%SZ")
            landing = datetime.strptime(input("What is the landing time? YYYY-MM-DDThh:mm:ssZ\n"), "%Y-%m-%dT%H:%M:%SZ")
            
            flight_number = input("What is the flight_number? if unknown, enter UNKNOWN\n")
            
        flight_data = recover_flight(device_data, takeoff, landing)
        flight_data['flight_number'] = flight_number
        # Add ADS-B data retrieval code here later
    
    #############################################################
    # Generate reference data from CARI-7A
    if os.path.isfile(os.getcwd() + "/CARI_7A_DVD/CARI-7A.exe") and parallel > 0:
        # Generate reference CARI-7A radiation values
        cari_data = gen_cari_data(flight_data, parallel=parallel, disable_weather=disable_cari_weather)
        
        device_takeoff_idx, device_landing_idx, R2 = align_time(device_data, flight_data, cari_data)
        
        data['R2'] = f"{R2:.4f}"
    # rough method to align data without CARI software (time alignment may be innacurate)
    else:
        cari_data = None
        print("Oh no, CARI-7A software wasn't found",
              "Radiation reference based on flight path is unavailable",
              "To resolve this error, download the CARI-7A software and",
              "extract the CARI_7A_DVD folder into the same directory",
              "as this python script.\n",
              "Program will fall back to less accurate takeoff measurement",
              "and all results will be displayed in CPM instead of μSv.", sep="\n")
        
        device_takeoff_idx, device_landing_idx = estimate_takeoff(device_data, flight_data['landing'] - flight_data['takeoff'])
    
    ##############################################################
    # Merge flight and device data, correcting for offset between flight and device time

    idx_range = slice(device_takeoff_idx, device_landing_idx+1)

    keys = ['flight_number', 'date', 'takeoff', 'landing',
            'origin', 'origin ICAO', 'destination', 'destination ICAO']

    for key in keys:
        data[key] = flight_data[key]
        
    data['device_id'] = device_data['device_id']
    
    data['cnt_1mn'] = device_data['cnt_1mn'][idx_range]
    data['cnt_5sc'] = device_data['cnt_5sc'][idx_range]
    
    data['time_offset'] = str(int((data['takeoff'] - device_data['time'][device_takeoff_idx]).total_seconds()))
    
    data['time'] = device_data['time'][idx_range]
    data['time'] = data['time'] - data['time'][0] + data['takeoff']
    
    if device_gps:
        data['lat'] = device_data['lat'][idx_range]
        data['lon'] = device_data['lon'][idx_range]
        data['alt'] = device_data['alt'][idx_range]
    else:
        data['lat'] = np.full_like(data['time'], np.nan, dtype=np.float64)
        data['lon'] = np.full_like(data['time'], np.nan, dtype=np.float64)
        data['alt'] = np.full_like(data['time'], np.nan, dtype=np.float64)
        
    ###############################################################
    #Interpolate the FlightAware gps data to estimate longitude, latitude, and altitude
    time = np.array([(d - data['takeoff']).total_seconds() for d in data['time']])
    time_flight = np.array([(d - data['takeoff']).total_seconds() for d in flight_data['time']])
    
    # unwrap longitude so interpolation works correctly across 180° line
    adjusted_lon = unravel_lon(np.array(flight_data['lon']))
    
    interp_lat = np.interp(time, time_flight, flight_data['lat'])
    interp_lon = np.interp(time, time_flight, adjusted_lon)
    interp_alt = np.interp(time, time_flight, flight_data['alt'])
    
    interp_lon = ravel_lon(interp_lon) # wrap longitude back up
    
    data['lat'] = np.where(np.isnan(data['lat']), interp_lat, data['lat'])
    data['lon'] = np.where(np.isnan(data['lon']), interp_lon, data['lon'])
    data['alt'] = np.round(np.where(np.isnan(data['alt']), interp_alt, data['alt']))
    
    if cari_data is not None:
        data['cari_total']    = np.interp(time, time_flight, cari_data['total'])
        data['total-neutron'] = np.interp(time, time_flight, cari_data['total-neutron'])
        
        # in linear regression, if force α = 0, then β = ∑(x*y) / ∑(x²)
        data['scaling_factor'] = np.sum(data['cnt_1mn'] * data['total-neutron']) / np.sum(data['cnt_1mn']**2)
        
    # Reset cnt_tot data to 0 + data['cnt_1mn'][0] at start of flight
    #data['cnt_tot'] -= data['cnt_tot'][0] - data['cnt_1mn'][0]
    
    return data
    
####################################################################################################
def read_safecast_log(log_filename):
    """
    Function to read the log data from a Safecast sensor .log file at the given path
    If gps data is invalid at an entry, entries will store np.nan values
    If device data is invalid at an entry, that entry is omitted from the data dictionary
    
    Parameters
    ----------
    log_filename : string of the absolute path to the log file
    
    Returns
    -------
    data : dictionary of the data from the log file, including the following keys:
        device_id, (string)
        cnt_1mn, cnt_5sc, time, lat, lon, alt (numpy arrays; times are datetime objects)
    """
    
    data = {'device_id' : "", 'cnt_1mn' : [], 'cnt_5sc' : [], #'cnt_tot' : [], 
            'time' : [], 'lat' : [], 'lon' : [], 'alt' : []}
    
    with open(log_filename, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if line.startswith('$BNRDD'):
                row = line.split(',')
                if not "A" in row[6]: # not valid device data
                    continue
                
                data['device_id'] = "Safecast " + row[1].strip()
                data['time'].append(datetime.strptime(row[2].strip(),'%Y-%m-%dT%H:%M:%SZ'))
                
                data['cnt_1mn'].append(int(row[3]))
                data['cnt_5sc'].append(int(row[4]))
                #data['cnt_tot'].append(int(row[5]))
                
                lat, lon, alt = np.nan, np.nan, np.nan
                # If we use the device gps data, append it
                # Else we will replace nan values with FlightAware data
                if row[12] == 'A':
                    lat = float(row[7][0:2]) + float(row[7][2:])/60
                    lon = float(row[9][0:3]) + float(row[9][3:])/60
                    if row[8] == 'S':
                        lat*=-1
                    if row[10] == 'W':
                        lon*=-1
                    alt = float(row[11])
                
                data['lat'].append(lat)
                data['lon'].append(lon)
                data['alt'].append(alt)
                
    #Turn data lists into numpy arrays now that they are at their final size
    for key in data:
        if type(data[key]) is list:
            data[key] = np.array(data[key])
            
    return data

####################################################################################################
def read_uct_data(data_filename):
    """
    Function to read the log data from a Radiacode or GMC sensor (identifies either automatically)
    Parameters
    ----------
    data_filename : string of the absolute path to the log file
    
    Returns
    -------
    data : dictionary of the data from the log file, including the following keys:
        device_id, (string)
        cnt_1mn, cnt_5sc, time, lat, lon, alt (numpy arrays; times are datetime objects)
    """
    
    data = {'device_id' : "", 'cnt_1mn' : [], 'cnt_5sc' : [], #'cnt_tot' : [], 
            'time' : [], 'lat' : [], 'lon' : [], 'alt' : []}
    
    data['device_id'] = "UCT"
    
    dt5 = timedelta(seconds=5)
    
    with open(data_filename, "r", encoding="utf-8", errors="replace") as f:
        line1 = f.readline()
        f.readline()

        # the year is later ignored and fitted to actual flight data anyways, but needed by datetime
        start_time = datetime.strptime("2026 " + line1.strip(), "%Y %d %b %H:%M:%S")
        
        data['time'].append(start_time)
        data['cnt_5sc'].append(0)
        data['cnt_1mn'].append(0)
        
        # It is assumed that the UCT data gives timestamps for actual triggers
        # rather than constant interval timestamps with recent cpm
        
        for line in f:
            millis = timedelta(milliseconds=int(line.split(',')[0].strip()))
            time = start_time + millis
            
            if time - data['time'][-1] > dt5:
                elapsed5 = int((time - data['time'][-1]).total_seconds() // 5)
                for i in range(elapsed5):
                    data['time'].append(data['time'][-1] + dt5)
                    
                    data['cnt_5sc'].append(0)
                    # cnt_1mn sums previous 60 seconds
                    data['cnt_1mn'].append(sum(data['cnt_5sc'][-12:]))
                    
                    data['lat'].append(np.nan)
                    data['lon'].append(np.nan)
                    data['alt'].append(np.nan)
            
            data['cnt_5sc'][-1] += 1
            data['cnt_1mn'][-1] += 1
        
    #Turn data lists into numpy arrays now that they are at their final size
    for key in data:
        if type(data[key]) is list:
            data[key] = np.array(data[key])
            
    
    return data

####################################################################################################
def read_otherdata_csv(data_filename):
    """
    Function to read the log data from a Radiacode, GMC or Rium sensor (identifies either automatically)
    Parameters
    ----------
    data_filename : string of the absolute path to the log file
    
    Returns
    -------
    data : dictionary of the data from the log file, including the following keys:
        device_id, (string)
        cnt_1mn, cnt_5sc, time, lat, lon, alt (numpy arrays; times are datetime objects)
    """
    
    data = {'device_id' : "", 'cnt_1mn' : [], 'cnt_5sc' : [],
            'time' : [], 'lat' : [], 'lon' : [], 'alt' : []}
    
    with open(data_filename, "r", encoding="utf-8", errors="replace") as f:
        line1 = f.readline()
        
        if "GQ Electronics LLC" in line1:
            data['device_id'] = "GMC"
        
            for line in f:
                try:
                    row = line.split(',')
                    temp_time = None
                    for fmt in ("%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M"):
                        try:
                            temp_time = datetime.strptime(row[0].strip(), fmt)
                        except ValueError:
                            continue
                    if temp_time is None:
                        continue
                    
                    temp_cnt = row[3]
                    
                    if temp_cnt == "":
                        continue

                    data['time'].append(temp_time)
                    
                    data['cnt_1mn'].append(int(temp_cnt))
                    data['cnt_5sc'].append(int(temp_cnt)//12) # //(60/5)
                    
                    data['lat'].append(np.nan)
                    data['lon'].append(np.nan)
                    data['alt'].append(np.nan)
                except ValueError: # e.g. datetime error
                    continue
                except IndexError: # e.g. not a data line
                    continue
                
        elif "Time;Timestamp;" in line1:
            data['device_id'] = "Radiacode"
        
            for line in f:
                try:
                    row = line.split(';')
                    
                    #strip milliseconds from time
                    temp_time = datetime.strptime(row[0].strip()[:18], "%Y-%m-%d %H:%M:%S")
                    
                    temp_cnt = row[3]
                    
                    if temp_cnt == "":
                        continue
                    data['time'].append(temp_time)
                    
                    data['cnt_1mn'].append(int(float(temp_cnt)*60))
                    data['cnt_5sc'].append(int(float(temp_cnt)*5))
                    
                    data['lat'].append(np.nan)
                    data['lon'].append(np.nan)
                    data['alt'].append(np.nan)
                except ValueError: # e.g. datetime error
                    continue
                except IndexError: # e.g. not a data line
                    continue
        
        elif "rium" in data_filename.lower():
            data['device_id'] = "Rium"
        
            for line in f:
                try:
                    row = line.split(',')
                    
                    time = row[0].strip() + " " + row[1].strip()
                    
                    #strip milliseconds from time
                    temp_time = datetime.strptime(time, "%d/%m/%Y %H:%M:%S")
                    
                    temp_cnt = row[2]
                    
                    if temp_cnt == "":
                        continue
                    data['time'].append(temp_time)
                    
                    # rium doesn't correctly use 24hr time
                    # this is later corrected by the fix_time function
                    
                    data['cnt_1mn'].append(int(float(temp_cnt)*60))
                    data['cnt_5sc'].append(int(float(temp_cnt)*5))
                    
                    data['lat'].append(np.nan)
                    data['lon'].append(np.nan)
                    data['alt'].append(np.nan)
                except ValueError: # e.g. datetime error
                    continue
                except IndexError: # e.g. not a data line
                    continue
        
        else:
            raise Exception("Could not interpret data format of provided .csv file")
                
    #Turn data lists into numpy arrays now that they are at their final size
    for key in data:
        if type(data[key]) is list:
            data[key] = np.array(data[key])
            
    
    return data

####################################################################################################
def read_flight_kml(kml_filename):
    """
    Function to read flight ADS-B data from a .kml file
    
    Parameters
    ----------
    kml_filename : string of the absolute path to the kml file
    
    Returns
    -------
    data : dictionary of the data for the flight, including the following keys:
        flight_number, origin, destination, origin ICAO, destination ICAO, (string)
        date (datetime.date), takeoff, landing, (datetime)
        time, lat, lon, alt (numpy arrays; times are datetime objects)
    """
    
    data = {'flight_number' : "", 'origin' : "", 'destination' : "",
            'origin ICAO' : "", 'destination ICAO' : "",
            'date' : None, 'takeoff' : None, 'landing' : None,
            'time' : [], 'lat' : [], 'lon' : [], 'alt' : []}
    
    # Open the file with mode errors="replace" (replaces undecodable characters with �)
    with open(kml_filename, "r", encoding="utf-8", errors="replace") as f:
        doc = parser.parse(f).getroot()
        
    # Load ICAO-based database to get city names
    airports = airportsdata.load('ICAO')
    # Extract the origin, destination, flight, and date
    kml_name = str(doc.Document.name)
    
    origin_index = kml_name.rfind("-") - 4
    dest_index = kml_name.rfind(")") - 4
    date_index = kml_name.rfind("(") - 11
    flight_index = kml_name[:date_index-1].rfind(" ") + 1
    
    data['origin ICAO'] = kml_name[origin_index:origin_index+4]
    data['destination ICAO'] = kml_name[dest_index:dest_index+4]
    data['origin'] = airports.get(data['origin ICAO'])['city']
    data['destination'] = airports.get(data['destination ICAO'])['city']
    data['date'] = datetime.strptime(kml_name[date_index:date_index+10], "%d-%m-%Y").date()
    data['flight_number'] = kml_name[flight_index:date_index-1]
    
    # Extract flight time data
    for e in doc.Document.Placemark[2].findall(".//{http://www.opengis.net/kml/2.2}when"):
        data['time'].append(datetime.strptime(e.text,"%Y-%m-%dT%H:%M:%SZ"))
    
    # Extract flight gps data
    for e in doc.Document.Placemark[2].findall(".//{http://www.google.com/kml/ext/2.2}coord"):
        tmp = e.text.split()
        data['lon'].append(float(tmp[0]))
        data['lat'].append(float(tmp[1]))
        data['alt'].append(float(tmp[2]))
        
    # Save takeoff and landing times (start and end of FlightAware data)
    data['takeoff'] = data['time'][0]
    data['landing'] = data['time'][-1]
    
    for key in data:
        if type(data[key]) is list:
            data[key] = np.array(data[key])
    
    return data

####################################################################################################
def read_flight_csv(csv_filename):
    """
    Function to read flight ADS-B data from a .csv file
    
    Parameters
    ----------
    csv_filename : string of the absolute path to the .csv file
    
    Returns
    -------
    data : dictionary of the data for the flight, including the following keys:
        flight_number, origin, destination, origin ICAO, destination ICAO, (string)
        date (datetime.date), takeoff, landing, (datetime)
        time, lat, lon, alt (numpy arrays; times are datetime objects)
    """
    
    data = {'flight_number' : "", 'origin' : "", 'destination' : "",
            'origin ICAO' : "", 'destination ICAO' : "",
            'date' : None, 'takeoff' : None, 'landing' : None,
            'time' : [], 'lat' : [], 'lon' : [], 'alt' : []}
    
    # Open the file with mode errors="replace" (replaces undecodable characters with �)
    with open(csv_filename, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if not line[0].isdecimal():
                continue
            line = line.replace('"', '').split(",")
            data['time'].append(datetime.strptime(line[1].strip(),"%Y-%m-%dT%H:%M:%SZ"))
            data['flight_number'] = line[2].strip()
            data['lat'].append(float(line[3]))
            data['lon'].append(float(line[4]))
            data['alt'].append(float(line[5]) * 0.3048) # convert feet to meters
            
    for key in data:
        if type(data[key]) is list:
            data[key] = np.array(data[key])
            
    data = recover_flight(data, data['takeoff'], data['landing'], data['flight_number'])
    
    return data

####################################################################################################
def read_processed_log(log_filename):
    """
    Function to read the data from a processed log file. Processed log files are files produced by
    the cosmic_on_air software.
    
    Parameters
    ----------
    log_filename : string of the absolute path to the log file
    
    Returns
    -------
    data : dictionary of the data from the log file, including the following keys:
        device_id, flight_number, origin, destination, origin ICAO, destination ICAO (string)
        date (datetime.date), takeoff, landing, (datetime)
        cnt_1mn, cnt_5sc, time, lat, lon, alt (numpy arrays; times are datetime objects)
        if CARI-7A data is found in the file it is included:
        cari_total, total-neutron (numpy array)
    """
    
    data = {'device_id' : "", 'flight_number' : "", 'citizen_id' : "",
            'origin' : "", 'destination' : "",
            'origin ICAO' : "", 'destination ICAO' : "",
            'date' : None, 'takeoff' : None, 'landing' : None, 
            'time_offset' : "", 'R2' : "", 'scaling_factor' : 0.0,
            'timestamps' : "",
            'cnt_1mn' : [], 'cnt_5sc' : [], 
            'time' : [], 'lat' : [], 'lon' : [], 'alt' : []}
    
    has_reference = False # assume no CARI data at first
    
    # function to extract the data from a comment line in the format "# description = data"
    def strip_context(string):
        return string[string.find("=")+1:].strip()
    
    airports = airportsdata.load("ICAO") # load airports dictionary
    
    with open(log_filename, "r", encoding="utf-8", errors="replace") as f:        
        for line in f:
            if line.startswith("#"):
                if "device_id =" in line:
                    data['device_id'] = strip_context(line)
                elif "reference_fit_r2 =" in line:
                    data['R2'] = strip_context(line)
                elif "reference_time_offset_s =" in line:
                    data['time_offset'] = strip_context(line)
                elif "origin =" in line:
                    data['origin ICAO'] = strip_context(line)
                    data['origin'] = airports.get(data['origin ICAO'])['city']
                elif "destination =" in line:
                    data['destination ICAO'] = strip_context(line)
                    data['destination'] = airports.get(data['destination ICAO'])['city']
                elif "flight_number =" in line:
                    data['flight_number'] = strip_context(line)
                elif "takeoff_utc =" in line:
                    data['takeoff'] = datetime.strptime(strip_context(line), "%Y-%m-%dT%H:%M:%SZ")
                    data['date'] = data['takeoff'].date()
                elif "landing_utc =" in line:
                    data['landing'] = datetime.strptime(strip_context(line), "%Y-%m-%dT%H:%M:%SZ")
                elif "simulation_model = CARI-7A" in line:
                    has_reference = True
                    data['cari_total'] = []
                    data['total-neutron'] = []
                elif "reference_scaling_beta =" in line:
                    try:
                        data['scaling_factor'] = float(strip_context(line))
                    except:
                        data['scaling_factor'] = 0.0
                elif "citizen_id =" in line:
                    data['citizen_id'] = strip_context(line)
                elif "detector_timestamps =" in line:
                    data['timestamps'] = strip_context(line)
            
            else:
                row = line.split(",")
                data['time'].append(datetime.strptime(row[0].strip(),'%Y-%m-%dT%H:%M:%SZ'))
                data['cnt_1mn'].append(int(row[1]))
                data['cnt_5sc'].append(int(row[2]))
                #data['cnt_tot'].append(int(row[3]))
                data['lat'].append(float(row[3]))
                data['lon'].append(float(row[4]))
                data['alt'].append(float(row[5]))
                
                if has_reference:
                    data['cari_total'].append(float(row[6]))
                    data['total-neutron'].append(float(row[6]) - float(row[7]))
    
    #Turn data lists into numpy arrays now that they are at their final size
    for key in data:
        if type(data[key]) is list:
            data[key] = np.array(data[key])
    
    return data

####################################################################################################
def write_newlog(data, new_file):
    """
    Function to create a new log file by combining all the data provided
    
    Parameters
    ----------
    data : combined device, ADS-B, and optionally CARI-7A data
    
    new_file : name of the new file to create
    
    """
    device_id = data['device_id'].lower()
    
    with open(str(new_file), 'w', encoding="utf-8") as f:
        
        f.write(f"# format = processedCOA-{version}\n")
        f.write("# data delimiter = comma\n")
        
        f.write("#\n")
        f.write(f"# device_id = {data['device_id']}\n")
        f.write("# detector_model = ???\n") # TODO: identify device models
        
        if "safecast" in device_id:
            f.write("# detector_native_quantity = cnt_5s\n")
            f.write("# cnt_1min_source = original\n")
            f.write("# cnt_5s_source = original\n")
        elif "uct" in device_id:
            f.write("# detector_native_quantity = event_timestamps\n")
            f.write("# cnt_1min_source = derived\n")
            f.write("# cnt_5s_source = derived\n")
        elif ("radiacode" in device_id) or ("rium" in device_id):
            f.write("# detector_native_quantity = average_cps_over_1_minute\n")
            f.write("# cnt_1min_source = derived\n")
            f.write("# cnt_5s_source = derived\n")
        elif "gmc" in device_id:
            f.write("# detector_native_quantity = cnt_1mn\n")
            f.write("# cnt_1min_source = original\n")
            f.write("# cnt_5s_source = derived\n")
            
        f.write("# processing_pipeline = ???\n")
        
        if 'cari_total' in data:
            f.write("#\n")
            f.write("# reference_id = cari7a\n")
            f.write("# reference_model = CARI-7A\n")
            f.write("# reference_quantity = H*(10)_total-neutron\n")
            f.write("# reference_alignment_method = time_offset_max_r2\n")
            f.write(f"# reference_time_offset_s = {data['time_offset']}\n")
            f.write(f"# reference_scaling_beta = {data['scaling_factor']:.4e}\n")
            f.write("# reference_scaling_units = μSv/h / CPM\n")
            f.write(f"# reference_fit_r2 = {data['R2']}\n")
            
            f.write("#\n")
            f.write("# simulation_model = CARI-7A\n")
            f.write("# simulation_version = ???\n") # TODO: indicate whether forbush effect and geomagnetic storms are included
            f.write("# simulation_total = H*10_total\n")
            f.write("# simulation_neutron = H*10_neutron\n")
            f.write("# simulation_unit = μSv/h\n")
        else:
            f.write("#\n")
            f.write("# reference_id = ???\n")
            f.write("# reference_model = ???\n")
            f.write("# reference_quantity = ???\n")
            f.write("# reference_alignment_method = ???\n")
            f.write("# reference_time_offset_s = ???\n")
            if data['scaling_factor'] > 0:
                f.write(f"# reference_scaling_beta = {data['scaling_factor']:.4e}\n")
            else:
                f.write("# reference_scaling_beta = ???\n")
            f.write("# reference_scaling_units = ???\n")
            f.write("# reference_fit_r2 = ???\n")
            
            f.write("#\n")
            f.write("# simulation_model = ???\n")
            f.write("# simulation_version = ???\n")
            f.write("# simulation_total = ???\n")
            f.write("# simulation_neutron = ???\n")
            f.write("# simulation_unit = ???\n")
            
            
        # flight info
        f.write("#\n")
        f.write("# airport_code_type = ICAO\n")
        f.write(f"# origin = {data['origin ICAO']}\n")
        f.write(f"# destination = {data['destination ICAO']}\n")
        f.write(f"# flight_number = {data['flight_number']}\n")
        f.write(f"# takeoff_utc = {data['takeoff'].strftime('%Y-%m-%dT%H:%M:%SZ')}\n")
        f.write(f"# landing_utc = {data['landing'].strftime('%Y-%m-%dT%H:%M:%SZ')}\n")
        
        # repaired timestamps?
        f.write("#\n")
        f.write(f"# detector_timestamps = {data['timestamps']}\n")
        
        # units
        f.write("#\n")
        f.write("# timestamp_format = UTC_ISO8601\n")
        f.write("# latitude_unit = degrees\n")
        f.write("# longitude_unit = degrees\n")
        f.write("# altitude_unit = metres\n")
        
        f.write("#\n")
        
        f.write(f"# citizen_id = {data['citizen_id']}\n")
        
        f.write("#\n")
        
        if 'cari_total' in data:
            f.write("# columns = timestamp_utc, cnt_1min, cnt_5s, latitude, longitude, altitude, simulation_total, simulation_neutron\n")
        else:
            f.write("# columns = timestamp_utc, cnt_1min, cnt_5s, latitude, longitude, altitude\n")
        
        for i in range(len(data['time'])):
            line = ", ".join([data['time'][i].strftime("%Y-%m-%dT%H:%M:%SZ"),
                              f"{data['cnt_1mn'][i]}",
                              f"{data['cnt_5sc'][i]}",
                              f"{data['lat'][i]:.5f}",
                              f"{data['lon'][i]:.5f}",
                              f"{data['alt'][i]:.0f}"])
            
            if 'cari_total' in data:
                line += ", " + ", ".join([f"{data['cari_total'][i]:.4e}",
                                          f"{(data['cari_total'][i] - data['total-neutron'][i]):.4e}"])
                
            f.write(line + "\n")
            
####################################################################################################
def find_processed(log_filename):
    """
    Function to check if there is an existing processed log file for the given data.
    It checks if a file following the format Processed_data_... where ... is all the numbers
    in the original filename.
    
    If it finds an existing file, it returns the data, otherwise it returns None.
    
    Parameters
    ----------
    
    log_filename : name of the raw log file to find a processed file of
    
    Returns
    -------
    
    data : the data of the processed file, or None if it couldn't find any
    """
    
    file_path = os.path.dirname(log_filename)
    file_name, ext = os.path.splitext(os.path.basename(log_filename))
    file_name = "".join(char for char in file_name if char.isdecimal()) #extract log number
    processed_file_name = f"{file_path}{os.sep}Processed_data_{file_name}.log"
    
    if os.path.isfile(processed_file_name):
        print("found existing processed data .log file")
        return read_processed_log(processed_file_name) 
    else:
        return None

####################################################################################################
def lat_lon_dist(lat1, lon1, lat2, lon2, radius=6371):
    """
    Function to find the distance between two points on the earth
    
    Parameters
    ----------
    lat1 : point 1 latitude (or set of points)
    
    lon1 : point 1 longitude (or set of points)
    
    lat2 : point 2 latitude (or set of points)
    
    lon2 : point 2 longitude (or set of points)
    
    radius : radius of the sphere; default: Earth's radius in km
    
    Returns
    -------
    
    distance : distance between the points (or sets of points)
    """
    theta1 = np.radians(lat1)
    theta2 = np.radians(lat2)
    phi1 = np.radians(lon1)
    phi2 = np.radians(lon2)
    
    a = np.sin((theta2-theta1)/2)**2 + np.cos(theta1) * np.cos(theta2) * np.sin((phi2-phi1)/2)**2
    c = 2 * np.atan2(np.sqrt(a), np.sqrt(1-a))
    return c * radius

def unravel_lon(lon):
    """
    Function to unravel longitude (prevent jumping over 180 to -180 line).
    
    Parameters
    ----------
    lon : numpy array of floats
    
    Returns
    -------
    unravelled_lon : numpy array of floats
    """
    # unravel longitude for certain graphs
    unravelled_lon = lon.copy()
    for i in range(1, unravelled_lon.size):
        while unravelled_lon[i] - unravelled_lon[i-1] > 180:
            unravelled_lon[i] -= 360
        while unravelled_lon[i] - unravelled_lon[i-1] < -180:
            unravelled_lon[i] += 360
            
    return unravelled_lon

def ravel_lon(lon):
    """
    Function to ravel up longitude (force range of (-180; 180]).
    
    Parameters
    ----------
    unravelled_lon : numpy array of floats
    
    Returns
    -------
    lon : numpy array of floats
    """
    return (lon + 180) % 360 - 180

####################################################################################################
def recover_flight(data, takeoff=None, landing=None, flight_number=""):
    """
    Function to recover some of the flight data by identifying the closest
    airports to takeoff and landing times.
    
    For best reliability, It is important to provide takeoff and landing times.

    Parameters
    ----------
    data : flight data dictionary, which must include lat, lon, alt, time
    
    takeoff : datetime object for takeoff time. If it is provided, data is trimmed to takeoff time.
    
    landing : datetime object for landing time. If it is provided, data is trimmed to landing time.
    
    flight_number : The unique identifier of flight path for the flight. Default is ""

    Returns
    -------
    flight_data : recovered flight data dictionary with the following keys;
        flight_number, origin, destination, origin ICAO, destination ICAO,
        date, takeoff, landing, time, lat, lon, alt

    """
    flight_data = {'flight_number' : "", 'origin' : "", 'destination' : "",
                   'origin ICAO' : "", 'destination ICAO' : "",
                   'date' : None, 'takeoff' : None, 'landing' : None,
                   'time' : [], 'lat' : [], 'lon' : [], 'alt' : []}
    
    if 'flight_number' in data:
        flight_data['flight_number'] = data['flight_number']
    
    valid = np.logical_not(np.isnan(data['lat'] + data['lon'] + data['alt']))
    if takeoff is not None:
        valid = valid & (data['time'] >= takeoff)
    if landing is not None:    
        valid = valid & (data['time'] <= landing)
    
    flight_data['lat'] = data['lat'][valid]
    flight_data['lon'] = data['lon'][valid]
    flight_data['alt'] = data['alt'][valid]
    flight_data['time'] = data['time'][valid]
    
    # Load ICAO-based database to get city names
    airports = airportsdata.load('ICAO')
    
    lat = np.zeros(len(airports))
    lon = np.zeros(len(airports))
    icao = np.full(len(airports), "", dtype="U4")
    i = 0
    for key in airports:
        lat[i] = airports[key]['lat']
        lon[i] = airports[key]['lon']
        icao[i] = airports[key]['icao']
        i += 1
        
    takeoff_lat = flight_data['lat'][0]
    takeoff_lon = flight_data['lon'][0]
    landing_lat = flight_data['lat'][-1]
    landing_lon = flight_data['lon'][-1]
    
    distances1 = lat_lon_dist(takeoff_lat, takeoff_lon, lat, lon)
    distances2 = lat_lon_dist(landing_lat, landing_lon, lat, lon)
    
    flight_data['origin ICAO'] = icao[np.argmin(distances1)]
    flight_data['destination ICAO'] = icao[np.argmin(distances2)]
    flight_data['origin'] = airports.get(flight_data['origin ICAO'])['city']
    flight_data['destination'] = airports.get(flight_data['destination ICAO'])['city']
    flight_data['takeoff'] = flight_data['time'][0]
    flight_data['landing'] = flight_data['time'][-1]
    flight_data['date'] = flight_data['takeoff'].date()
    flight_data['flight_number'] = flight_number
    
    return flight_data

####################################################################################################
def fix_times(data_time, delta=-1, max_dt=1800):
    """
    Function to attempt to repare corrupted data timestamps.
    It takes note of any decreases in time, or increases by more than max_dt (default 120) seconds
    in time and interpolates between the last correct time and the nearest future correct time.
    If times at the beginning or end of the data are corrupted (i.e. no end time to interpolate to),
    it adds the average time delta to timestamps to repair corrupted times instead.
    
    Parameters
    ----------
    data_time : array of datetime objects
    
    delta : float value for time delta used if end times are corrupted.
        (default=-1, it determines a typical delta time from the data)
        
    max_dt: the maximum acceptable time delta for the data. Anything greater is considered corrupt.
        (default=1800; 30 minutes)
    
    Returns
    -------
    data_time : a new array of datetime objects with attempted corrections to timestamps.
    """
    
    times = np.array([(d - data_time[0]).total_seconds() for d in data_time], dtype=np.int64)
    
    dt = times[1:] - times[:-1]
    
    
    valid_dt = (0<dt) & (dt<=max_dt) # dt must be under max_dt
    
    if np.all(valid_dt): # return same object if times are correct
        return data_time
    
    # determine delta if not known
    if delta <= 0: 
        
        # data completely corrupt, don't try fixing it
        if not np.any(valid_dt):
            raise Exception("Unable to repair without specific dt; time too corrupt.")
        
        delta = np.median(dt[valid_dt])
    
    def valid(dt):
        if dt <= 0: return False
        if dt > max_dt: return False
        return True
    
    def increment_binary(array, i = 0):
        if array[-i] == False:
            array[-i] = True
            return
        else:
            array[-i] = False
            if i + 1 < len(array):
                increment_binary(array, i+1)
    
    errors = []
    for i in range(dt.size):
        if dt[i] == 0:
            dt[i] = delta
            # for non-incrementing errors, try group consecutive ones
            if i>1 and not valid_dt[i-1]:
                errors[-1] -= delta
            else:
                errors.append(-delta)
            
            continue
            
        if not valid_dt[i]:
            fixed = False
            
            if errors:
                # try find a combination of previous errors that cancel current error
                arr = [False] * len(errors)
                arr[0] = True
                for j in range(2**len(errors) - 1):
                    if sum(arr) <= 10: # don't try combine more than 10 errors (exponential)
                        error = 0
                        
                        for k in range(len(errors)):
                            if arr[k]: error += errors[k]
                        
                        if valid(dt[i] + error):
                            dt[i] += error
                            
                            for k in range(len(errors)-1, -1, -1):
                                if arr[k]: del errors[k]
                            
                            fixed = True
                            break
                
                    increment_binary(arr)
                
            if not fixed: # if no errors matched, add to list of errors and overwrite it
                errors.append(dt[i] - delta)
                dt[i] = delta
    
    times = np.concatenate([[0], np.cumsum(dt)])
    
    # correct_idx = np.argmax(valid_dt) + 1 # set to the first correct index
    
    # # correct times working backward from first valid timestamp
    # for i in range(correct_idx, 0, -1):
    #     times[i-1] = times[i] - delta
    
    # # interpolate ranges where start and end times are known
    # for i in range(correct_idx+1, times.size):
    #     if times[correct_idx] < times[i] < times[correct_idx] + delta * (i-correct_idx) + max_dt:
    #         # if there is a range of incorrect time, interpolate them
    #         if correct_idx != i-1:
    #             times[correct_idx:i] = np.linspace(times[correct_idx], times[i], i-correct_idx)
                
    #         correct_idx = i
    #     # else index is incorrect
    
    # correct times after last valid time to end of data
    
    # for i in range(correct_idx+1, times.size):
    #     times[i] = times[i-1] + delta
        
    # times = np.round(times)

    # finally return the correct times, rounded to the nearest second
    return np.array([data_time[0] + timedelta(seconds=int(t)) for t in times])

####################################################################################################
def estimate_takeoff(data, flight_duration, max_diff=100):
    """
    Function to estimate the takeoff and landing indeces of the data when given the flight duration.
    These indeces are found by determining the range that yields the highest total number of sensor
    counts (recall radiation dose is larger at higher altitude).
    
    This function is generally accurate to around 5 minutes.
    
    If CARI-7A data is available, the align_time function should be used instead as it is more accurate.
    
    Parameters
    ----------
    data : Radiation device data (with time and cnt_5sc keys)
    
    flight_duration : datetime.timedelta object with the duration of the flight
    
    max_diff : maximum change in sensor 5 second count Dose Rate. This is used to correct for spikes
        in data due to luggage scanners at airports.

    Returns
    -------
    takeoff_idx : estimated index of the takeoff in the data
    
    landing_idx : estimated index of the landing in the data (inclusive)
    """
    size = data['cnt_5sc'].size
    
    # To prevent error due to sudden spikes in sensor reading (possible from airport scans)
    # Find the difference between all the cnt_5sc, and cap it to a maximum difference
    cnt_5sc = np.zeros_like(data['cnt_5sc'])
    cnt_5sc[0] = data['cnt_5sc'][0]
    delta = np.zeros_like(data['cnt_5sc'])
    delta[1:] = data['cnt_5sc'][1:] - data['cnt_5sc'][:-1]
    delta = np.where(delta > max_diff, max_diff, delta)
    delta = np.where(delta < -max_diff, -max_diff, delta)
    
    # can't use cumsum for this since cnt_5sc mustn't dip below 0, which can occur if deltas were capped
    for i in range(1, size):
        cnt_5sc[i] = cnt_5sc[i-1] + delta[i]
        if cnt_5sc[i] < 0:
            cnt_5sc[i] = 0
    
    # get cummulative some of cnt_5sc data to speed up computing sums of ranges of the data in the loop
    cumsum = np.cumsum(cnt_5sc)  
    
    # find the window sum of cnt_5sc that yields the largest value
    takeoff_idx = 0
    landing_idx = size - 1
    max_sum = 0
    
    zero_dt = timedelta(seconds=0)
    
    window_stop = 0
    window_start = 0
    while window_stop != size - 1:
        # get next window stop (this check must be done since sensor equal poll time not guarenteed)
        for i in range(window_stop+1, size):
            dt = data['time'][i] - data['time'][window_start]
            if dt < flight_duration:
                window_stop = i
            else:
                break
        
        # conditions to handle possible edge cases that occur with currupted timestamps
        if dt <= zero_dt:
            window_start += 1
            window_stop = window_start + 1
            continue
                
        this_sum = cumsum[window_stop] - cumsum[window_start]
        if this_sum >= max_sum:
            max_sum = this_sum
            takeoff_idx = window_start
            landing_idx = window_stop
            
        window_start +=1
    
    return takeoff_idx, landing_idx
   
####################################################################################################
def align_time(device_data, flight_data, cari_data):
    """
    Function to find takeoff and landing indeces of the device data using flight data and cari data
    derived from the flight data.
    
    The function determines the slice of the device_data which has the best fit (highest R²) to the
    cari reference data using a linear regression with α = 0 bound.
    
    Parameters
    ----------
    device_data : Radiation device data (with time and cnt_5sc keys)
    
    flight_data : Flight path data (with takeoff, landing keys)
    
    cari_data : CARI-7A output data (with total-neutron key)
    
    Returns
    -------
    takeoff_idx : index of the takeoff in the data
    
    landing_idx : index of the landing in the data (inclusive)
    
    R2 : fit of the measured data to the CARI-7A data using a linear regression (0<=R2<=1)
    """
    
    size = device_data['time'].size
    flight_duration = flight_data['landing'] - flight_data['takeoff']
    reference = cari_data['total-neutron']
    reference_times = np.array([(t - flight_data['takeoff']).total_seconds() for t in flight_data['time']])
    
    times_from_start = np.array([(d - device_data['time'][0]).total_seconds() for d in device_data['time']])
    
    max_R2 = 0.0
    
    takeoff_idx = 0
    landing_idx = size - 1
    
    window_end = 0
    window_start = 0
    
    while window_end != size - 1:
        # get next window stop (this check must be done since sensor equal poll time not guarenteed)
        for i in range(window_end+1, size):
            dt = device_data['time'][i] - device_data['time'][window_start]
            if dt > flight_duration:
                window_end = i-1
                break
        else: #if it reaches end of data, then window_end = size-1
            window_end = size-1
        
        dt = device_data['time'][window_end] - device_data['time'][window_start]
        
        # if dt < 0 or dt < half of flight duration or less than 5 data points  (implausible flight window)
        if dt < 0.5 * flight_duration or window_end - window_start < 5:
            window_start += 1
            window_end = window_start + 1
            continue

        measurement = device_data['cnt_1mn'][window_start:window_end+1]
        measurement_times = times_from_start[window_start:window_end+1] - (device_data['time'][window_start] - device_data['time'][0]).total_seconds()
        
        reference_adjusted = np.interp(measurement_times, reference_times, reference)
        
        
        
        window_start += 1 
        
        ###############################################
        # linear regression from reference to measurement
        beta_hat = np.sum(measurement * reference_adjusted) / np.sum(reference_adjusted**2)
        if beta_hat < 0:
            continue
        
        e = measurement - beta_hat * reference_adjusted
        
        ESS = np.sum(e**2)
        TSS = np.sum((measurement - measurement.mean())**2)
        
        if TSS == 0: # avoid division by zero error
            continue
        
        R2 = 1 - ESS/TSS
        
        if R2 > max_R2:
            takeoff_idx = window_start
            landing_idx = window_end
            max_R2 = R2
        
    return takeoff_idx, landing_idx, max_R2
    
####################################################################################################
def gen_cari_data(location, parallel=4, disable_weather=True):
    """
    Function to generate reference dose rate in μSv/h for the given flight path.
    It interacts with the CARI-7A software to produce resultant data.
    
    Retfile includes additional keys:
    'total', 'total-neutron'.
    
    To speed up computing, the function produces multiple copies of the CARI-7A software
    in a temporary directory. It then distributes the data equally across the files.
    Additionally a subsample value can be provided to provide every ith datapoint to the
    CARI-7A software and interpolate the result.
    
    Note: Requires CARI_7A_DVD folder in the current working directory.
    
    Parameters
    ----------
    
    location : dictionary with latitude, longitude and altitude keys to arrays
    
    parallel : number of instances of the CARI-7A software to run in parallel
    
    disable_weather : boolean value to disable the Geomagnetic storm and Furbush effect
        correction in the CARI-7A software
    
    Returns
    -------
    
    cari_data : dictionary with 'total' and 'total-neutron' keys with arrays aligned to
        input data
    """
    
    start_time = time.time()
    
    lat = [location['lat'][0]]
    lon = [location['lon'][0]]
    alt = [location['alt'][0]]
    date = [location['time'][0].strftime('%Y/%m/%d')]
    hour = [str(location['time'][0].hour + 1)] # cari requires UTC+1
    t_subsampled = [0]
    
    t = np.array([(d - location['time'][0]).total_seconds() for d in location['time']])
    
    # only include entries if displacement >2km or altitude change >0.1km
    for i in range(1, len(location['lat'])):
        if (abs(location['alt'][i] - float(alt[-1])) >= 0.1 or
            lat_lon_dist(location['lat'][i], location['lon'][i], lat[-1], lon[-1]) > 2.0):
            lat.append(location['lat'][i])
            lon.append(location['lon'][i])
            alt.append(location['alt'][i])
            
            date.append(location['time'][i].strftime('%Y/%m/%d'))
            hour.append(str(location['time'][i].hour + 1))
            t_subsampled.append(t[i])
            
    
    lat_dir =["N" if d > 0 else "S" for d in lat]
    lon_dir =["E" if d > 0 else "W" for d in lon]
    lat = [f"{abs(d):.2f}" for d in lat]
    lon = [f"{abs(d):.2f}" for d in lon]
    alt = [f"{d/1000:.3f}" for d in alt]
    
    size = np.size(lat)
    
    total = np.zeros(size)
    neutron = np.zeros(size)
    
    total = np.zeros(size)
    neutron = np.zeros(size)
    
    """
    CARI-7A format:
    C N/S, LATITUDE, E/E, LONGITUDE, G/F/K, DEPTH, DATE, HR, D, P, GCR, SP 
    N, XX.XXXXX, E, XXX.XXXX, F, XXXX.XXXX YYYY/MM/DD, HXX, DX, PXX, CX, SX

    C EVERY LINE ABOVE THE FIRST INSTANCE OF START OR BELOW THE FIRST STOP IS A COMMENT
    C BETWEEN START AND STOP, BEGIN A COMMENT LINE WITH A 'C '
    C LIMIT DATA LINES TO 66 CHARACTERS
    
    It is also very slow, and uses very little hardware resources,
    so python code duplicates the folder and runs multiple subprocesses in parallel
    """
    
    #round width up
    widths = [int(size/parallel + 1)] * parallel
    widths[-1] = size - sum(widths[:-1])
    
    # Path to the original folder
    src = os.path.join(os.getcwd(), 'CARI_7A_DVD')

    # change CARI-7A settings to work in non-menus mode
    ini_file = os.path.join(src, 'CARI.INI')
    with open(ini_file, "r+") as f:
        lines = f.readlines()
        lines[5] = lines[5].replace("YES", "NO!")
        f.seek(0)
        f.writelines(lines)

    # change CARI-7A settings to work in non-menus mode
    default_file = os.path.join(src, 'DEFAULT.INP')
    with open(default_file, "r+") as f:
        lines = f.readlines()
        lines[4] = " DATA.LOC\n"
        f.seek(0)
        f.writelines(lines)
        
    # Set CARI-7A to ignore geomagnetic storms or Furbush effect in the reference
    userdata_file = os.path.join(src, 'FROMUSER.DAT')
    with open(userdata_file, "r+") as f:
        lines = f.readlines()
        if disable_weather:
            lines[0] = " 2, 'Kp index'\n" #geomagnetically quiet
            lines[2] = " 1.0000, 'Forbush scale factor'\n" #no forbush decrease
        else:
            print("Accounting for cosmic weather")
            lines[0] = " -1, 'Kp index'\n"
            lines[2] = " -1.0000, 'Forbush scale factor'\n"
        f.seek(0)
        f.writelines(lines)
    
    # Create a temporary directory to parallelize CARI_7A execution in
    with tempfile.TemporaryDirectory() as tmpdirname:
        offset = 0
        for p in range(parallel):
            dst = os.path.join(tmpdirname, str(p))
        
            # Copy the folder and all its contents
            shutil.copytree(src, dst)
                
            #Name of output files
            filename = os.path.join(dst, 'DATA.LOC') #Total radiation
            
            #Writes all the formatted information to the total and neutronradiation .LOC file        
            with open(filename, "w") as f:
                f.write("START-------------------------------------------------\n")
                for i in range(offset, offset+widths[p]):
                    #P0, specifies total radiation in CARI-7, P1 specifies neutron radiation
                    f.write(f"{lat_dir[i]}, {lat[i]}, {lon_dir[i]}, {lon[i]}, K, {alt[i]}, {date[i]}, H{hour[i]}, D4, P0, C4, S0\n")    
                    f.write(f"{lat_dir[i]}, {lat[i]}, {lon_dir[i]}, {lon[i]}, K, {alt[i]}, {date[i]}, H{hour[i]}, D4, P1, C4, S0\n")    
                f.write("STOP-------------------------------------------------\n")
                        
            offset += widths[p]
    
        print("Generated files for CARI-7A to predict radiation")
    
        try:
            instances = []
            stdout = subprocess.PIPE
            for i in range(parallel):  # launch 3 instances
                dst = os.path.join(tmpdirname, str(i))
                exe = os.path.join(dst, 'CARI-7A.exe')
                p = subprocess.Popen(
                    exe,
                    cwd=dst,
                    stdin=subprocess.DEVNULL, # no stdin needed
                    stdout=stdout, # only the first one's stdout is collected
                    stderr=None, # errors are passed to console
                    creationflags=subprocess.ABOVE_NORMAL_PRIORITY_CLASS,
                    text=True,
                    bufsize=1
                )
                instances.append(p)
                stdout = subprocess.DEVNULL
                
            # to simplify code and avoid multithreading, serially get progress from first
            # instance only, this can mean it might stay at 100% for a short time after
            # the first instance finishes while others aren't yet done
            print("\rProgress:  0.0% ", end="")  
            while any(p.poll() is None for p in instances):
                line = instances[0].stdout.readline().strip()   # keep updating
                
                progress = "".join(char for char in line if char.isdecimal())
                if progress:
                    progress = int(progress)/(widths[0]+1) * 50
                    print(f"\rProgress: {progress:4.1f}% ", end="")
                    
            for p in instances:
                if p.poll() is None:
                    p.wait()
        
        # finally terminate all the processes (incase of a keyboard or other interrupt)
        finally:
            for p in instances:
                if p.poll() is None:
                    p.kill()
            for p in instances:
                if p.poll() is None:
                    p.wait()

        print()
        print("done")
        
        offset = 0
        for p in range(parallel):
            dst = os.path.join(tmpdirname, str(p))
            
            #Name of output files
            filename = os.path.join(dst, 'DATA.ANS') #Total radiation
            
            with open(filename, "r") as f:
                f.readline()
                for i in range(offset, offset+widths[p]):
                    line = f.readline().split(",")
                    total[i] = float(line[8])
                    
                    line = f.readline().split(",")
                    neutron[i] = float(line[8])
            
        
            offset += widths[p]
            
        
    interp_total = np.interp(t, t_subsampled, total)
    interp_neutron = np.interp(t, t_subsampled, neutron)
            
    # Create new data lists for generated data
    cari_data = {}
    cari_data['total'] = interp_total
    cari_data['total-neutron'] = interp_total - interp_neutron
    
    print(f"CARI-7A Runtime: {(time.time() - start_time):.2f} s")
        
    return cari_data

####################################################################################################
def moving_average(arr, w):
    """
    Function to apply a moving average to a list of data
    Endpoint data is not modified by the moving average
    
    Parameters
    ----------
    arr : the list of data (integer of float) to apply a moving average to
    
    w : the width of the moving average. This width must be an odd number
    
    Returns
    -------
    arr : the list of data after the moving average is applied
    """
    if w % 2 == 0:
        raise ValueError("width must be odd")

    arr = np.array(arr)

    # Convolve with a uniform window
    arr[w//2:-w//2+1] = np.convolve(arr, np.ones(w)/w, mode='valid')

    return arr

####################################################################################################
def plot_latitude(data, plot_title):
    """
    Function to plot the Counts per minute vs Latitude graph with matplotlib
    
    Parameters
    ----------
    
    data : data from device .log file
    
    plot_title : String for the title of the plot
    """

    lat = data['lat']

    # take a moving average of the data to smoothen the graph out
    cpm = data['cnt_1mn']
    cpm = moving_average(cpm, 101)

    #Plots the figure and labels the axes
    plt.figure()
    plt.scatter(lat, cpm, marker = '.', label="CPS-1min", color = '#FF7518')
    plt.xlabel('Latitude (Degrees)', fontsize=14)
    plt.ylabel('Counts per Minute', fontsize=14)
    #Titles the plot - you can change this to match your data
    plt.title(plot_title, fontsize=14)

    #Ensures that take-off is always plotted on the left and landing on the right
    if lat[0] > lat[-1]:
        plt.gca().invert_xaxis()

####################################################################################################
def plot_longitude(data, plot_title):
    """
    Function to plot the Counts per minute vs Longitude graph with matplotlib
    
    Parameters
    ----------
    
    data : data from device .log file
    
    plot_title : String for the title of the plot
    """

    lon = data['lon']
    
    #Plots the figure and labels the axes
    plt.figure()

    # take a moving average of the data to smoothen the graph out
    cpm = data['cnt_1mn']
    cpm = moving_average(cpm, 101)

    plt.scatter(lon, cpm, marker = '.', label="CPS-1min", color = '#FF7518')
    plt.xlabel('Longitude (Degrees)', fontsize=14)
    plt.ylabel('Counts per Minute', fontsize=14)
    #Titles the plot - you can change this to match your data
    plt.title(plot_title, fontsize=14)

    #Ensures that take-off is always plotted on the left and landing on the right
    if lon[0] > lon[-1]:
        plt.gca().invert_xaxis()

####################################################################################################
def plot_altitude(data, plot_title):
    """
    Function to plot the Counts per minute vs Altitude graph with matplotlib
    
    Parameters
    ----------
    
    data : data from device .log file
    
    plot_title : String for the title of the plot
    """

    alt = data['alt']/1000    

    #Plots the figure and labels the axes
    plt.figure()

    # take a moving average of the data to smoothen the graph out
    cpm = data['cnt_1mn']
    cpm = moving_average(cpm, 3)

    plt.scatter(alt, cpm, marker = '.',label="CPS-1min", color = '#FF7518')
    plt.xlabel('Altitude (km)', fontsize=14)
    plt.ylabel('Counts per Minute', fontsize=14)
    #Titles the plot - you can change this to match your data
    plt.title(plot_title, fontsize=14)

####################################################################################################
def plot_world(data, plot_title):
    """
    Function to plot the Counts per minute as coloured points on a worldmap with matplotlib
    
    Parameters
    ----------
    
    data : data from device .log file
    
    plot_title : String for the title of the plot
    """

    #Plots the world map
    fig = plt.figure(figsize=(25, 15))
    ax1 = fig.add_subplot(111,projection=ccrs.PlateCarree())
    ax1.stock_img()

    #Ensures that any radiation > 95% of the max radiation does not skew the colour map
    d_tmp = data['cnt_1mn']
    threshold = max(d_tmp) * 0.95
    d_tmp = [d if d < threshold else threshold for d in d_tmp]

    # take a moving average of the data to smoothen the graph out
    d_tmp = moving_average(d_tmp, 5)

    #Plots the flight path
    p1=plt.scatter(data['lon'], data['lat'], c=d_tmp, cmap='inferno_r', vmin = 0, vmax = threshold)

    #Ensures that the whole world map is plotted
    #These limits can be changed if you would like to zoom into the flight path
    plt.xlim(-180, 180)
    plt.ylim(-90,90)
    #Titles the plot
    plt.title(plot_title, fontsize = 24)

    #Creates a legend for the colour-map
    cbar = plt.colorbar(p1, ax=ax1, orientation='vertical')
    cbar.set_label('Dose Rate (CPM)', labelpad=30, fontsize=22)
    cbar_ticks = [0, threshold  * 0.2, threshold  * 0.4, threshold  * 0.6, threshold  * 0.8, threshold]
    cbar.set_ticks(cbar_ticks)
    cbar.set_ticklabels([f'{tick:.1f}' for tick in cbar_ticks])
    cbar.ax.tick_params(labelsize=20)

####################################################################################################
def plotly_plot(data, moving_average_width=-1, main=-1, subsample=-1):
    """
    Function to plot a detailed figure summarising the data using various subplots in plotly.
    It also generates custom figure titles, hoverdata and layout using the flight data.
    
    If a list of sets of data are instead provided, it checks that the data are from the same flight,
    and if so, it plots the CPM of each data on each. (data from the first entry is used for map,
    altitude and CARI traces).
    scatter plot. Data with UNKNOWN flight number are considered part of the same flight. All data 
    must also have CARI-7A total-neutron to be plotted.
    
    Graphs plotted:
        * CPM vs latitude
        * CPM vs longitude
        * CPM vs altitude
        * CPM vs time
        * CPM on world map
        * altitude as secondary axis on the latitude, longitude and time plots
        * if calibration factor is provided, all plots are instead in μSv/h
        * if CARI-7A μSv/h data is available, all plots are in μSv/h and
            CARI reference is plotted in addition (calibration_factor is estimated)

    Parameters
    ----------
    data : data from the device log file and FlightAware kml file (or list of dictionaries of data 
        from multiple measurements of flight)
    
    moving_average_width : width of the moving average to denoise data for plotting
        (default=-1, it determines an appropriate width to average over a roughly 10 minute window)
    
    main : main trace to use for reference, altitude and map plots.
        Only useful when plotting multiple data.
        (default=-1, it selects the dataset with the smallest dt.)
    
    subsample : subsample data points to minimize plot file size. World map is not subsampled.
        (default=-1, it determines an appropriate subsampling of roughly 1 minute)
    
    Returns
    -------
    fig : A plotly figure summarising the data
    """
    
    #############################################################
    # standardize format of input variables
    if type(data) is dict:
        data = [data]
    
    if type(moving_average_width) is not list:
        moving_average_width = [moving_average_width] * len(data)
        
    if type(subsample) is not list:
        subsample = [subsample] * len(data)
    
    #############################################################
    # determine appropriate value for default arguments (subsample, moving average, main)
    min_dt = 3600
    selected_main=0
        
    for i, d in enumerate(data):
        time = np.array([(t - d['takeoff']).total_seconds() for t in d['time']])
        average_dt = np.average(time[1:] - time[:-1])
        if average_dt <= 0:
            average_dt=1e-9
        
        if moving_average_width[i] == -1:
            moving_average_width[i] = int(600 / average_dt)# make sure moving average is odd
            moving_average_width[i] += (moving_average_width[i]%2==0)
        if subsample[i] == -1:
            subsample[i] = int(60 / average_dt)
            if subsample[i] < 1:
                subsample[i] = 1
            
        if average_dt < min_dt:
            min_dt = average_dt
            selected_main=i
    
    if main < 0:
        main = selected_main

    #############################################################
    # Check that all measurements are from same flight
    flight_number = data[0]['flight_number']
    date = data[0]['date']
    for d in data:
        if d['date'] != date:
            raise ValueError("Not all data are from same flight")
        
        if len(data) > 1 and 'total-neutron' not in d:
            raise ValueError("All data must have CARI-7A total-neutron data")
        
        if "UNKNOWN" in d['flight_number']:
            continue
        if d['flight_number'] != flight_number:
            raise ValueError("Not all data are from same flight")    
    
    ###################################################################
    # Generate title and annotations
    title = f"<b>{data[0]['origin']} to {data[0]['destination']}</b>"
    annotation = f"{data[0]['flight_number']}<br>{data[0]['date'].strftime('%d %B %Y').strip('0')}"
    annotation += f"<br>{data[0]['takeoff'].strftime('%H:%M')}"
    annotation += f" to {data[0]['landing'].strftime('%H:%M')}"
    if len(data) == 1:
        annotation += f"<br>Detector {data[0]['device_id']}"
    else:
        annotation += "<br>Multiple Detectors"

    yaxis_unit = "CPM"
    
    ###############################################################
    # Calculate padded width of each dataset using the 'main' data
    unravelled_lon = unravel_lon(data[main]['lon'])
    
    time = np.array([(t - d['takeoff']).total_seconds()/3600 for t in data[main]['time']])
    lat_pad = 0.05 * np.ptp(data[main]['lat'])
    lon_pad = 0.05 * np.ptp(unravelled_lon)
    alt_pad = 0.05 * np.ptp(data[main]['alt']/1000)
    time_pad = 0.05 * np.ptp(time)
    lat_range = [data[main]['lat'].min() - lat_pad, data[main]['lat'].max() + lat_pad]
    lon_range = [unravelled_lon.min() - lon_pad, unravelled_lon.max() + lon_pad]
    alt_range = [(data[main]['alt']/1000).min() - alt_pad, (data[main]['alt']/1000).max() + alt_pad]
    time_range = [time.min() - time_pad, time.max() + time_pad]
    #Ensures that take-off is always plotted on the left and landing on the right
    if data[main]['lat'][0] > data[main]['lat'][-1]:
        lat_range[0], lat_range[1] = lat_range[1], lat_range[0]
    if unravelled_lon[0] > unravelled_lon[-1]:
        lon_range[0], lon_range[1] = lon_range[1], lon_range[0]
    
    ###############################################################
    # Create a figure with 6 subplots in a 2x3 grid
    fig = make_subplots(
        rows=2, cols=3,
        specs=[[{"type":"xy", "secondary_y":True}, {"type":"xy"},  {"type":"xy", "secondary_y":True}],
               [{"type":"xy", "secondary_y":True}, {"type":"map"}, {"type":"xy", "secondary_y":True}]],
        subplot_titles=("Dose Rate vs Latitude", title, "Dose Rate vs Longitude",
                        "Dose Rate vs Altitude", "Dose Rate on Worldmap", "Dose Rate vs Time")
    )
    
    ###############################################################
    # Iterate over data to plot all the data
    for i, d in enumerate(data):
        ###########################################################
        # Prepare variables for plot
        device_id = d['device_id']
        lon = np.array(d['lon'], dtype=np.float32)
        lat = np.array(d['lat'], dtype=np.float32)
        alt = np.array(d['alt']/1000, dtype=np.float32)
        cpm = np.array(d['cnt_1mn'], dtype=np.float32)
        time = np.array([(t - d['takeoff']).total_seconds()/3600 for t in d['time']], dtype=np.float32)
        time_string = [d.strftime("%H:%M:%S") for d in d['time']]
    
        # if data contains reference values for radiation, instead plot again radiation level
        if 'total-neutron' in d:
            reference = np.array(d['total-neutron'], dtype=np.float32)
            ss_reference = reference[::subsample[i]]
            
        if d['scaling_factor'] > 0:
            yaxis_unit = "μSv/h"
            cpm = cpm * d['scaling_factor']
        
        # Generate moving average CPM arrays for cleaner plots
        cpm_average = moving_average(cpm, moving_average_width[i])
        
        # Generate hoverdata for plots
        customdata = np.stack((lat, lon, alt, cpm_average, time_string), axis=-1)
        hovertemplate = (f'Device: {d["device_id"]}' +
                        '<br>Lat: %{customdata[0]:.2f}°' +
                        '<br>Lon: %{customdata[1]:.2f}°' +
                        '<br>Alt: %{customdata[2]:.2f} km' +
                        '<br>Time: %{customdata[4]}')
        if d['scaling_factor'] > 0:
            hovertemplate += '<br>Dose Rate: %{customdata[3]:.2f} ' + yaxis_unit
            hovertemplate += f'<br>Calibration Factor: {d["scaling_factor"]:.5f} μSv/h / CPM'
        else:
            hovertemplate += '<br>Dose Rate: %{customdata[3]:.0f} ' + yaxis_unit
        if 'total-neutron' in data[0]:
            customdata = np.stack((lat, lon, alt, cpm_average, time_string, reference), axis=-1)
            hovertemplate += '<br>reference Dose Rate: %{customdata[5]:.2f} ' + yaxis_unit
        hovertemplate += "<extra></extra>"
        
        # subsample data to minimize size of html file at basically no noticalbe visual loss.
        ss_lon = lon[::subsample[i]]
        ss_lat = lat[::subsample[i]]
        ss_alt = alt[::subsample[i]]
        ss_cpm = cpm_average[::subsample[i]]
        ss_time = time[::subsample[i]]
        
        if 'total-neutron' in data[0]:
            ss_customdata = np.stack((ss_lat, ss_lon, ss_alt, ss_cpm, 
                                      time_string[::subsample[i]],
                                      reference[::subsample[i]]), axis=-1)
        else:
            ss_customdata = np.stack((ss_lat, ss_lon, ss_alt, ss_cpm, 
                                      time_string[::subsample[i]]), axis=-1)
        
        # unravel longitude for certain graphs
        unravelled_lon = unravel_lon(ss_lon)
        
        ##############################################################
        # Plot data
        # Dose Rate vs Latitude
        fig.add_trace(go.Scatter(
            x=ss_lat,
            y=ss_cpm,
            mode='lines',
            line=dict(color="orange"),
            customdata=ss_customdata,
            hovertemplate=hovertemplate,
            name=device_id,
            legendgroup=device_id,
            legendrank=1
        ), row=1, col=1)
        # Dose Rate vs Longitude
        fig.add_trace(go.Scatter(
            x=unravelled_lon,
            y=ss_cpm,
            mode='lines',
            line=dict(color="orange"),
            customdata=ss_customdata,
            hovertemplate=hovertemplate,
            legendgroup=device_id,
            showlegend=False
        ), row=1, col=3)
        # Dose Rate vs Altitude
        fig.add_trace(go.Scatter(
            x=ss_alt,
            y=ss_cpm,
            mode='lines',
            line=dict(color="orange"),
            customdata=ss_customdata,
            hovertemplate=hovertemplate,
            legendgroup=device_id,
            showlegend=False
        ), row=2, col=1)
        # Dose Rate vs Time
        fig.add_trace(go.Scatter(
            x=ss_time,
            y=ss_cpm,
            mode='lines',
            line=dict(color="orange"),
            customdata=ss_customdata,
            hovertemplate=hovertemplate,
            legendgroup=device_id,
            showlegend=False
        ), row=2, col=3)
            
        #################################################################
        # If this is the main data, use it to plot altitude, CARI and worldmap data
        if i == main:
            
            # altitude vs latitude
            fig.add_trace(go.Scatter(
                x=ss_lat,
                y=ss_alt,
                mode='lines',
                line=dict(color="#aaaaee"),
                customdata=ss_customdata,
                hovertemplate=hovertemplate,
                name="Altitude",
                legendgroup="B",
                legendrank=2
            ), row=1, col=1, secondary_y=True)
            # altitude vs longitude
            fig.add_trace(go.Scatter(
                x=unravelled_lon,
                y=ss_alt,
                mode='lines',
                line=dict(color="#aaaaee"),
                customdata=ss_customdata,
                hovertemplate=hovertemplate,
                legendgroup="B",
                showlegend=False
            ), row=1, col=3, secondary_y=True)
            # altitude vs time
            fig.add_trace(go.Scatter(
                x=ss_time,
                y=ss_alt,
                mode='lines',
                line=dict(color="#aaaaee"),
                customdata=ss_customdata,
                hovertemplate=hovertemplate,
                legendgroup="B",
                showlegend=False
            ), row=2, col=3, secondary_y=True)

            # Dose Rate on Worldmap (don't subsample start and end of flight)
            
            fig.add_trace(go.Scattermap(
                lat=lat,
                lon=lon,
                customdata=customdata,
                hovertemplate=hovertemplate,
                showlegend=False,
                mode="markers",
                marker=dict(
                    size=10,
                    color=cpm_average,
                    cmin=0,
                    cmax=cpm_average.max(),
                    colorscale="Inferno_r",
                    colorbar=dict(
                        title=f"Dose Rate ({yaxis_unit})",
                        orientation="h",
                        x=0.5, y=-0.15, len=0.45,
                        outlinecolor = "black",
                        outlinewidth=1,
                        ticks="outside",
                    )
                )
            ), row=2, col=2)
            
            # if the data contains reference radiation doses, add these to the plot
            if 'total-neutron' in data[0]:
                # Dose Rate vs Latitude
                fig.add_trace(go.Scatter(
                    x=ss_lat,
                    y=ss_reference,
                    mode='lines',
                    line=dict(color="green"),
                    customdata=ss_customdata,
                    hovertemplate=hovertemplate,
                    name="CARI-7 [H*(10) - neutron]",
                    legendgroup="C",
                    legendrank=3
                ), row=1, col=1)
                # Dose Rate vs Longitude
                fig.add_trace(go.Scatter(
                    x=unravelled_lon,
                    y=ss_reference,
                    mode='lines',
                    line=dict(color="green"),
                    customdata=ss_customdata,
                    hovertemplate=hovertemplate,
                    legendgroup="C",
                    showlegend=False
                ), row=1, col=3)
                # Dose Rate vs Altitude
                fig.add_trace(go.Scatter(
                    x=ss_alt,
                    y=ss_reference,
                    mode='lines',
                    line=dict(color="green"),
                    customdata=ss_customdata,
                    hovertemplate=hovertemplate,
                    legendgroup="C",
                    showlegend=False
                ), row=2, col=1)
                # Dose Rate vs Time
                fig.add_trace(go.Scatter(
                    x=ss_time,
                    y=ss_reference,
                    mode='lines',
                    line=dict(color="green"),
                    customdata=ss_customdata,
                    hovertemplate=hovertemplate,
                    legendgroup="C",
                    showlegend=False
                ), row=2, col=3)
    
    #######################################################################
    # Add markers for the origin and destination airport
    airports = airportsdata.load("ICAO")
    dept = airports.get(data[0]['origin ICAO'])
    dest = airports.get(data[0]['destination ICAO'])
    dept['elevation'] *= 0.3048 # convert elevation to m
    dest['elevation'] *= 0.3048
    airport_hover = ("%{customdata.name}" +
                     "<br>%{customdata.iata}/%{customdata.icao}" +
                     "<br>%{customdata.city}, %{customdata.country}" +
                     "<br>lon: %{customdata.lon:.2f}°" +
                     "<br>lat: %{customdata.lat:.2f}°" +
                     "<br>alt: %{customdata.elevation:.0f} m<extra></extra>")
    fig.add_trace(go.Scattermap(
        lat=[dept['lat'], dest['lat']],
        lon=[dept['lon'], dest['lon']],
        customdata=[dept, dest],
        hovertemplate=airport_hover,
        showlegend=False,
        mode="markers",
        marker=dict(
            size=8,
            color="blue",
            symbol="triangle"
        )
    ), row=2, col=2)

    ###########################################################################
    # Update figure layout
    
    # Update subplot title fonts and positions
    fig.layout.annotations[1].update(x=0.5, y=1.05, font=dict(size=28, color="black"))
    fig.layout.annotations[4].update(x=0.5, y=0.55)
    fig.layout.annotations[2].update(x=0.89)
    fig.layout.annotations[5].update(x=0.89)
    
    # Update all axis labels
    fig.update_layout(
        xaxis=dict(title="Latitude (Degrees)", range=lat_range),
        yaxis=dict(title=f"Dose Rate ({yaxis_unit})"),
        yaxis2=dict(title="Altitude (km)"),
        
        xaxis3=dict(title="Longitude (Degrees)", range=lon_range, domain=[0.775,1.0]),
        yaxis4=dict(title=f"Dose Rate ({yaxis_unit})"),
        yaxis5=dict(title="Altitude (km)"),
        
        xaxis4=dict(title="Altitude (km)", range=alt_range),
        yaxis6=dict(title=f"Dose Rate ({yaxis_unit})"),
        
        xaxis5=dict(title="Time Since Takeoff (hours)", range=time_range, domain=[0.775,1.0]),
        yaxis8=dict(title=f"Dose Rate ({yaxis_unit})"),
        yaxis9=dict(title="Altitude (km)"),
        
        map_style="satellite",  # 2D view with satellite data
        map=dict(
            domain=dict(x=[0.28, 0.72],  y=[0.0, 0.54]),
            center=dict(lat=float(lat.mean()), lon=float(ravel_lon(unravelled_lon.mean()))),
            zoom = 0.2
        ),
        
        # Hide all trace legends, set the paper colour to light orange, and set mode to pan subplots
        paper_bgcolor='#FAE7C8', 
        dragmode="pan",
        shapes=[dict(
            type="rect",
            xref="paper", yref="paper",
            x0=0.28, y0=0, x1=0.72, y1=0.54,
            line=dict(color="black", width=2)
        )],
        
        # Move legend legend
        legend=dict(
            x=0.64, y=0.71,              # position
            xanchor="center",       # anchor legend’s right side
            yanchor="middle"          # anchor legend’s top side
        ),
        
        # Add buttons to zoom onto airports
        updatemenus=[
            dict(
                type="buttons",
                direction="down",
                x=0.36,
                y=0.73,
                xanchor="center",
                yanchor="middle",
                buttons=[
                       dict(
                        label="Reset Zoom",
                        method="relayout",
                        args=[{"map.zoom": 0.2, "map.center":dict(lon=float(ravel_lon(unravelled_lon.mean())), lat=float(lat.mean()))}]
                    ), dict(
                        label=dept['city'],
                        method="relayout",
                        args=[{"map.zoom": 9, "map.center": {'lon': dept['lon'], 'lat': dept['lat']}}]
                    ), dict(
                        label=dest['city'],
                        method="relayout",
                        args=[{"map.zoom": 9, "map.center": {'lon': dest['lon'], 'lat': dest['lat']}}]
                    )
                ]
            )
        ]
    )
    
    # Update legend again if there are multiple figures
    if len(data) > 1:
        fig.update_layout(
            legend=dict(
                maxheight=0.25,     # forces a max height for the legend (adds scrollbar)
                tracegroupgap=2,    #decreases gap between traces
                x=0.64, y=0.72,     # position
                xanchor="center",   # anchor legend’s right side
                yanchor="middle"    # anchor legend’s top side
            )
        )

    # Add subtitle with flight and device details
    fig.add_annotation(
        text=annotation,
        xref="paper", yref="paper",  # use paper coordinates (not on plot itself)
        x=0.5, y=1.05,
        showarrow=False,
        align="center",
        font=dict(size=18, color="black")
    )

    # Add logo image (accesses from internet)
    fig.add_layout_image(dict(
            source="https://yt3.googleusercontent.com/MzSEPIbzsSqBPAgir5TVHWnypHltkojxjUuHUYR7SVIT1uyg2M6P98Rz5cQ7laRtOxDDYn0D=s160-c-k-c0x00ffffff-no-rj",
            x=0.5, y=0.72,  # position in data coordinates
            xref="paper", yref="paper",
            sizex=0.2, sizey=0.2,
            xanchor="center", yanchor="middle"
    ))
    
    return fig

####################################################################################################
def annomaly_test(data, calibration_factor=-1):
    """
    IN PROGRESS
    
    A Function that does various statistical tests and calculations to provide insights into data,
    including indications of deviation from the reference data.
    We do a hypothesis test to check if there is a linear regression between measurements and predictions
    H₀ : β₀ = 1
    Hₐ : β₀ ≠ 1
    
    Note: if calibration_factor is not provided, the test makes a point estimate of the calibration factor:
        calibration_factor = mean(cpm_1mn) / mean(reference μSv/h)
    Parameters
    ----------
    data : data from the device log file and FlightAware kml file
    
    calibration_factor : factor to multiply sensor CPM by to obtain μSv/h

    Returns
    -------
    None.

    """
    
    ##############################################
    # variables : x → reference, y → measurement
    
    n = data['time'].size
    
    #times = np.array([d - data['takeoff'] for d in data['time']])
    
    reference = data['total-neutron']
     
    if calibration_factor > 0:
        measurement = data['cnt_1mn'] * calibration_factor
    
    measurement = data['cnt_1mn']
        
        
    ###############################################
    # linear regression from reference to measurement (beta should be ≈ 1)
    cov = np.sum((measurement - measurement.mean()) * (reference - reference.mean())) / n
    #corr = cov / np.sqrt(np.var(measurement) * np.var(reference))
        
    beta_hat = cov / np.var(reference)
    alpha_hat = measurement.mean() - beta_hat * reference.mean()
        
    measurement_hat = alpha_hat + beta_hat * reference
    
    e = measurement - measurement_hat
    
    k = 1
    
    ###############################################
    # Exploratory Data Analysis
    ESS = np.sum(e**2)
    TSS = np.sum((measurement - measurement.mean())**2)
    #RSS = np.sum((reference - measurement.mean())**2)
    
    R2 = 1 - ESS/TSS
    
    #MAE = np.sum(np.abs(e)) / n
    #MSE = ESS / (n-k-1)
    #RMSE = np.sqrt(MSE)
    #MAPE = 100 / n * np.sum(np.abs(e/measurement))
    #MSR = RSS / k
    
    #F = MSR / MSE
    #pr_F = 1 - f.cdf(F, k, n-k-1)
    
    print("\nmeasurement vs CARI-7A reference EDA:")
    #print(f"ρ: {corr:.4f}")
    print(f"R²: {R2:.4f}")
    #print(f"MAE: {MAE:.4f}")
    #print(f"MSE: {MSE:.4f}")
    #print(f"RMSE: {RMSE:.4f}")
    #print(f"MAPE: {MAPE:.2f}%")
    #print(f"F value: {F:.2f}")
    #print(f"Pr(>F): {pr_F:.4f} (smaller → better)")
    
    ###############################################
    
    #idx = np.random.choice(n, size=n//200, replace=False)

    
    # H₀: α = 0, β = 1
    # H₁: α > 0 or β > 1
    # measurement = α + β reference
    s = np.sqrt(np.sum((measurement - alpha_hat - beta_hat*reference)**2) / (n-k-1))
    Sxx = np.sum((reference - reference.mean())**2)
    
    SE_beta = s / np.sqrt(Sxx)
    SE_alpha = s * np.sqrt(1/n + reference.mean()**2/Sxx)
    
    #print()
    
    
    #test statistic
    #t_beta = (beta_hat - 1) / SE_beta
    #t_alpha = (alpha_hat - 0) / SE_alpha
    # n is large, so it tₙ₋₂ ~ z
    #beta_p_value =  1-t.cdf(t_beta, n-k-1)
    #alpha_p_value = 1-t.cdf(t_alpha, n-k-1)
    
    
    print(f"β: {beta_hat:.4f} ± {SE_beta:.2f}")#"    p-value: {beta_p_value:.4f}")
    print(f"α: {alpha_hat:.4f} ± {SE_alpha:.2f}")#"    p-value: {alpha_p_value:.4f}")
    
    if calibration_factor < 0:
        print(f"Estimated device calibration: {(beta_hat):.0f} CPM ≈ 1 μSv/h")
    
    return
