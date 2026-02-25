# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 15:40:57 2026

@author: aidan
"""

import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from pykml import parser

cari_out_file = "FLIGHT.ANS"
flight_file = "FlightAware_KLM598_FACT_EHAM_20260116.kml"

spectrum = {0: "total", 1: "neutrons", 2: "photons", 3: "electrons",
            4: "positrons", 5: "neg muons", 6: "pos muons", 7: "protons",
            8: "pos pions", 9: "neg pions", 10: "deuterons", 11: "tritons",
            12: "helions", 13: "alphas", 14: "lithium", 15: "beryllium",
            16: "boron", 17: "carbon", 18: "nitrogen", 19: "oxygen",
            20: "fluorine", 21: "neon", 22: "sodium", 23: "magnesium",
            24: "aluminum", 25: "silicon", 26: "phosphorus", 27: "sulphur",
            28: "chlorine", 29: "argon", 30: "potassium", 31: "calcium",
            32: "scandium", 33: "titanium", 34: "vanadium", 35: "chromium",
            36: "manganese", 37: "iron"}

####################################################################################################
def read_flight_kml(kml_filename):
    
    data = []
    
    # Open the file with mode errors="replace" (replaces undecodable characters with �)
    with open(kml_filename, "r", encoding="utf-8", errors="replace") as f:
        doc = parser.parse(f).getroot()
        
    # Extract flight time data
    for e in doc.Document.Placemark[2].findall(".//{http://www.opengis.net/kml/2.2}when"):
        data.append(datetime.strptime(e.text,"%Y-%m-%dT%H:%M:%SZ"))
    
    
    return np.array(data)

####################################################################################################
def read_cari_ans(cari_out_file):
    output = {}
    for key in spectrum:
        output[spectrum[key]] = []

    output['lat'] = []
    output['lon'] = []
    output['alt'] = []

    with open(cari_out_file, "r") as f:
        f.readline()
        for line in f:
            columns = line.lower().split(",")
            columns = [column.strip() for column in columns]
            
            if columns[0] == "c": continue
        
            
            added = False
            for key in spectrum:
                if columns[7] == spectrum[key]:
                    added = True
                    
                    if (len(output['lat']) == 0 or
                        output['lat'][-1] != columns[0] or
                        output['lon'][-1] != columns[1] or
                        output['alt'][-1] != columns[2]):
        
                        output['lat'].append(columns[0])
                        output['lon'].append(columns[1])
                        output['alt'].append(columns[2])
                        for k in spectrum:
                            output[spectrum[k]].append("nan")
                    output[spectrum[key]][-1] = columns[8]
                    
                    continue
                
            if added == False:
                print(columns)
                raise Exception("Could not identify column")

    for key in output:
        output[key] = np.array([float(d) for d in output[key]])
        
    return output

####################################################################################################

kml_times = read_flight_kml(flight_file)
times = [(d - kml_times[0]).total_seconds() / 3600 for d in kml_times]

output = read_cari_ans(cari_out_file)
output['times'] = times

for key in output:
    if key == 'times' or key =='lat' or key == 'lon' or key == 'alt':
        continue
    plt.scatter(output['times'], output[key], label=key)
    
plt.xlabel("Flight time (hours)")
plt.ylabel("Dose rate (μSv/h)")
plt.legend(fontsize=5, loc="right")
plt.title("Dose Rate vs flight time")