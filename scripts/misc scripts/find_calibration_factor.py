# -*- coding: utf-8 -*-
"""
Created on Mon Feb  2 18:47:55 2026

@author: aidan
"""

import cosmic_on_air_db as coa_db
import cosmic_on_air as coa
import os
import numpy as np


database_path = os.path.join(os.getcwd(), "data_archive")

db = coa_db.CoaDatabase(database_path, show_figures=True)

entries = db.get_entries()

factors = {}

types = {"Safecast": [], "UCT": [], "Rium": [], "Radiacode": [], "GMC": []}
    

i=0

with open("Calibration factors.txt", "w", encoding="utf-8") as f:
    for entry in entries:
        data = coa.read_processed_log(os.path.join(database_path, entry[8]))
        
        if float(data['R2']) < 0.9:
            continue
        
        f.write(f"{coa.data_id(data)}: {data['scaling_factor']:.5f}\n")
        
        factor = float(data['scaling_factor'])
        
        if data['device_id'] in factors:
            factors[data['device_id']].append(factor)
        else:
            factors[data['device_id']]= [factor]
            
        for key in types:
            if key in data['device_id']:
                types[key].append(factor)  
                break
        else:
            print("New: ", data['device_id'])

for key in factors:
    temp = np.array(factors[key])
    factors[key] = [np.average(temp), np.std(temp), temp.size]
    
for key in types:
    temp = np.array(types[key])
    types[key] = [np.average(temp), np.std(temp), temp.size]
    
with open("Calibration factors.txt", "r", encoding="utf-8") as f:
    lines = f.read()

with open("Calibration factors.txt", "w", encoding="utf-8") as f:
    f.write("Factors per sensor type: \n")
    
    for key in types:
        f.write(f"{key}: {types[key][0]:.6f} ± {types[key][1]:.6f} (n={types[key][2]})\n")
    
    f.write("\nFactors for each specific detector:\n")
    
    for key in factors:
        f.write(f"{key}: {factors[key][0]:.6f} ± {factors[key][1]:.6f} (n={factors[key][2]})\n")
    
    f.write("\nFactors for each log file:\n")
        
    f.write(lines)
    