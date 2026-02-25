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

factors = []

i=0

for entry in entries:
    data = coa.read_processed_log(os.path.join(database_path, entry[8]))
    
    if data['timestamps'] == "original":
        continue
    
    i += 1
    
    print(f"Reprocessing {entry[0]}, {i}.")
    
    db.reprocess(entry[0], prompt_confirm=False)
    print("Done")