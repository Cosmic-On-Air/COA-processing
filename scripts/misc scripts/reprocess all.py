# -*- coding: utf-8 -*-
"""
Created on Mon Feb  2 17:50:43 2026

@author: aidan
"""

import cosmic_on_air_db as coa_db
import os


database_path = os.path.join(os.getcwd(), "data_archive")

db = coa_db.CoaDatabase(database_path, show_figures=True)

ids = db.get_ids()

i = 0
total = len(ids)

for data_id in ids:
    i+=1
    if i < 25:
        continue
    print(f"Reprocessing {data_id}, {i}/{total}.")
    db.reprocess(data_id, prompt_confirm=False)
    print("Done")