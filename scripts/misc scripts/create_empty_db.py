# -*- coding: utf-8 -*-
"""
Created on Fri Jan  9 11:21:52 2026

@author: aidan
"""

import cosmic_on_air_db as ca_db
import os

db = ca_db.CoaDatabase(os.getcwd(), new_db=True)
db.connect()
db.close()