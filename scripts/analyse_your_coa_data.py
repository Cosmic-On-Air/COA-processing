"""
Name: analyse_your_coa_data.py
Description:
    *   This script is designed to interact with the cosmic_on_air function library
    *   It facilitates collected data processing and requires minimal modification by the user
    *   To ensure full use of features, download the CARI-7A software and extract it into the
        same folder as this script.
        https://www.faa.gov/data_research/research/med_humanfacs/aeromedical/radiobiology/cari7
    *   Additionally, ensure that cosmic_on_air.py is in the same folder as this script

Cosmic On Air (cosmic-on-air.org; cosmiconair@gmail.com)

Version: 28 Nov 2025

Contributors:
C. Briand, Laboratory for Space Studies and Instrumentation in Astrophysics, Observatoire de Paris, France
J. Trickett, Department of Physics, University of Cape Town, South Africa
A. Gebbie, Department of Physics, University of Cape Town, South Africa

What to edit (for users):
    1.  Ensure that all required python libraries are installed
        (os, tempfile, shutil, datetime, time, numpy, subprocess, pykml, plotly, matplotlib, cartopy, airportsdata)
    1.  Copy the absolute path to your data file (e.g. .log/.csv) and paste it between the
        quotation marks at the line data_file = r"" (line 44)
    2.  Copy the absolute path to your flight data file (e.g. .kml/.csv) and paste it between
        the quotation marks a the line flight_file = r"" (line 45)
    3.  Ensure that the CARI_7A_DVD folder is in the same folder as this code.
    4.  Run the code.
    5.  Wait for it to complete; a figure should launch in your default webbrowser summarising all
        your data.
    6.  The figure .html and processed .log files should also appear in this folder.
        
    Feel free to try figure out some of the code!
"""

import cosmic_on_air as ca
import plotly.io as pio
import os
pio.renderers.default = 'browser'

####################################################################################################
#Path to FlightAware and raw data files
#Change these to match your own directories and file names
data_file = r""
flight_file = r""


# Create data dictionary using files given
data = ca.find_processed(data_file)
#if data is None:
#    data = ca.read_raw_log(data_file, flight_file, time_delta=5)

plot_title = (f"{data['origin']} to {data['destination']}" +
              f" - {data['date'].strftime('%d/%m/%Y')} - Detector {data['device_id']}")
print(plot_title)

fig = ca.plotly_plot(data)
fig.show()

#Creates the new .log file to store all the plotted and adjusted parameters
file_path = os.path.dirname(data_file)
file_name, ext = os.path.splitext(os.path.basename(data_file))
file_name = "".join(char for char in file_name if char.isdecimal())
new_file = os.path.join(file_path, f"Processed_data_{file_name}.log")
fig_file = os.path.join(file_path, f"Figure_{file_name}.html")

ca.write_newlog(data, new_file)
fig.write_html(fig_file)