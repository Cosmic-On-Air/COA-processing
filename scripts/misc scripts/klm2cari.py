#######################################################################################################################
#
#Name: convert_your_data_COA.py
#
#Description:
        #This code takes the new .log file that was created by the analyse_your_data_COA.py code in "Part 2.1: Analyse your Data" 
        #and converts it into a format that CARI-7 can process.
        #It will output two files, one in which the total radiation and a second one in which the neutron contribution 
        #to the radiation will be returned after running them through CARI-7.
#
#Cosmic On Air (cosmic-on-air.org; cosmiconair@gmail.com)
#
#July 2024
#
#Contributors:
#C. Briand, Laboratory for Space Studies and Instrumentation in Astrophysics, Observatoire de Paris, France
#J. Trickett, Department of Physics, University of Cape Town, South Africa

#################################################################################################################
#Imports the necessary modules
import cosmic_on_air as coa

dest_file = "FLIGHT.LOC"

data = coa.read_flight_kml("FlightAware_KLM598_FACT_EHAM_20260116.kml")

date = [d.strftime('%Y/%m/%d') for d in data['time']]
hour = [str(data['time'][0].hour + 1) for d in data['time']]

lat_dir =["N" if d > 0 else "S" for d in data['lat']]
lon_dir =["E" if d > 0 else "W" for d in data['lon']]
lat = [f"{abs(d):.2f}" for d in data['lat']]
lon = [f"{abs(d):.2f}" for d in data['lon']]
alt = [f"{d/1000:.3f}" for d in data['alt']]

#Writes all the formatted information to the total and neutronradiation .LOC file        
with open(dest_file, "w") as f:
    f.write("START-------------------------------------------------\n")
    for i in range(len(lat)):
        #P0, specifies total radiation in CARI-7, P1 specifies neutron radiation
        f.write(f"C, spectrum for {lat_dir[i]}, {lat[i]}, {lon_dir[i]}, {lon[i]}, K, {alt[i]}, {date[i]}, H{hour[i]}\n")
        for j in range(0,38):
            f.write(f"{lat_dir[i]}, {lat[i]}, {lon_dir[i]}, {lon[i]}, K, {alt[i]}, {date[i]}, H{hour[i]}, D4, P{j}, C4, S0\n")
    f.write("STOP-------------------------------------------------\n")
