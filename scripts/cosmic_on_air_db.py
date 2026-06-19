"""
Name: cosmic_on_air_db.py
Description:
    *   This code provides a class to interact with a database of radiation measurements on a flight
    *   It provides basic methods to interact with the SQL .md file that stores the metadata to the 
        file archive.
    *   It additionally has a Command Line Interface script at the end of the program, allowing one
        to use this script to interact with the SQL database.
        
    *   To ensure full use of features, download the CARI-7A software and extract it into the
        same folder as this script.
        https://www.faa.gov/data_research/research/med_humanfacs/aeromedical/radiobiology/cari7
    
Structure and format of the database:
    *   a coa.db file is found in the database folder. It follows an SQLite database format
        and can be editted by the code, or even directly using software such as SQLiteStudio
    *   data measurements in a folder structure following this format:
        > data_id     (unique id in the format FLIGHTNUMBER YYYY-MM-DD detector_id)
             |>  Data data_id.log
             |>  backup             (folder containing original data)
                     |> *flight_file*
                     |> *log_file*
    
    *   It is encouraged that the user make frequent manual backups of the database to avoid risk of 
        loss of data.

Cosmic On Air (cosmic-on-air.org; cosmiconair@gmail.com)

Version: 19 Jun 2026

Contributors:
A. Gebbie, Department of Physics, University of Cape Town, South Africa
"""

##############################################
# Import required classes
import sqlite3
import cosmic_on_air as coa
import stat
import airportsdata
import plotly.io as pio
from pathlib import Path
from iso3166 import countries # to retrieve country name since airportsdata only stores country code
from datetime import datetime, UTC

BASE_DIR = Path(__file__).parent.resolve()

def _rmtree(root):
    """
    Function to delete a file tree. It ensures folders are writable before deleting

    Parameters
    ----------
    root : Path object of root folder
    """
    root.chmod(stat.S_IWRITE) # make sure file is writable
    for p in root.iterdir():
        if p.is_dir():
            _rmtree(p)
        else: p.unlink()
    root.rmdir()

class CoaDatabase:
    def __init__(self, path, new_db=False, show_figures=True, include_plotlyjs=True, show_progress=True):
        """
        Constructor for the database object.

        Parameters
        ----------
        path : Path to the database folder (not including the .md file)
        
        new_file : Allow for creation of a new database as the provided path
            (default:False to prevent accidental creation).
        
        show_plots : Show figures of entries that are interacted with
            default:True shows figure after add/update/search/etc in webbrowser).
    
        include_plotlyjs : Include the interactable plotly script in the file
            (default=True, to reduce html file size by 3MB use "cdn", 
             this fetches it online (see plotly write_html for details))
        """
        path = Path(path)
        database_file = path / "coa.db"
    
        if not new_db and not database_file.is_file():
            raise ValueError("Invalid path to database")
        
        pio.renderers.default = 'browser'
        
        self.db = database_file
        self.db_path = path
        self.conn = None
        self.show = show_figures
        self.include_plotlyjs = include_plotlyjs
        self.show_progress= show_progress

    def connect(self):
        """
        Connect to the object's database and return the cursor.

        Returns
        -------
        cursor : cursor object to interact with the database.

        """
        # Connect to database (creates file if it doesn't exist)
        connection = sqlite3.connect(self.db)
        cursor = connection.cursor()
        
        # Create airports table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS airports (
            icao TEXT PRIMARY KEY,
            iata TEXT,
            name TEXT,
            city TEXT,
            country TEXT
        )
        """)
    
        # Create flights table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS flights (
            data_id TEXT PRIMARY KEY,
            detector_id TEXT,
            flight_number TEXT,
            departure_airport TEXT NOT NULL,
            arrival_airport TEXT NOT NULL,
            departure_time TEXT,
            arrival_time TEXT,
            reference_R2 TEXT,
            data_file TEXT NOT NULL,
            old_log TEXT,
            old_flight TEXT,
            citizen_id TEXT,
            submission_date TEXT,
            FOREIGN KEY (departure_airport) REFERENCES airports(icao),
            FOREIGN KEY (arrival_airport) REFERENCES airports(icao)
        )
        """)
        
        self.conn = connection
        
        return cursor

    def close(self):
        """
        Closes the objects connection with the database, releasing the file.
        """
        self.conn.close()
        
    def commit(self):
        """
        Commit a change to the database. Changes are not permanent until they are committed.
        """
        self.conn.commit()
        
    def rollback(self):
        """
        Rollback a change to the database. Changes can be rolled back until the commit method is called.
        """
        self.conn.rollback()
        
    def get_ids(self):
        """
        Get a list of all data_id keys in the database.
        
        Returns
        -------
        keys : a list of all the unique data_id keys in the database.
        """
        cursor = self.connect()
        try:
            cursor.execute("SELECT data_id FROM flights")
            rows = cursor.fetchall()
        finally:
            self.close()
        
        return [d[0] for d in rows]
    
    def get_entry(self, data_id):
        """
        Get the full entry of a given data_id.
        
        Parameters
        ----------
        data_id : string of the unique data_id of the entry being searched for.
        
        Returns
        -------
        entry : a tuple of the entry.
        """
        cursor = self.connect()
        try:
            cursor.execute("SELECT * FROM flights WHERE data_id = ?", (data_id,))
            entry = cursor.fetchall()
        finally:
            self.close()
        
        return entry
    
    def get_entries(self):
        """
        Get a list of all full entries in the database.
        
        Returns
        -------
        entries : a list of all the unique tuples of entries in the database.
        """
        cursor = self.connect()
        try:
            cursor.execute("SELECT * FROM flights")
            rows = cursor.fetchall()
        finally:
            self.close()
        
        return rows
    
    def get_all_data(self):
        """
        Get a list of all full flight datasets in the database.
        
        Returns
        -------
        entries : a list of all the dictionaries of data in the database.
        """
        
        entries = self.get_entries()
        
        data = []
        
        for entry in entries:            
            file = self.db_path / entry[8]
            data.append(coa.read_processed_log(file))
        
        return data
    
    def plot_all_data(self, figure_dest=""):
        """
        Get a plotly world map plot of all the data in the database (that have scaling factors)
        
        Returns
        -------
        fig : a list of all the dictionaries of data in the database.
        """
        
        fig = coa.plot_all(self.get_all_data())
        figure_dest = Path(figure_dest)
        
        if self.show:
            fig.show()    
            
        if figure_dest != Path("") and figure_dest.is_dir():
            filename = "all_flights.html"
            print(f"Figure saved as '{filename}'")
            fig.write_html(figure_dest / filename, include_plotlyjs=self.include_plotlyjs)
        
        return fig
        
    def search(self, keywords, exact=False):
        """
        Perform a search on the database to find entries which match the provided keywords.

        Parameters
        ----------
        keywords : A dictionary of all the keywords. Not all keywords are required to be provided.
            Valid keys to search with are:
        *   'detector_id' : the name of the measuring detector (e.g. Safecast 1124) 
        *   'flight_number' : the name of the flight (e.g. AFR995)
        *   'fit' : How well the measurement fits the CARI-7A reference (number between 0.0 and 1.0)
        *   'dept_airport' : ICAO/IATA/city-name of the departing airport
        *   'dest_airport' : ICAO/IATA/city-name of the destination airport
        *   'takeoff' : takeoff time YYYY-MM-DD hh:mm:ss
        *   'landing' : landing time YYYY-MM-DD hh:mm:ss
        
        exact : Boolean value. If False, the method will search for substrings that match instead of
            exact keyword matches. (e.g. 2025-02 will find all flights in february 2025 if exact=False)

        Returns
        -------
        items : A list of tuples of the metadata for matching flights, including:
            unique data_id id, detector id, flight number, 
            origin ICAO, destination ICAO, takeoff time, landing time, reference fit,
            relative path to processed data, relative path to raw data,
            relative path to flight data, additional airport information

        """
        for key in ['detector_id', 'flight_number', 'fit',
                    'dept_airport', 'dest_airport', 'takeoff', 'landing']:
            if key not in keywords:
                keywords[key] = "%"
        
        if not exact:
            for key in keywords:
                keywords[key] = "%" + keywords[key] + "%"
                
        icao = airportsdata.load("ICAO")
        iata = airportsdata.load("IATA")
        
        dept_type = ""
        dest_type = ""
        
        if keywords['dept_airport'] in icao:
            dept_type = "icao"
        elif keywords['dept_airport'] in iata:
            dept_type = "iata"
        else:
            dept_type = "city"
            
        if keywords['dest_airport'] in icao:
            dest_type = "icao"
        elif keywords['dest_airport'] in iata:
            dest_type = "iata"
        else:
            dest_type = "city"
            
        query = f"""
        SELECT *
        FROM flights f
        JOIN airports o ON o.icao = f.departure_airport
        JOIN airports d ON d.icao = f.arrival_airport
        WHERE f.detector_id LIKE ?
          AND f.flight_number LIKE ?
          AND f.departure_time LIKE ? 
          AND f.arrival_time LIKE ?
          AND f.reference_R2 LIKE ?
          AND o.{dept_type} LIKE ?
          AND d.{dest_type} LIKE ?;
        """
        
        cursor = self.connect()
        try:
            # Query: list all flights with departure/arrival airport names
            cursor.execute(query, 
                           (keywords['detector_id'], keywords['flight_number'], 
                            keywords['takeoff'], keywords['landing'], keywords['fit'],
                            keywords['dept_airport'], keywords['dest_airport']))
            
            items = cursor.fetchall()
        finally:
            self.close()
            
        return items
    
    def get_data(self, keywords, exact=False):
        """
        get the data dictionary of the first flight matching the keywords.
        If show_figures=True on construction, figures will be shown in browser.

        Parameters
        ----------
        keywords : dictionary of keywords following rules of method 'search'
            alternatively it can be a string of the data id
        
        exact : only search exact match for keywords (default=False)
        
        Returns
        -------
        data : the data dictionary for the first flight found matching the keywords

        """
        if type(keywords) == str:
            items = self.get_entry(keywords)
        else:
            items = self.search(keywords, exact)
        
        if not items:
            print("Nothing found.")
            return None
        
        file = self.db_path / items[0][8]
        data = coa.read_processed_log(file)
        
        return data
    
    def add(self, log_file, 
            flight_file="", 
            detector="UNKNOWN", 
            detector_serial="UNKNOWN", 
            citizen_id="UNKNOWN", 
            submission_date=None, 
            parallel=8, 
            time_delta=-1, 
            raise_duplicate=True):
        """
        Method to add a new detector measurement to the database.
        
        Parameters
        ----------
        log_file: Absolute path to the detector log file to add to the database.
        
        flight_file : Absolute path to the flight ADS-B file to add to the database
            (default: ""; can be left blank, and read_raw_log function will instead use detector gps)
            
        detector : string name of the detector
            (default: "UNKNOWN")
        
        detector_serial : string serial number of the detector
            (default: "UNKNOWN")
        
        citizen_id : identity of the individual who submitted the data
            (default: "UNKNOWN")
            
        parallel: Number of parallel instances of CARI-7A software to run to speed up reference data
            compute speed (default: 8 threads)
            
        time_delta : default=-1. If greater than 0, the software will attempt to recover corrupted timestamps in data
            the value it is set to will be the delta time used between measurements if the end timestamp is corrupted.
        
        raise_duplicate : default=True. If true, it will raise an error if the flight is already in the database,
            else it will silence the error, not modify the database, but still return the processed data dictionary.
        
        Returns
        -------
        flight : tuple of values for the data details that is added to coa.db SQL
        
        data : dictionary of values of the processed radiation data
        
        unique : True if the data was new and hence added to the database, otherwise false.
        """
        
        log_file = Path(log_file)
        if not log_file.is_file():
            raise IOError("log file does not exist")
        flight_file = Path(flight_file)
        
        data = coa.read_raw_log(log_file, flight_file, detector=detector, detector_serial=detector_serial, citizen_id=citizen_id, submission_date=submission_date, parallel=parallel, time_delta=time_delta, show_progress=self.show_progress)
        
        if self.show:
            coa.plotly_plot(data).show()
        
        data_id = coa.data_id(data)
        
        airports = airportsdata.load("ICAO")
        dept = airports.get(data['origin ICAO'])
        dest = airports.get(data['destination ICAO'])
        
        # generate absolute paths to use to create archive entry
        folder = self.db_path / data_id
        backup_folder = folder / "backup"
        
        backup_log = backup_folder / log_file.name
        if flight_file != Path(""):
            backup_flight = backup_folder / flight_file.name
        else:
            backup_flight = Path("")
            
        new_log = folder / f"Data {data_id}.log"
        
        # Relative paths to store as metadata in SQL database
        rel_path = f"{data_id}/Data {data_id}.log"
        old_log = f"{data_id}/backup/{log_file.name}"
        if flight_file != Path(""):
            old_flight = f"{data_id}/backup/{flight_file.name}"
        else:
            old_flight = ""
        
        # add info to .db file
        cursor = self.connect()
        
        unique = True # boolean indicator that entry is new to database
        
        try:
            # add entries to database
            airports = [
                (dept['icao'], dept['iata'], dept['name'], dept['city'], countries.get(dept['country']).name),
                (dest['icao'], dest['iata'], dest['name'], dest['city'], countries.get(dest['country']).name)
            ]
            cursor.executemany("INSERT OR IGNORE INTO airports VALUES (?, ?, ?, ?, ?)", airports)
            
            flight = (data_id, 
                      data['detector'] + " " + data['detector_serial'], 
                      data['flight_number'], 
                      dept['icao'],
                      dest['icao'], 
                      data['takeoff'].strftime("%Y-%m-%d %H:%M:%S"), 
                      data['landing'].strftime("%Y-%m-%d %H:%M:%S"),
                      data['R2'], 
                      rel_path, old_log, old_flight,
                      citizen_id,
                      data['submission_date']
            )
            
            cursor.execute("INSERT INTO flights VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", flight)
            
            # create folder and copy files into archive
            backup_folder.mkdir(parents=True)
            log_file.copy(backup_log)
            if flight_file != Path("") and flight_file.is_file():
                flight_file.copy(backup_flight)
            coa.write_newlog(data, new_log)
            
            self.commit()
        except sqlite3.IntegrityError:
            self.rollback()
            unique = False
            if raise_duplicate:
                raise
        except Exception:
            self.rollback()   # Undo all changes since the transaction began
            raise
        finally:
            self.close()
        
        return flight, data, unique # returns entry as well
             
    def reprocess(self, data_id, prompt_confirm=True, time_delta=-1, disable_cari_weather=True):
        """
        Reprocess data in the database by creating the processed data from scratch from the
        data and flight files stored in the database (internal) backup folder.
        
        A time delta can be provided to attempt to fix corrupt log times.
        In extreme cases, one might have to manually edit the corrupted data files to attempt
        to recover data before reprocessing.
        
        Parameters
        ----------
        prompt_confirm : If true (default), the method will request a confirmation from the console
            to reprocess. This exists to prevent accidental modification of data.
            
        time_delta : default=-1. If greater than 0, the software will attempt to recover corrupted timestamps in data
            the value it is set to will be the delta time used between measurements if the end timestamp is corrupted.
        
        subsample : number of points to subsample when using the CARI software, significantly reduces time taken at
            minimal loss in precision
        """
        
        cursor = self.connect()
        
        cursor.execute("SELECT * FROM flights WHERE data_id LIKE ?", (data_id,))
        
        item = cursor.fetchone()
        
        if not item:
            print("Nothing found")
            return
        
        if prompt_confirm:
            confirm = input(f"Confirm reprocess {item[0]}. Enter Y/n.\n")
        
            if not confirm == "Y":
                print("Cancelled.")
                return
            
        # delete old log file
        (self.db_path / item[8]).unlink(missing_ok=True)
        
        # Get locations of flight files
        old_folder = (self.db_path / item[8]).parent
        raw_log = self.db_path / item[9]
        raw_flight = self.db_path / item[10]
        
        if item[10] == "" or item[10] == None:
            takeoff = item[5]
            landing = item[6]
            if len(takeoff) == 16: # backwards compatibility, append seconds
                takeoff += ":00"
            if len(landing) == 16: # backwards compatibility, append seconds
                landing += ":00"
                
            # takeoff, landing, flight_number
            raw_flight = takeoff + "," + landing + "," + item[2]
            
        citizen_id = item[11]
        if item[11] == "" or item[11] == None:
            citizen_id = "UNKNOWN"
            
        try:
            submission_date = datetime.strptime(item[12], "%Y-%m-%dT%H:%M:%SZ")
        except:
            submission_date = None
        
        detector = item[1]
        splitup = detector.strip().split(" ")
        serial = splitup[-1]
        detector = "".join(splitup[:-1])
            
        # Reprocess the data
        data = coa.read_raw_log(raw_log, raw_flight, detector=detector, detector_serial=serial, citizen_id=citizen_id, submission_date=submission_date, parallel=8, time_delta=time_delta, disable_cari_weather=disable_cari_weather, show_progress=self.show_progress)
        data_id = coa.data_id(data)
        
        # update all file and folder names
        new_log = self.db_path / data_id / f"Data {data_id}.log"
        rel_path = f"{data_id}/Data {data_id}.log"
        new_folder = self.db_path / data_id
        
        if old_folder != new_folder:
            old_folder.rename(new_folder)
        
        raw_log = f"{data_id}/backup/{raw_log.name}"
        raw_flight = f"{data_id}/backup/{raw_flight.name}"
        if item[10] == "":
            raw_flight = ""
        
        if self.show:
            coa.plotly_plot(data).show()    
        
        try:
            # Save the new data
            coa.write_newlog(data, new_log)
            
            # Update the R² value in the .md table; and update the file names (to update older versions)
            cursor.execute("DELETE FROM flights WHERE data_id = ?", (item[0],))
            
            # add updated entry to database
            airports = airportsdata.load("ICAO")
            dept = airports.get(data['origin ICAO'])
            dest = airports.get(data['destination ICAO'])
            
            airports = [
                (dept['icao'], dept['iata'], dept['name'], dept['city'], countries.get(dept['country']).name),
                (dest['icao'], dest['iata'], dest['name'], dest['city'], countries.get(dest['country']).name)
            ]
            cursor.executemany("INSERT OR IGNORE INTO airports VALUES (?, ?, ?, ?, ?)", airports)
            
            flight = (data_id, 
                      data['detector'] + " " + data['detector_serial'], 
                      data['flight_number'], 
                      dept['icao'],
                      dest['icao'], 
                      data['takeoff'].strftime("%Y-%m-%d %H:%M:%S"), 
                      data['landing'].strftime("%Y-%m-%d %H:%M:%S"),
                      data['R2'], 
                      rel_path, raw_log, raw_flight,
                      data['citizen_id'],
                      data['submission_date']
            )
            
            cursor.execute("INSERT INTO flights VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", flight)
        
            # Save changes and close
            self.commit()
            
        except Exception:
            self.rollback()   # Undo all changes since the transaction began
            raise
        finally:
            self.close()
            
        print(f"Reprocessed {data_id}.")
    
    def delete(self, data_id, prompt_confirm=True):
        """
        Delete the entry from the database with the matching data_id unique key.
        
        Parameters
        ----------
        data_id : unique key of the measurement to delete.
        
        prompt_confirm : If true (default), the method will request a confirmation from the console
            to delete. This exists to prevent accidental loss of data.
            
        """
        cursor = self.connect()
        
        cursor.execute("SELECT * FROM flights WHERE data_id LIKE ?", (data_id,))
        
        item = cursor.fetchone()
        
        if not item:
            print("Nothing found")
            return
        
        if prompt_confirm:
            confirm = input(f"Confirm delete {item[0]}. Enter YES/n.\n")
            
            if not confirm == "YES":
                print("Cancelled.")
                return
        
        try:
            folder = (self.db_path / item[8]).parent
            
            # function to delete tree of folder (incase there are other files/folders in the folder)
            if folder.is_dir(): _rmtree(folder)
            
            cursor.execute("DELETE FROM flights WHERE data_id = ?", (item[0],))
            
            self.commit()
            
            print(f"Deleted {item[0]}.")
        except Exception:
            self.rollback()   # Undo all changes since the transaction began
            raise
        finally:
            self.close()

    def find_and_plot(self, keywords, exact=False, same_figure=False, figure_dest="", max_plot=10):
        """
        Find and plot figures of all flights matching keywords.
        If show_figures=True on construction, figures will be shown in browser.

        Parameters
        ----------
        keywords : dictionary of keywords following rules of method 'search'
        
        exact : only search exact match for keywords (default=False)
        
        same_figure : If true, program will attempt to plot all figures on one graph.
            Note, cosmic_on_air.py can throw an error if data from different flights
            are attempted to be plotted on same graph.
            
        figure_dest : Destination folder to save resulting figure to.
            If blank ("") figure won't be saved

        max_plot : Maximum number of figures to attempt to plot. This limit is put
            in place to avoid trying to plot every single figure in the database,
            causing a possible crash to the computer. The default max is 10.

        Returns
        -------
        figs : If same_figure=False, list of plotly figure objects, 
            or if same_figure=True, a plotly figure object.

        """
        figure_dest = Path(figure_dest)
        items = self.search(keywords, exact)
        
        if not items:
            print("Nothing found.")
            return
        
        i = 0
        
        data = []
        
        show_all = False
        
        for item in items:
            print(item[0].strip())
            
            file = self.db_path / item[8]
            data.append(coa.read_processed_log(file))
            
            if i > max_plot and not same_figure and not show_all:
                print("\nOnly showing first 10 items.")
                confirm = input(f"Would you like to see all {len(items)} items? Y/n\n")
                if confirm != "Y":
                    break
                show_all = True
            
            i += 1
          
        figs = []
        
        if same_figure:
            figs = coa.plotly_plot(data)
            if self.show:
                figs.show()
            
            if figure_dest != Path("") and figure_dest.is_dir():
                filename = ("combined_fig_" + 
                            data[0]['flight_number'] + "_" + 
                            data[0]['date'].strftime("%Y-%m-%d") + ".html")
                print(f"Figure saved as '{filename}'")
                figs.write_html(figure_dest / filename, include_plotlyjs=self.include_plotlyjs)
        
        else:
            for datum in data:
                fig = coa.plotly_plot(datum)
                figs.append(fig)
                
                if self.show:
                    fig.show()
                    
                data_name = coa.data_id(datum).replace(" ", "_")
                
                if figure_dest != Path("") and figure_dest.is_dir():
                    filename = f"Figure_{data_name}.html"
                    print(f"Figure saved as '{filename}'")
                    fig.write_html(figure_dest / filename, include_plotlyjs=self.include_plotlyjs)
        
        return figs
        
    def export_db(self, keywords, destination_path, include_html=False):
        """
        Export the selected flights to a destination folder, copying the data from
        the database as is, and including a figure .html file as well.
        
        An 'export' folder will be created at the destination path, and will contain
        a subfolder for each flight. If an export folder with flights already exists,
        those flights may be overwritten.
        
        Parameters
        ----------
        keywords : dictionary of search keywords following rules of 'search' method
        
        destination_path : absolute path to destination folder.
            Note : The program raise an error if the destination path doesn't exist.
        """
        items = self.search(keywords)
        
        if not items:
            print("Nothing found")
            return
            
        destination_path = Path(destination_path) / "export"
        
        # intentionally only allow for one level of folder creation to minimize damage from error
        # due to incorrect input path
        destination_path.mkdir(exist_ok=True, parents=False)
            
        db = CoaDatabase(destination_path, show_figures=False, new_db=True)
        
        cursor = db.connect()
        
        for item in items:
            print(item[0].strip())
            # create desti_folder early since error handling requires its existance
            dest_folder = Path("")
            
            file = self.db_path / item[8]
            data = coa.read_processed_log(file)
            
            fig = coa.plotly_plot(data)
            
            if self.show:
                fig.show()
            
            # create destination path names
            data_id = coa.data_id(data)
            dest_folder = destination_path / data_id
            
            backup_log = self.db_path / item[9]
            backup_flight = self.db_path / item[10]
            
            data_file = dest_folder / f"Data {data_id}.log"
            fig_file =  dest_folder / f"Figure {data_id}.html"
            new_backup_log = dest_folder / "backup" / backup_log.name
            new_backup_flight = dest_folder / "backup" / backup_flight.name
            
            # if there is an existing export by the same name in the folder, delete it
            if dest_folder.is_dir(): _rmtree(dest_folder) #ensure to delete all subfolders
            
            dest_folder.mkdir()
            
            # finally export data
            coa.write_newlog(data, data_file)
            if include_html:
                fig.write_html(fig_file, include_plotlyjs=self.include_plotlyjs)
            
            # copy backup data
            new_backup_log.parent.mkdir() # make /backup folder
            backup_log.copy(new_backup_log)
            backup_flight.copy(new_backup_flight)
            
            try:
                # add updated entry to database
                flight = item [0:13]
                airports = [item[13:18], item[18:]]
                
                cursor.executemany("INSERT OR IGNORE INTO airports VALUES (?, ?, ?, ?, ?)", airports)
                         
                cursor.execute("DELETE FROM flights WHERE data_id = ?", (item[0],))
                cursor.execute("INSERT INTO flights VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", flight)
            
                # Save changes and close
                db.commit()
                
            except Exception:
                db.rollback()   # Undo all changes since the transaction began
                db.close()
                if dest_folder != Path("") and dest_folder.is_dir(): _rmtree(dest_folder)
                raise
        db.close()

    def import_db(self, import_path, reprocess=False):
        """
        Import flights from the selected external database, copying the data as is from
        the other database into this one.
        
        Optionally, data can then all be reprocessed to ensure it is in an up to date format.
        
        Parameters
        ----------
        import_path : absolute path to folder of the database to import from.
        
        reprocess : (default=False) If true, entries will all be reprocessed after they
            are imported.
        """
        
        # get list of items from other database
        
        db = CoaDatabase(import_path, show_figures=False)
        items = db.get_entries()
        
        # identify items already in the main database
        existing_items = self.get_ids()
        for i in range(len(items) - 1, -1, -1):
            if items[i][0] in existing_items:
                del items[i]
        
        if not items:
            print("No new entries in import database...")
            return
        
        airports = airportsdata.load("ICAO")
        
        for item in items:            
            print(item[0].strip())
            # create desti_folder early since error handling requires its existance
            dest_folder = Path("")
            
            file = db.db_path / item[8]
            data = coa.read_processed_log(file)
            
            # create destination path names
            data_id = coa.data_id(data)
            
            dest_folder = self.db_path / data_id
            dest_backup = dest_folder / "backup"
            
            backup_log = db.db_path / item[9]
            backup_flight = db.db_path / item[10]
            
            new_backup_log = dest_backup / backup_log.name
            new_backup_flight = dest_backup / backup_flight.name
            
            data_file = dest_folder / f"Data {data_id}.log"
            
            rel_path = f"{data_id}/Data {data_id}.log"
            raw_log = f"{data_id}/backup/{backup_log.name}"
            raw_flight = f"{data_id}/backup/{backup_flight.name}"
            
            dept = airports.get(data['origin ICAO'])
            dest = airports.get(data['destination ICAO'])
            
            # create flight folder
            if dest_folder.is_dir(): _rmtree(dest_folder)
            dest_folder.mkdir()
            dest_backup.mkdir()
                
            # finally import data
            coa.write_newlog(data, data_file)
            backup_log.copy(new_backup_log)
            backup_flight.copy(new_backup_flight)
            
            try:
                # add updated entry to database
                airport_data = [
                    (dept['icao'], dept['iata'], dept['name'], dept['city'], countries.get(dept['country']).name),
                    (dest['icao'], dest['iata'], dest['name'], dest['city'], countries.get(dest['country']).name)
                ]
                
                flight = (data_id, 
                          data['detector'] + " " + data['detector_serial'], 
                          data['flight_number'], 
                          dept['icao'],
                          dest['icao'], 
                          data['takeoff'].strftime("%Y-%m-%d %H:%M:%S"), 
                          data['landing'].strftime("%Y-%m-%d %H:%M:%S"),
                          data['R2'], 
                          rel_path, raw_log, raw_flight,
                          data['citizen_id'],
                          data['submission_date']
                )
                
                cursor = self.connect()
                
                cursor.executemany("INSERT OR IGNORE INTO airports VALUES (?, ?, ?, ?, ?)", airport_data)
                         
                cursor.execute("DELETE FROM flights WHERE data_id = ?", (item[0],))
                cursor.execute("INSERT INTO flights VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", flight)
            
                # Save changes and close
                self.commit()
                
            except Exception:
                self.rollback()   # Undo all changes since the transaction began
                if dest_folder != Path("") and dest_folder.is_dir(): _rmtree(dest_folder)
                raise
            finally:
                self.close()
            
            if self.show:
                fig = coa.plotly_plot(data)
                fig.show()
        
        # Finally, reprocess the data if needed.
        if reprocess:
            i = 0
            total = len(items)
            
            for item in items:
                i += 1
                print(f"Reprocessing {data_id}, {i}/{total}.")
                self.reprocess(item[0], prompt_confirm=False)

##########################################################################################
# Below is a Command Line Interface script to interact with the database from a console.
# It will execute of the program is run directly.
# If won't execute if the cosmic_on_air_db.py program is instead imported into another 
#   python script.

if __name__ == "__main__":
    # Find the location of the database; first check the current working directry
    database_file = BASE_DIR / "data_archive" / "coa.db"
    while True:
        if database_file.is_file():
            print("Found database at", database_file)
            break
        database_file = Path(input("What is the absolute path to the database .db file?\n"))
    
    database_path = database_file.parent
        
    db = CoaDatabase(database_path, show_figures=True, show_progress=True)
        
    # CLI for user to interact with database
    while True:
        print("\nWelcome to the Cosmic on Air database CLI query program.")    
        
        print("Enter a number to select an option:")
        print("1. List all entries in the archive.")
        print("2. Plot all entries on world map.")
        print("3. Search keyword and plot entries.")
        print("4. Search keyword and plot entries to same axis.")
        print("5. Export database.")
        print("6. Import database.")
        print("7. Add a data file.")
        print("8. Reprocess a data file.")
        print("9. Delete a data file.")
        print("q. Quit.")
        
        number = input()
        
        print()
        
        # quit
        if number == "q":
            break
        
        if not number.isdecimal():
            print("Please enter a valid number")
            continue
        
        number = int(number)
        
        # List all database entries
        if number == 1:
            ids = db.get_ids()
            for x in ids:
                print(x)
            
            print(f"{len(ids)} entries in archive.")
            
        if number == 2:
            figure_dest = Path(input("Save figure to folder: (If left blank, figure won't be saved):\n").strip("'\""))

            db.plot_all_data(figure_dest=figure_dest)
        
        # Perform a search to plot figures or export
        if number == 3 or number == 4 or number == 5:
            print("For each category, enter keyword, OR enter nothing to skip")
            detector_id = input("detector id: ").strip()
            flight_number = input("flight number: ").strip()
            dept = input("departure city: ").strip()
            dest = input("destination city: ").strip()
            date = input("Flight date (YYYY-MM-DD): ").strip()
            fit = input("Fit of measured to CARI data [0.0,1.0]: ").strip()
            
            keywords = {'detector_id': detector_id, 'flight_number': flight_number, 'fit': fit,
                        'dept_airport': dept, 'dest_airport': dest, 'takeoff': date}
            
            # plot figures
            if number == 3 or number == 4:
                figure_dest = Path(input("Save figure to folder: (If left blank, figure won't be saved):\n").strip("'\""))
                
                db.find_and_plot(keywords, same_figure=(number==4), figure_dest=figure_dest)
            
            # export
            if number == 5:
                while True:
                    dest_path = Path(input("What folder would you like to copy the data to?\n"))
                    if not dest_path.is_dir():
                        print("Please provide an existing folder")
                        continue
                    break
                
                db.export_db(keywords, dest_path)
                
        # export
        if number == 6:
            while True:
                import_path = Path(input("What is the path to the database to import files from?\n"))
                reprocess = input("Do you want to reprocess the data after importing? Y/n\n")
                reprocess = (reprocess=="Y")
                if not import_path.is_dir():
                    print("Please provide an existing folder")
                    continue
                break
            
            db.import_db(import_path, reprocess=reprocess)
                
        # add a measurement to the database  
        if number == 7:
            file = Path(input("Enter a log file absolute path.\n"))
            flight = Path(input("Enter a flight file absolute path, or leave blank.\n"))
            detector = input("What is the detector name? or leave blank.\n")
            detector_serial = input("What is the detector serial number? If unknown, leave blank\n")
            citizen_id = input("Enter the data collector's citizen id, or leave it blank.\n")
            submission_date = input("What is the submission date? YYYY-MM-DDTHH:mm:ssZ or leave blank.\n")
            
            if detector == "":
                detector_serial = "UNKNOWN"
            
            if detector_serial == "":
                detector_serial = "UNKNOWN"
            
            if citizen_id == "":
                citizen_id = "UNKNOWN"
            
            try:
                submission_date = datetime.strptime(submission_date, "%Y-%m-%dT%H:%M:%SZ")
            except:
                print("Using current UTC time for submission date.")
                submission_date = datetime.now(UTC)
            
            flight_valid = False
            if flight == Path("") or flight.is_file():
                flight_valid = True
            
            if file.is_file() and flight_valid:
                db.add(file, flight, detector=detector, detector_serial=detector_serial, citizen_id=citizen_id, submission_date=submission_date)
            if not file.is_file():
                print("Data file path invalid.")
            if not flight_valid:
                print("Flight file path invalid.")
         
        # reprocess a measurement in the database
        if number == 8:
            data_id = input("Enter data_id to reprocess (e.g.AFR81 Safecast 1083)\n") + "%"
            
            db.reprocess(data_id)
         
        # delete a measurement from the database
        if number == 9:
            data_id = input("Enter data_id to delete (e.g.AFR81 Safecast 1083)\n") + "%"
            
            db.delete(data_id)
        
        # The CLI program then returns to its main menu to ask for another prompt.
