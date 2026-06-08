# COA-processing

Processing scripts and database structures for radiation measurement data collected during aircraft flights as part of the Cosmic On Air (COA) citizen science project.

## Project Overview

This project provides a complete pipeline for processing cosmic radiation measurements collected by citizen scientists flying on commercial aircraft. The data is collected using various radiation detectors (Safecast, Radiacode, GMC, Rium) and processed using flight trajectory data from FlightAware and radiation simulations from CARI-7A software.

## Core Features

- **Multi-detector support**: Process data from Safecast, GMC, Radiacode, and Rium radiation detectors
- **Flight data integration**: Read and process ADS-B flight data from FlightAware (KML and CSV formats)
- **Radiation simulations**: Automated interaction with CARI-7A software for reference radiation calculations
- **Data visualization**: Advanced Plotly-based visualization with multiple subplots and interactive features
- **Database management**: SQLite-based data archive with searchable metadata
- **Google integration**: Automated processing of form submissions, file uploads, and email responses via Google APIs
- **Calibration & alignment**: Automatic calibration factor calculation and time offset alignment

## Project Structure

```
COA-processing/
├── scripts/
│   ├── cosmic_on_air.py              # Core library for data processing and visualization
│   ├── cosmic_on_air_db.py           # Database management class and CLI
│   ├── coa_automation_script.py       # Google API automation and email handling
│   ├── analyse_your_coa_data.py       # User-friendly data analysis script
│   ├── images/                         # Logo and diagram assets
│   └── misc scripts/
│       ├── create_empty_db.py         # Initialize empty database
│       ├── find_calibration_factor.py # Analyze calibration factors across database
│       ├── klm2cari.py                # Convert KML flight data to CARI format
│       ├── plot_raw_cari.py           # Visualize raw CARI simulation output
│       ├── reprocess_all.py           # Batch reprocess all database entries
│       └── reprocess_repaired_times.py # Reprocess entries with corrected timestamps
├── database/
│   └── README.md                      # Database format specification
├── .github/workflows/
│   └── main.yml                       # GitHub Actions CI/CD workflow
├── requirements.txt                    # Python dependencies
├── .gitignore                         # Git configuration
└── README.md                          # This file
```

## Main Scripts

### cosmic_on_air.py
Core library providing all functions for flight data processing:
- Read detector data files (Safecast, GMC, Radiacode, Rium formats)
- Parse flight trajectory data (KML and CSV)
- Interface with CARI-7A software for radiation reference calculations
- Calibrate detector measurements against CARI-7A simulations
- Generate comprehensive Plotly visualizations
- Save and retrieve processed data

**Requirements**: CARI-7A software must be in the same directory

### cosmic_on_air_db.py
Database management and CLI tool:
- Manages SQLite database (`coa.db`) for flight metadata
- Handles data archive structure with organized folder hierarchy
- Provides search functionality by flight number, date, detector ID, etc.
- Includes command-line interface for database interactions
- Batch processing and reprocessing capabilities
- Export data for visualization and analysis

**Database location**: Expects `cwd/data archive/coa.db` or prompts for path

### coa_automation_script.py
Automates submission processing workflow:
- Retrieves form submissions from Google Forms and Google Drive
- Processes radiation measurements automatically
- Generates summary graphs for detected flights
- Sends processed results to citizen scientists via email
- Sends weekly summaries to Cosmic On Air administration
- Logs all activities and errors

**Requirements**: OAuth2 credentials for Google APIs (Drive, Sheets, Gmail)

### analyse_your_coa_data.py
Standalone user-friendly script for processing individual detector files:
- Minimal modification required - just set file paths
- Processes single detector and flight files
- Generates HTML visualizations in browser
- Exports processed log files
- Educational value for understanding the pipeline

## Installation & Setup

### Prerequisites
- Python 3.13.5 (or compatible version)
- CARI-7A software (free download: https://www.faa.gov/data_research/research/med_humanfacs/aeromedical/radiobiology/cari7)

### Python Dependencies
```
airportsdata==20250909
cartopy==0.25.0
google-api-python-client==2.187.0
google-auth-oauthlib==1.2.3
iso3166==2.1.1
matplotlib==3.10.8
numpy==2.4.2
plotly==6.5.0
protobuf==7.34.0
pykml==0.2.0
pyopensky==2.16
requests==2.32.5
scipy==1.17.1
kaleido==1.2.0
```

### Installation Steps

1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Download CARI-7A software and extract to the scripts folder:
   ```
   scripts/CARI_7A_DVD/cari7a_4.2.0(intel_linux)  # Linux/macOS
   scripts/cari7a420.exe                           # Windows
   ```

3. For automation features, obtain OAuth2 credentials from Google Cloud Console

## Data Workflow

### Submission Workflow
1. Users complete a Google Form with flight information and upload detector data file
2. Optional: Upload FlightAware KML file (required for full processing)
3. Administrator can also upload KML file if needed
4. `coa_automation_script.py` processes submissions daily
5. Processed data with visualization is emailed to user
6. Data added to archive database for later retrieval

### Processing Pipeline
1. **Data Reading**: Load detector measurements and flight trajectory
2. **Calibration**: Align detector and CARI-7A simulation timestamps
3. **Scaling**: Calculate conversion factor between detector counts and radiation dose
4. **Simulation**: Run CARI-7A to get reference radiation estimates
5. **Output**: Generate processed log file and interactive visualization

## Data Formats

### Processed Log Format
Output files follow a standardized header with metadata and columnar data:
```
# format = processedCOA-v1
# data delimiter = comma
# device_id = Safecast 1225
# detector_model = ???
# reference_scaling_beta = 2.3106e-03
# reference_alignment_method = time_offset_max_r2
...
```

Columns: `timestamp_utc, cnt_1min, cnt_5s, latitude, longitude, altitude, simulation_total, simulation_neutron`

See [database/README.md](database/README.md) for complete format specification.

## Automation & CI/CD

The project includes GitHub Actions workflow (`.github/workflows/main.yml`) that:
- Runs every 6 hours (at least once daily)
- Can be manually triggered
- Sets up Python 3.13.5
- Installs dependencies including CARI-7A
- Extracts and configures CARI software
- Loads Google API credentials from secrets
- Executes `coa_automation_script.py`

## Usage Examples

### Process Individual Flight Data
```python
import cosmic_on_air as coa

# Load detector and flight data
data = coa.find_processed("detector_file.log")
flight = coa.read_flight_kml("FlightAware_AFR81_KSFO_LFPG_20250627.kml")

# Generate visualization
fig = coa.plot_summary(data, flight)
```

### Database Interactions
```python
import cosmic_on_air_db as ca_db

db = ca_db.CoaDatabase("data_archive")
db.connect()
entries = db.get_entries()
db.add_entry(...)
db.close()
```

### Calibration Analysis
```python
# Find calibration factors across database
python scripts/misc\ scripts/find_calibration_factor.py
```

## Important Notes

- **CARI-7A Path**: When using cosmic_on_air functions, ensure CARI_7A_DVD folder is in the current working directory
- **Database Path**: When using cosmic_on_air_db directly, the script looks for `cwd/data archive/coa.db`. If not found, it will prompt for the absolute path
- **Backups**: Regular manual backups of the database are recommended to prevent data loss
- **Google Credentials**: Store OAuth2 tokens securely; do not commit to repository

## Contributors

- **C. Briand** - Laboratory for Space Studies and Instrumentation in Astrophysics, Observatoire de Paris, France
- **J. Trickett** - Department of Physics, University of Cape Town, South Africa
- **A. Gebbie** - Department of Physics, University of Cape Town, South Africa

## Project Information

- **Website**: cosmic-on-air.org
- **Email**: cosmiconair@gmail.com
- **Latest Update**: June 2026
- **License**: See LICENSE file in repository

## Related Projects

This project is part of the Cosmic On Air ecosystem:
- [AutoSolarActivid](../COA-AutoSolarActivid/) - Solar activity monitoring
