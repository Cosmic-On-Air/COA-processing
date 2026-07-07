current processed log header format:

```
# format = processed_coa_v1
# data delimiter = comma
#
# detector_name = Safecast
# detector_serial_number = 1225
# detector_native_quantity = cnt_5s
# cnt_1min_source = original
# cnt_5s_source = original
# processing_pipeline = fit_data_to_cari_without_weather
#
# airport_code_type = ICAO
# origin = LOWW
# destination = LFPG
# flight_number = AFR68HV
# takeoff_utc = 2024-10-23T16:03:44Z
# landing_utc = 2024-10-23T17:49:11Z
#
# detector_timestamps = original
#
# timestamp_format = UTC_ISO8601
# latitude_unit = degrees
# longitude_unit = degrees
# altitude_unit = metres
#
# citizen_id = UNKNOWN
# submission_date = 2026-02-21T17:23:17Z
# processing_date = 2026-06-19T15:57:31Z
#
# reference_id = cari7a
# reference_model = CARI-7A
# reference_quantity = H*(10)_total-neutron
# reference_alignment_method = time_offset_max_r2
# reference_time_offset_s = -30
# reference_scaling_beta = 2.6262e-03
# reference_scaling_units = μSv/h / CPM
# reference_fit_r2 = 0.9900
#
# simulation_model = CARI-7A
# simulation_version = CARI-7A v4.2.0
# simulation_total = H*10_total
# simulation_neutron = H*10_neutron
# simulation_unit = μSv/h
#
# columns = timestamp_utc, cnt_1min, cnt_5s, latitude, longitude, altitude, simulation_total, simulation_neutron
2024-10-23T16:03:44Z, 30, 3, 48.11771, 16.55555, 0, 2.9136e-02, 1.3250e-02
2024-10-23T16:03:50Z, 33, 3, 48.11760, 16.55588, 0, 2.9146e-02, 1.3255e-02
...
```
