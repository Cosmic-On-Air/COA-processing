current processed log header format:

```
# format = processedCOA-v1
# data delimiter = comma
#
# device_id = Safecast 1225
# detector_model = ???
# detector_native_quantity = cnt_5s
# cnt_1min_source = original
# cnt_5s_source = original
# processing_pipeline = ???
#
# reference_id = cari7a
# reference_model = CARI-7A
# reference_quantity = H*(10)_total-neutron
# reference_alignment_method = time_offset_max_r2
# reference_time_offset_s = 140
# reference_scaling_beta = 2.3106e-03
# reference_scaling_units = μSv/h / CPM
# reference_fit_r2 = 0.8953
#
# simulation_model = CARI-7A
# simulation_version = ???
# simulation_total = H*10_total
# simulation_neutron = H*10_neutron
# simulation_unit = μSv/h
#
# airport_code_type = ICAO
# origin = KSFO
# destination = LFPG
# flight_number = AFR81
# takeoff_utc = 2025-06-27T03:51:58Z
# landing_utc = 2025-06-27T13:46:51Z
#
# detector_timestamps = original
#
# timestamp_format = UTC_ISO8601
# latitude_unit = degrees
# longitude_unit = degrees
# altitude_unit = metres
#
# citizen_id = UNKNOWN
#
# columns = timestamp_utc, cnt_1min, cnt_5s, latitude, longitude, altitude, simulation_total, simulation_neutron
2025-06-27T03:51:58Z, 22, 2, 37.62222, -122.38325, 107, 2.5259e-02, 9.2992e-03
2025-06-27T03:52:03Z, 23, 2, 37.62372, -122.38671, 174, 2.6447e-02, 9.7332e-03
...
```
