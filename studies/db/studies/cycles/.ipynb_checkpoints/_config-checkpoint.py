# Prepare import of modules from parent directory.
import os
import sys
module_path = os.path.abspath(os.path.join('../../'))
if module_path not in sys.path:
    sys.path.append(module_path)

from studies import time_range_provider

CURRENT_IDENTIFIER = 'all_mondays' # Needs to be one of the keys in TIMERANGES, set this according to the study you want to run.

OBSERVATIONS_CSV_FILE = f'outputs/{CURRENT_IDENTIFIER}_observations.csv'
RECONSTRUCTED_CYCLES_JSON_FILE = f'outputs/{CURRENT_IDENTIFIER}_reconstructed_cycles.json'
CLEANED_CYCLES_JSON_FILE = f'outputs/{CURRENT_IDENTIFIER}_cleaned_cycles.json'
CLEANUP_STATS_JSON_FILE = f'outputs/{CURRENT_IDENTIFIER}_cleanup_stats.json'
PROGRAM_PREDICTABILITY_DISTANCE_DATA = f'outputs/{CURRENT_IDENTIFIER}_program_predictability_distance_data.json'
PROGRAM_PREDICTABILITY_DISTANCE_DATA_STATS = f'outputs/{CURRENT_IDENTIFIER}_program_predictability_distance_data_stats.json'

TIMERANGES = {
    'example_timerange': [time_range_provider.TimeRangeProvider(False).get_example_hour_timestamp_range()], # For testing of the study scripts, need to add array because we need to provide a list of time ranges.
    'all_mondays': time_range_provider.TimeRangeProvider(False).mondays_timestamp_ranges
}
