# Prepare import of modules from parent directory.
import os
import sys
module_path = os.path.abspath(os.path.join('../../'))
if module_path not in sys.path:
    sys.path.append(module_path)

from studies import time_range_provider

CURRENT_IDENTIFIER = 'example_timerange' # Needs to be one of the keys in TIMERANGES

OBSERVATIONS_CSV_FILE = f'outputs/{CURRENT_IDENTIFIER}_observations.csv'
RECONSTRUCTED_CYCLES_JSON_FILE = f'outputs/{CURRENT_IDENTIFIER}_reconstructed_cycles.json'
CLEANED_CYCLES_JSON_FILE = f'outputs/{CURRENT_IDENTIFIER}_cleaned_cycles.json'

TIMERANGES = {
    'example_timerange': time_range_provider.TimeRangeProvider(False).get_example_hour_timestamp_range()
}
