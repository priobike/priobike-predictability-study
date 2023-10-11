import pandas as pd
import numpy as np
from datetime import datetime
import statistics
from juliacall import Main as jl
jl.seval("using OnlineStats")

from preparations import things_provider

INVALID_STATE_TRANSITIONS = {
    1: [2],
    2: [3,4],
    3: [4],
    4: [1,2], 
}

MAX_STATE_LENGTHS = {
    2: 6,
    4: 2,
}
        
        
def phase_wise_distance(cycle_1, cycle_2):
    distance = 0
    length = max(len(cycle_1), len(cycle_2))
    for i in range(length):
        if i >= len(cycle_2):
            distance += 1
            continue
        
        if i >= len(cycle_1):
            distance += 1
            continue
        
        if cycle_1[i] != cycle_2[i]:
            distance += 1
            continue
        distance += 0
        
    return distance

def structure_observation_data(csv_file: str) -> dict:
    """
    Converts our csv observation file to json files for each thing where each contains a dict of the three datastream types
    (primary_signal, cycle_second, signal_program) with a dataframe of their corresponding observations for that datastream and thing.
    
    {
        'thing_name': {
            'primary_signal': pd.DataFrame,
            'cycle_second': pd.DataFrame,
            'signal_program': pd.DataFrame
        },
        ...
    }
    """
    
    # Parse csv file into pandas dataframe
    df = pd.read_csv(csv_file)

    # Sort by phenonemon_time
    df = df.sort_values(by=['phenomenon_time'])
    
    thing_datastreams = {}

    # Split dataframe such that we have one dataframe per datastream_id
    queried_datastreams = {}
    for datastream_id in df['datastream_id'].unique():
        queried_datastreams[datastream_id] = df[df['datastream_id'] == datastream_id]
        
    tp = things_provider.ThingsProvider()
    things = tp.get_things()

    # Stats
    not_returned_by_db_counter = 0
    total_number_of_datastreams = 0
    total_number_of_relevant_datastreams = 0

    for thing in things:
        name = thing['name']
        datastreams = thing['Datastreams']
        for datastream in datastreams:
            total_number_of_datastreams += 1
            layer_name = datastream['properties']['layerName']
            if layer_name != 'primary_signal' and layer_name != 'cycle_second' and layer_name != 'signal_program':
                # Not relevant for cycles
                continue
            total_number_of_relevant_datastreams += 1
            id = datastream['@iot.id']
            if id not in queried_datastreams:
                not_returned_by_db_counter += 1
                continue
            if name not in thing_datastreams:
                thing_datastreams[name] = {}
            thing_datastreams[name][layer_name] = queried_datastreams[id]

    print('Total number of datastreams: ' + str(total_number_of_datastreams))
    print('Total number of relevant datastreams: ' + str(total_number_of_relevant_datastreams))
    print('Number of datastreams not queried: ' + str(not_returned_by_db_counter))
    
    return thing_datastreams

def reconstruct_cycles_algo(datastreams: dict, last_result_before_first_known_primary_signal: int=None):
    """
    datastreams should be a dict with at least the following structure:
    {
        "primary_signal": pd.DataFrame,
        "cycle_second": pd.DataFrame,
    }
    Attention: The dataframes have to be sorted by phenomenon_time.
    """

    # Primary signal observations and cycle second observations are required.
    primary_signal_missing = False
    cycle_second_missing = False
    
    # Check if required datastreams are present. Early return if not.
    if 'primary_signal' not in datastreams:
        primary_signal_missing = True
    if 'cycle_second' not in datastreams:
        cycle_second_missing = True
    if primary_signal_missing or cycle_second_missing:
        return None, 0, primary_signal_missing, cycle_second_missing
    
    
    primary_signal_observations = datastreams['primary_signal']
    cycle_second_observations = datastreams['cycle_second']
    
    primary_signal_observation_count= len(primary_signal_observations)
    cycle_second_observation_count = len(cycle_second_observations)
    
    # Current looked at primary signal observation
    primary_signal_index = 0
    
    # Current looked at cycle second observation
    cycle_second_index = 0
    
    # The chances are very low that we only receive one primary signal (if none are received at all we already have an early return).
    # Thus if this happens we throw an exception to indicate that there might be a bug in the code leading to this.
    if primary_signal_index + 1 >= primary_signal_observation_count:
        raise Exception('Not enough primary signals to reconstruct cycles. Maybe a bug in the code? -> Look at comment in code.')
    
    first_primary_signal_phenonmenon_time = primary_signal_observations.iloc[primary_signal_index]['phenomenon_time']
    first_cycle_second_phenonmenon_time = cycle_second_observations.iloc[cycle_second_index]['phenomenon_time']
    
    # If the first primary signal observation is after the first cycle second observation,
    # we use, if available, the last primary signal of the last window.
    if first_primary_signal_phenonmenon_time > first_cycle_second_phenonmenon_time and last_result_before_first_known_primary_signal is not None:
        result = last_result_before_first_known_primary_signal
        # The phenomenon time of the next primary signal observation (used to look ahead when we switch to the next primary signal observation).
        upcoming_primary_signal_observation_phenomenon_time = primary_signal_observations.iloc[primary_signal_index]['phenomenon_time']
        # No current primary signal observation
        primary_signal_index = None
    else:
        # The result of the current primary signal
        result = primary_signal_observations.iloc[primary_signal_index]['result']
        # The phenomenon time of the next primary signal observation (used to look ahead when we switch to the next primary signal observation).
        upcoming_primary_signal_observation_phenomenon_time = primary_signal_observations.iloc[primary_signal_index + 1]['phenomenon_time']
        
    # We start at the first received primary signal or cycle second observation and go on second by second.
    # During this process we construct cycles and throw away primary signals that don't belong to a cycle.
    # If the primary signal came before the cycle it's important to start there such that we know the result one the cycle starts.
    # If the cycle came before the primary signal we start there because we don't know the result of the primary signal before the cycle starts.
    # We only try to use the result last primary signal of the previous window.
    ticker_second = min(first_primary_signal_phenonmenon_time, first_cycle_second_phenonmenon_time)
    
    # Before we reconstruct the programs we first reconstruct all cycles regardless of the programs.
    cycles: list[Cycle] = []
    
    # Where we save the data (start time, end time, primary signal observation results) of the current cycle.
    current_cycle = None
    
    # Start and end phenomenon time of the currently looked at cycle
    cycle_time_start = None
    cycle_time_end = None
    
    # How many times we skipped cycles where the primary signals were missing
    skipped_cycles = 0
    
    while ticker_second < cycle_second_observations.iloc[-1]['phenomenon_time']:
        if cycle_second_index + 1 >= cycle_second_observation_count:
            # End of data ("+ 1") because we also need to have an end for the cycle
            break
        
        # First cycle
        if cycle_time_start is None:
            cycle_time_start = cycle_second_observations.iloc[cycle_second_index]['phenomenon_time']
        if cycle_time_end is None:
            cycle_time_end = cycle_second_observations.iloc[cycle_second_index + 1]['phenomenon_time']
        
        # Update current cycle for all upcoming cycles after the first cycle.
        if ticker_second >= cycle_time_end:
            # If we proceed to the next cycle without having saved any data for the current cycle this means that there we no corresponding primary signals observations.
            # Thus we skip this cycle.
            if current_cycle is None:
                skipped_cycles += 1
            else:
                # Save current cycle
                cycles.append(current_cycle)
                current_cycle = None
            
            cycle_second_index += 1
            cycle_time_start = cycle_second_observations.iloc[cycle_second_index]['phenomenon_time']
            cycle_time_end = cycle_second_observations.iloc[cycle_second_index + 1]['phenomenon_time']
        
        # We reached a time with the ticker where we have a new primary signal observation.
        if upcoming_primary_signal_observation_phenomenon_time is not None and ticker_second >= upcoming_primary_signal_observation_phenomenon_time:
            # Update current primary signal.
            if primary_signal_index is None:
                primary_signal_index = 0
            else:
                primary_signal_index += 1
            result = primary_signal_observations.iloc[primary_signal_index]['result']
            # Check if there are still primary signal observations left and update the upcoming primary signal observation phenonemon time accordingly.
            if primary_signal_index + 1 >= primary_signal_observation_count:
                upcoming_primary_signal_observation_phenomenon_time = None
            else:
                upcoming_primary_signal_observation_phenomenon_time = primary_signal_observations.iloc[primary_signal_index + 1]['phenomenon_time']
            
        # If the current cycle is none (either because it is the first cycle or because we just saved the last cycle) we create a new cycle,
        # but only if the ticker is at the start of the current cycle.
        # This is checked to assure that we only create cycles where we have corresponding primary signal observation data.
        if current_cycle is None and ticker_second == cycle_time_start and result is not None:
            current_cycle = Cycle(start=cycle_time_start, end=cycle_time_end)
        
        # Fill up the results of the current cycle with the current primary signal observation result until:
        # option 1: we reach the end of the current cycle
        # option 2: we reach the next primary signal observation
        # The option that comes first is the one that is executed.
        if current_cycle is not None and ticker_second >= cycle_time_start:
            if upcoming_primary_signal_observation_phenomenon_time is None:
                diff_upcoming = 999_999_999_999
            else:
                diff_upcoming = upcoming_primary_signal_observation_phenomenon_time - ticker_second
                
            diff_cycle_end = cycle_time_end - ticker_second
            
            diff = min(diff_upcoming, diff_cycle_end)
            results_to_append = [result] * diff
            
            current_cycle.results.extend(results_to_append)
    
            ticker_second += diff
        else:
            # If we are not in a cycle we just go on second by second.
            ticker_second += 1
            
    return cycles, skipped_cycles, primary_signal_missing, cycle_second_missing

class Cycle:
    def __init__(self, start, end):
        self.start = start
        self.end = end
        self.results = []

class Thing:
    def __init__(self, name, window_size, validation, retrieve_all_cleanup_stats=False):
        # Meta
        self.name = name
        
        # Program settings
        self.window_size = window_size
        self.validation = validation # Whether to validate cycles (expensive)
        self.retrieve_all_cleanup_stats = retrieve_all_cleanup_stats # More expensive, but gives more detailed stats
        self.ticker = 0
        
        # Data
        self.observations_by_datastream = {
            "primary_signal": pd.DataFrame(columns=['phenomenon_time', 'result']),
            "cycle_second": pd.DataFrame(columns=['phenomenon_time', 'result']),
        }
        self.cycles: list[Cycle] = []
        self.last_window_last_result = None
        
        # Metrics (one for each hour of the week)
        self.metrics = [
            [
                jl.OnlineStats.Series(jl.OnlineStats.Quantile()) for i in range(24)
            ] for j in range(7)
        ]
        
        # Reconstruction Stats
        self.primary_signal_missing_count = 0
        self.cycle_second_missing_count = 0
        self.total_skipped_cycles = 0
        
        # General stats
        self.total_cycles_count = 0
        
        # Clean up stats
        self.total_removed_cycles = 0
        self.total_invalid_cycles_length = 0
        self.total_invalid_cycles_transitions = 0
        self.total_invalid_cycles_missing = 0
        
        
        
    def add_observation(self, layer_name, phenomenon_time, result):
        self.observations_by_datastream[layer_name].loc[len(self.observations_by_datastream[layer_name])] = [phenomenon_time, result]
        self.ticker += 1
        if self.ticker > self.window_size:
            self.process_window()
            self.ticker = 0
            
    def update_metrics(self):
        for i in range(len(self.cycles) - 1):
            cycle_1 = self.cycles[i]
            cycle_2 = self.cycles[i + 1]
            distance = phase_wise_distance(cycle_1.results, cycle_2.results)
            
            date_time = datetime.fromtimestamp(cycle_1.start)
            weekday = date_time.weekday()
            hour = date_time.hour
            self.metrics[weekday][hour]
            jl.OnlineStats.fit_b(self.metrics[weekday][hour], distance)
            
            
    def process_window(self):
        # 1. Calculate metrics
        self.update_metrics()
        
        # 2. Reconstruct cycles
        cycles, last_result, observations_after_last_cycle, skipped_cycles, primary_signal_missing, cycle_second_missing = self.reconstruct_cycles()
        
        # 3. Validate cycles
        if self.validation:
            self.validate_cycles(cycles)
        
        # 3. Store results
        self.cycles = cycles
        self.last_window_last_result = last_result
        self.observations_by_datastream = observations_after_last_cycle
        self.total_skipped_cycles += skipped_cycles
        if primary_signal_missing:
            self.primary_signal_missing_count += 1
        if cycle_second_missing:
            self.cycle_second_missing_count += 1
        
        # 4. Clean up cycles
        self.clean_up_cycles()
            
            
    def reconstruct_cycles(self):
        """
        Returns the following:
        1. List of constructed cycles
        2. Last result of last cycle
        3. Observations after the last cycle (thus unused in the current window, may be used in the next window)
        
        """
        
        datastreams = self.observations_by_datastream
        
        cycles, skipped_cycles, primary_signal_missing, cycle_second_missing = reconstruct_cycles_algo(datastreams, self.last_window_last_result)
        
        if cycles is None or len(cycles) == 0:
            return  None, None, datastreams, skipped_cycles, primary_signal_missing, cycle_second_missing
            
        else:
            end_last_cycle = cycles[-1].end
            # Get all observations after the last cycle
            observations_after_last_cycle = {}
            for layer_name in datastreams:
                # Important, use ">=" such that the end observation for the last cycle can be also the start observation for the next cycle
                observations_after_last_cycle[layer_name] = datastreams[layer_name][datastreams[layer_name]['phenomenon_time'] >= end_last_cycle]
                observations_after_last_cycle[layer_name] = observations_after_last_cycle[layer_name].reset_index(drop=True)
            
            last_result = cycles[-1].results[-1]

            return cycles, last_result, observations_after_last_cycle, skipped_cycles, primary_signal_missing, cycle_second_missing
        
    def validate_cycles(self, cycles):
        if len(cycles) == 0:
            print ('No cycles.')
            return
        
        """
        --------------------------------
        FIRST: Check if the count of results in the cycles is equal to the difference between the start and end time.
        --------------------------------  
        """

        # List of bools where each bool indicates if a problem was found for a cycle.
        # Thus, if all bools are False, then there are no problems
        seconds_in_cycle_differing = np.array([])
        
        for cycle in cycles:
            cycle_start = cycle.start
            cycle_end = cycle.end
            diff = cycle_end - cycle_start
            count = len(cycle.results)
            seconds_in_cycle_differing= np.append(seconds_in_cycle_differing, diff != count)
        
        # Count True values
        diff_count = np.sum(seconds_in_cycle_differing)
        if diff_count != 0:
            raise Exception(f'Attention: Number of cycles with differing start/end times: {diff_count}. This should not happen and is a bug.')
        
        """
        --------------------------------
        SECOND: Check for the following, if there exist a corresponding observation:
        1. Random cycle start time: Is there a corresponding cycle_second observation for that thing?
        2. Random result in cycle: Is there a corresponding primary_signal observation for that thing?
        Do this 100 times, just to make sure..
        --------------------------------  
        """
        # Count of how many checks we made (primary signal and cycle second are not counted seperatly).
        checked_count = 0

        while checked_count < 50:
            # Random cycle
            cycle = np.random.choice(cycles)
            
            # Find all state changes (e.g. when result changes from 2 to 1)
            state_changes = []
            previous_result = None
            idx = 0
            for result in cycle.results:
                if previous_result is not None and result != previous_result:
                    state_changes.append((result, cycle.start + idx))
                previous_result = result
                idx += 1
                
            # Random state change (result and exact time of result)
            if len(state_changes) == 0:
                continue
            result_idx = np.random.choice(len(state_changes))
            result = state_changes[result_idx]
            
            # Check if there exists a corresponding observation and whether results match
            result = self.observations_by_datastream["primary_signal"]\
                [(self.observations_by_datastream["primary_signal"]['result'] == result[0]) & \
                    (self.observations_by_datastream["primary_signal"]['phenomenon_time'] == result[1])]
            if len(result) != 1:
                print("Cycle start:\n")
                print(cycle.start)
                print("Cycle end:\n")
                print(cycle.end)
                print("Cycle results:\n")
                print(cycle.results)
                print("State changes:\n")
                print(state_changes)
                print("Primary Signal Observations:\n")
                print(self.observations_by_datastream["primary_signal"].to_string())
                print("Cycle Second Observations:\n")
                print(self.observations_by_datastream["cycle_second"].to_string())
                raise Exception(f'Attention: There exists at least one primary signal state change without a corresponding observation.')
                
            cycle_start = cycle.start
            
            result = self.observations_by_datastream["cycle_second"]\
                [(self.observations_by_datastream["cycle_second"]['phenomenon_time'] == cycle_start)]
            if len(result) != 1:
                print("Cycle start:\n")
                print(cycle.start)
                print("Cycle end:\n")
                print(cycle.end)
                print("Cycle results:\n")
                print(cycle.results)
                print("State changes:\n")
                print(state_changes)
                print("Primary Signal Observations:\n")
                print(self.observations_by_datastream["primary_signal"].to_string())
                print("Cycle Second Observations:\n")
                print(self.observations_by_datastream["cycle_second"].to_string())
                raise Exception(f'Attention: There exists at least one cycle start second without a corresponding observation.')
            
            checked_count += 1
            
    def clean_up_cycles(self):
        """
        Goal: Remove cycles that are invalid or too long/short.

        Color encoding:
        0 = dark
        1 = red
        2 = amber
        3 = green
        4 = red amber
        5 = amber flashing
        6 = green flashing
        
        1. Find invalid state transitions.

            Typical cycles:
            1. -> Red -> RedAmber -> Green -> Amber -> Red ->
            2. -> Red -> Green -> Red ->

            Thus, we can safely say that the following state transitions are invalid:
            Red -> Amber
            Amber -> Green
            Amber -> RedAmber
            Green -> RedAmber
            RedAmber -> Red
            RedAmber -> Amber
        
        2. Find missing observations.

            We can do that by looking at the length of amber and red amber phases.
            By definition, amber is maximum 6 seconds long and red amber is maximum 2 seconds long.
        """
        
        # Stats
        cycles_count = 0
        removed_cycles_count = 0
        total_invalid_cycles_length = 0
        total_invalid_cycles_transitions = 0
        total_invalid_cycles_missing = 0
        
        cleaned_up_cycles = []
        
        cycle_lengths = []
        for cycle in self.cycles:
            cycle_lengths.append(len(cycle.results))
        median_cycle_length = statistics.median(cycle_lengths)
        
        for cycle in self.cycles:
            cycles_count += 1
            
            results = cycle.results
            # Check for too long or too short cycles
            wrong_length = False
            if len(results) > median_cycle_length * 1.5 or len(results) < median_cycle_length * 0.5:
                wrong_length = True
            
            # Check for invalid state transitions
            wrong_transition = False
            current_state = None
            for i in range(len(results)):
                if current_state is not None:
                    if results[i] in INVALID_STATE_TRANSITIONS[current_state]:
                        wrong_transition = True
                        break
                if results[i] in INVALID_STATE_TRANSITIONS:
                    current_state = results[i]
                else:
                    current_state = None
            
            # Check for missing observations
            missing = False
            max_state_length = None
            max_state_length_counter = 0
            for i in range(len(results)):
                if max_state_length is not None:
                    if max_state_length_counter > max_state_length:
                            missing = True
                            break
                    
                    if results[i] == results[i-1]:
                        max_state_length_counter += 1
                    else:
                        max_state_length = None
                        max_state_length_counter = 0
                if results[i] in MAX_STATE_LENGTHS:
                    max_state_length = MAX_STATE_LENGTHS[results[i]]

            if wrong_length or wrong_transition or missing:
                removed_cycles_count += 1
                if wrong_length:
                    total_invalid_cycles_length += 1
                if wrong_transition:
                    total_invalid_cycles_transitions += 1
                if missing:
                    total_invalid_cycles_missing += 1
                
            else:
                cleaned_up_cycles.append(cycle)
                
        self.cycles = cleaned_up_cycles
        
        self.total_cycles_count += cycles_count
        self.total_removed_cycles += removed_cycles_count
        
        self.total_invalid_cycles_length += total_invalid_cycles_length
        self.total_invalid_cycles_transitions += total_invalid_cycles_transitions
        self.total_invalid_cycles_missing += total_invalid_cycles_missing