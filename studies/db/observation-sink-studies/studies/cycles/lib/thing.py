import pandas as pd
import numpy as np
import statistics

from studies.cycles.lib import data_processing

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

class Thing:
    def __init__(self, name, window_size, validation, retrieve_all_cleanup_stats=False):
        # Meta
        self.name = name
        
        # Program settings
        self.window_size = window_size
        self.validation = validation # Whether to validate cycles (expensive)
        self.retrieve_all_cleanup_stats = retrieve_all_cleanup_stats # More expensive, but gives more detailed stats
        
        # Reconstruction Stats
        self.primary_signal_missing_count = 0
        self.cycle_second_missing_count = 0
        self.total_skipped_cycles = 0
        
        # Data
        self.observations_by_datastream = {
            "primary_signal": pd.DataFrame(columns=['phenomenon_time', 'result']),
            "cycle_second": pd.DataFrame(columns=['phenomenon_time', 'result']),
        }
        """
        [
            {
                "start": ...,
                "end": ...,
                "results": []
            },
            ...
        ]
        """
        self.cycles = []
        self.last_window_last_result = None
        
        # General stats
        self.total_cycles_count = 0
        
        # Clean up stats
        self.total_removed_cycles = 0
        self.total_invalid_cycles_length = 0
        self.total_invalid_cycles_transitions = 0
        self.total_invalid_cycles_missing = 0
        
        
        
    def add_observation(self, layer_name, phenomenon_time, result):
        self.observations_by_datastream[layer_name] = self.observations_by_datastream[layer_name].append({'phenomenon_time': phenomenon_time, 'result': result}, ignore_index=True)
        self.ticker += 1
        if self.ticker > self.window_size:
            self.process_window()
            self.ticker = 0
            
            
    def process_window(self):
        # 1. Calculate metrics
        
        
        # 2. Reconstruct cycles
        cycles, last_result, observations_after_last_cycle, skipped_cycles, primary_signal_missing, cycle_second_missing = self.reconstruct_cycles()
        
        # 3. Validate cycles
        if self.validation:
            self.validate_cycles()
        
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
            
            
    def reconstruct_cyles(self):
        """
        Returns the following:
        1. List of constructed cycles
        2. Last result of last cycle
        3. Observations after the last cycle (thus unused in the current window, may be used in the next window)
        
        """
        
        datastreams = self.observations_by_datastream
        
        cycles, primary_signal_missing, cycle_second_missing, skipped_cycles = data_processing.reconstruct_cycles(datastreams, self.last_window_last_result)
        
        if cycles is None:
            return  None, None, datastreams, skipped_cycles, primary_signal_missing, cycle_second_missing
            
        else:
            end_last_cycle = cycles[-1]['end']
            # Get all observations after the last cycle
            observations_after_last_cycle = {}
            for layer_name in datastreams:
                # Important, use ">=" such that the end observation for the last cycle can be also the start observation for the next cycle
                observations_after_last_cycle[layer_name] = datastreams[layer_name][datastreams[layer_name]['phenomenon_time'] >= end_last_cycle]
            
            last_result = cycles[-1]['results'][-1]

            return cycles, last_result, observations_after_last_cycle
        
    def validate_cycles(self):
        """
        --------------------------------
        FIRST: Check if the count of results in the cycles is equal to the difference between the start and end time.
        --------------------------------  
        """

        # List of bools where each bool indicates if a problem was found for a cycle.
        # Thus, if all bools are False, then there are no problems
        seconds_in_cycle_differing = np.array([])
        
        for cycle in self.cycles:
            cycle_start = cycle['start']
            cycle_end = cycle['end']
            diff = cycle_end - cycle_start
            count = len(cycle['results'])
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
            cycle = np.random.choice(self.cycles)
            
            # Find all state changes (e.g. when result changes from 2 to 1)
            state_changes = []
            previous_result = None
            idx = 0
            for result in cycle['results']:
                if previous_result is not None and result != previous_result:
                    state_changes.append((result, cycle['start'] + idx))
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
                raise Exception(f'Attention: There exists at least one primary signal state change without a corresponding observation.')
                
            cycle_start = cycle['start']
            
            result = self.observations_by_datastream["primary_signal"]\
                [(self.observations_by_datastream["primary_signal"]['phenomenon_time'] == cycle_start)]
            if len(result) != 1:
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
            cycle_lengths.append(len(cycle["results"]))
        median_cycle_length = statistics.median(cycle_lengths)
        
        for cycle in self.cycles:
            cycles_count += 1
            
            results = cycle["results"]
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