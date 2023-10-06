import pandas as pd

from preparations import things_provider

def structure_observation_data(csv_file: str) -> dict:
    """
    Converts our csv observation file to a dict which maps thing names to a dict of the three datastream types
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

    # Directory of things where each thing ID maps to a dict of the three datastream types for that thing
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

def reconstruct_cycles(datastreams: dict):
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
        return None, primary_signal_missing, cycle_second_missing, 0
    
    
    primary_signal_observations = datastreams['primary_signal']
    cycle_second_observations = datastreams['cycle_second']
    
    primary_signal_observation_count= len(primary_signal_observations)
    cycle_second_observation_count = len(cycle_second_observations)
    
    # Current looked at primary signal observation
    primary_signal_index = 0
    
    # We start at the first received primary signal and go on second by second.
    # During this process we construct cycles and throw away primary signals that don't belong to a cycle.
    ticker_second = primary_signal_observations.iloc[primary_signal_index]['phenomenon_time']
    # The result of the current primary signal
    result = primary_signal_observations.iloc[primary_signal_index]['result']
    
    # The chances are very low that we only receive one primary signal (if none are received at all we already have an early return).
    # Thus if this happens we throw an exception to indicate that there might be a bug in the code leading to this.
    if primary_signal_index + 1 >= primary_signal_observation_count:
        Exception('Not enough primary signals to reconstruct cycles. Maybe a bug in the code? -> Look at comment in code.')
        
    # The phenomenon time of the next primary signal observation (used to look ahead when we switch to the next primary signal observation).
    upcoming_primary_signal_observation_phenomenon_time = primary_signal_observations.iloc[primary_signal_index + 1]['phenomenon_time']
    
    # Before we reconstruct the programs we first reconstruct all cycles regardless of the programs.
    cycles = []
    
    # Where we save the data (start time, end time, primary signal observation results) of the current cycle.
    current_cycle = None

    # Current looked at cycle second observation
    cycle_second_index = 0
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
        if current_cycle is None and ticker_second == cycle_time_start:
            current_cycle = {
                'start': cycle_time_start,
                'end': cycle_time_end,
                'results': []
            }
        
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
            
            current_cycle['results'].extend(results_to_append)
    
            ticker_second += diff
        else:
            # If we are not in a cycle we just go on second by second.
            ticker_second += 1
            
    return cycles, primary_signal_missing, cycle_second_missing, skipped_cycles

def reconstruct_programs(cycles, signal_program_observations):
    """
    Cycles has to be the output of reconstruct_cycles.
    
    signal_program_observations should either be a pd.DataFrame according to our observations csv file or None (they are optional).
    
    Attention: The dataframe has to be sorted by phenomenon_time.
    """
    
    if signal_program_observations is None:
        signal_program_observations_count = 0
    else:
        signal_program_observations_count = len(signal_program_observations)
    
    # Where we save the reconstructed programs.
    programs = {}
    
    # For cycles where we don't have a corresponding signal program observation we use the unknown program identifier.
    UNKNWON_PROGRAM_IDENTIFIER = 'unknown'
      
    # If we don't have any signal program observations we just put all cycles in the unknown program.
    if signal_program_observations is None or signal_program_observations_count == 0:
        programs = {
            UNKNWON_PROGRAM_IDENTIFIER: cycles
        }
    else:
        # Start with unknown until we reach the first signal program observation.
        current_program = UNKNWON_PROGRAM_IDENTIFIER
        # Index of the current looked at signal program observation.
        signal_program_index = 0
        # Start and end time of the current looked at program.
        program_time_start = None
        program_time_end = None
        for cycle in cycles:
            # First program:
            if program_time_start is None and signal_program_index < signal_program_observations_count:
                program_time_start = signal_program_observations.iloc[signal_program_index]['phenomenon_time']
                current_program = str(signal_program_observations.iloc[signal_program_index]['result'])
            if program_time_end is None and signal_program_index + 1 < signal_program_observations_count:
                program_time_end = signal_program_observations.iloc[signal_program_index + 1]['phenomenon_time']
                
            # Update current program if we know our program end time and the current cycle starts at or after the program end time.
            if program_time_end is not None and cycle['start'] >= program_time_end:
                # Select next program (if there is one, otherwise switch to unknown program)
                # Can also happenn that we know the start time of the program but not the end time.
                signal_program_index += 1
                if signal_program_index < signal_program_observations_count:
                    program_time_start = signal_program_observations.iloc[signal_program_index]['phenomenon_time']
                    current_program = str(signal_program_observations.iloc[signal_program_index]['result'])
                else:
                    program_time_start = None
                    current_program = UNKNWON_PROGRAM_IDENTIFIER
                if signal_program_index + 1 < signal_program_observations_count:
                    program_time_end = signal_program_observations.iloc[signal_program_index + 1]['phenomenon_time']
                else:   
                    program_time_end = None
                    
            # Cycle starts before program starts and ends after program starts (something wrong in data).
            if cycle['start'] < program_time_start and cycle['end'] > program_time_start:
                continue
                
            # Add cycle to program.
            if current_program not in programs:
                programs[current_program] = []
            programs[current_program].append(cycle)
    
    return programs