import multiprocessing
from os import getpid

from preparations import things_provider

tp = things_provider.ThingsProvider()
tp.filter_only_primary_signal_datastreams()
primary_signal_datastream_ids = tp.get_datastream_ids()

tp = things_provider.ThingsProvider()
tp.filter_only_cycle_second_datastreams()
cycle_second_datastream_ids = tp.get_datastream_ids()

datastream_ids = primary_signal_datastream_ids + cycle_second_datastream_ids

tp = things_provider.ThingsProvider()
things_and_layer_names_by_datastream_id = tp.get_map_from_datastream_ids_to_thing_names_and_layer_names()

PROCESSES = 1
datastream_ids_by_things = {}
for datastream_id in datastream_ids:
    thing_name, layer_name = things_and_layer_names_by_datastream_id[datastream_id]
    if layer_name != 'primary_signal' and layer_name != 'cycle_second':
        continue
    if thing_name not in datastream_ids_by_things:
        datastream_ids_by_things[thing_name] = []
    datastream_ids_by_things[thing_name].append(datastream_id)
datastream_ids_for_processes = [[] for i in range(PROCESSES)]
i = 0
for thing_name, datastreams in datastream_ids_by_things.items():
    process_index = i % PROCESSES
        
    datastream_ids_for_processes[process_index] += datastreams
    i += 1
    
# Safety check:
total_datastreams = 0
for datastreams in datastream_ids_for_processes:
    total_datastreams += len(datastreams)
if total_datastreams != len(datastream_ids):
    raise Exception('Total datastreams is not equal to number of datastreams: ' + str(total_datastreams) + ' != ' + str(len(datastream_ids)))
for datastream in datastream_ids:
    found = False
    for datastreams in datastream_ids_for_processes:
        if datastream in datastreams:
            found = True
            break
    if not found:
        raise Exception('Datastream not found in any process: ' + str(datastream))
    

# Test with one thing:
""" thing_name = None
for datastream_id in things_and_layer_names_by_datastream_id:
    if thing_name is None and (
        things_and_layer_names_by_datastream_id[datastream_id][1] == 'primary_signal' or
        things_and_layer_names_by_datastream_id[datastream_id][1] == 'cycle_second'
    ):
        datastream_ids.append(datastream_id)
        thing_name = things_and_layer_names_by_datastream_id[datastream_id][0]
    elif thing_name == things_and_layer_names_by_datastream_id[datastream_id][0] and (
        things_and_layer_names_by_datastream_id[datastream_id][1] == 'primary_signal' or
        things_and_layer_names_by_datastream_id[datastream_id][1] == 'cycle_second'
    ):
        datastream_ids.append(datastream_id)
        
if len(datastream_ids) != 2:
    raise Exception('Number of datastream ids is not 2: ' + str(len(datastream_ids))) """

def run(datastream_ids):
    from studies import db
    from studies.cycles.lib import query_builder
    from studies.cycles.lib.thing import Thing
    from preparations import things_provider
    from time import time
    
    start_time = time()
    
    WINDOW_SIZE_DB = 50000
    WINDOW_SIZE_THINGS = 500
    LIMIT = 1000000
    
    db = db.DBClient(WINDOW_SIZE_DB, id)
    relevant_observations_query_bike = query_builder.get_relevant_observations(datastream_ids, LIMIT)
    relevant_observation_rows_bike_generator = db.execute_query(relevant_observations_query_bike)
    
    tp = things_provider.ThingsProvider()
    things_and_layer_names_by_datastream_id = tp.get_map_from_datastream_ids_to_thing_names_and_layer_names()

    VALIDATION = False
    RETRIEVE_ALL_CLEANUP_STATS = False

    things: dict[str,Thing] = {}
    i = 0
    print(f"{getpid()}: lets go \n")
    for row in relevant_observation_rows_bike_generator:
        thing_name, layer_name = things_and_layer_names_by_datastream_id[row[2]]
        if thing_name is None:
            raise Exception('Thing name is None for datastream id: ' + str(row[2]))
        if layer_name is None:
            raise Exception('Layer name is None for datastream id: ' + str(row[2]))
        if thing_name not in things:
            things[thing_name] = Thing(thing_name, WINDOW_SIZE_THINGS, VALIDATION, RETRIEVE_ALL_CLEANUP_STATS)
            
        things[thing_name].add_observation(layer_name, row[0], row[1])
        if i % 5000 == 0:
            # clear_output(wait=True)
            print(str(getpid()) + ': Iteration: ' + str(i) +' - Number of things ' + str(len(things)) + "\n")
            intermediate_time = time()
            print(f"{getpid()}: Intermediate time: {intermediate_time - start_time} seconds")
        
        i += 1

    db.close()
    
    end_time = time()
    print(f"{getpid()}: done in {end_time - start_time} seconds")
    
    return things

pool = multiprocessing.Pool(processes = PROCESSES)
returns = pool.map(run, datastream_ids_for_processes)
