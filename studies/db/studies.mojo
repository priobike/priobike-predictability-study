from python import Python

fn main():
   print("Hello, world!")
   try:
      Python.add_to_path("/home/admin/priobike-observation-sink-studies/studies")
      Python.add_to_path("/home/admin/priobike-observation-sink-studies/studies/cycles/lib")
      Python.add_to_path("/home/admin/priobike-observation-sink-studies/preparations")
      let things_provider = Python.import_module("things_provider")
      let db_modulde = Python.import_module("db")
      let query_builder = Python.import_module("query_builder")
      let thing_module = Python.import_module("thing")
      let python_builtins: PythonObject = Python.import_module("builtins")
      let dict: PythonObject = python_builtins.dict
      let str: PythonObject = python_builtins.str
      let len: PythonObject = python_builtins.len
      let time = Python.import_module("time")

      let start_time = time.time()

      var tp = things_provider.ThingsProvider()
      _ = tp.filter_only_primary_signal_datastreams()
      let primary_signal_datastream_ids = tp.get_datastream_ids()

      tp = things_provider.ThingsProvider()
      _ = tp.filter_only_cycle_second_datastreams()
      let cycle_second_datastream_ids = tp.get_datastream_ids()

      let datastream_ids = primary_signal_datastream_ids + cycle_second_datastream_ids

      let WINDOW_SIZE_DB = 50000
      let WINDOW_SIZE_THINGS = 500
      let LIMIT = 1000000
      
      let db = db_modulde.DBClient(WINDOW_SIZE_DB, "1")
      let relevant_observations_query_bike = query_builder.get_relevant_observations(datastream_ids, LIMIT)
      var relevant_observation_rows_bike_generator = db.execute_query(relevant_observations_query_bike)
      
      tp = things_provider.ThingsProvider()
      let things_and_layer_names_by_datastream_id = tp.get_map_from_datastream_ids_to_thing_names_and_layer_names()

      let VALIDATION = False
      let RETRIEVE_ALL_CLEANUP_STATS = False

      let things = dict()
      var i = 0
      print("lets go \n")
      for row in relevant_observation_rows_bike_generator:
         let bla = things_and_layer_names_by_datastream_id[row[2]]
         let thing_name = bla[0]
         let layer_name = bla[1]
         if thing_name.__eq__("None"):
            print('Thing name is None for datastream id: ')
         if layer_name.__eq__("None"):
            print('Layer name is None for datastream id: ')
         let thing = things.get(thing_name)
         if thing.__eq__(None):
               _ = things.__setitem__(thing_name, thing_module.Thing(thing_name, WINDOW_SIZE_THINGS, VALIDATION, RETRIEVE_ALL_CLEANUP_STATS))
         
         _ = things[thing_name].add_observation(layer_name, row[0], row[1])
         if i % 5000 == 0:
            # clear_output(wait=True)
            print('Iteration: ')
            print(str(i))
            print("Number of things")
            print(len(things))
            let intermediate_time = time.time()
            print("Time elapsed: ")
            print(str(intermediate_time - start_time))
         
         i = i + 1

      _ = db.close()

      let end_time = time.time()

      print("Time elapsed: ")
      print(str(end_time - start_time))

   except e:
      print("Fail")
      print(e)
   
