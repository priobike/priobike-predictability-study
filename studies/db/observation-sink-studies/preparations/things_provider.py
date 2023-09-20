import json
import os

class ThingsProvider:
    """
    Use this to get or filter things and datastreams from the things.json file.
    Examples:
    things = ThingsProvider().filter_only_bike_things().get_things()
    datastream_ids = ThingsProvider().filter_only_bike_things().filter_only_primary_signal_datastreams().get_datastream_ids()
    NOTE: The filters are not reversible. Create a new ThingsProvider object if you want to use a different filter on the whole things list.
    """
    def __init__(self):
        # Get path to things.json file
        path = os.path.dirname(os.path.abspath(__file__))
        with open(f'{path}/things.json', 'r', encoding='utf-8') as JSON:
            self.things = json.load(JSON)
            
    def filter_only_bike_things(self):
        bike_things = []
        for thing in self.things:
            if thing["properties"]["laneType"] == "Radfahrer" or\
                thing["properties"]["laneType"] == "KFZ/Radfahrer" or\
                    thing["properties"]["laneType"] == "Fußgänger/Radfahrer" or\
                        thing["properties"]["laneType"] == "Bus/Radfahrer" or\
                            thing["properties"]["laneType"] == "KFZ/Bus/Radfahrer" :
                bike_things.append(thing)
                
        self.things = bike_things
        
    def get_all_existing_layer_names(self):
        layer_names = []
        for thing in self.things:
            for datastream in thing["Datastreams"]:
                if datastream["properties"]["layerName"] not in layer_names:
                    layer_names.append(datastream["properties"]["layerName"])
        return layer_names
        
    def get_things(self):
        print(f"Amount of things: {len(self.things)}")
        return self.things
    
    def filter_only_primary_signal_datastreams(self):
        for i in range(len(self.things)):
            datastreams = []
            thing = self.things[i]
            for datastream in thing["Datastreams"]:
                if datastream["properties"]["layerName"] == "primary_signal":
                    datastreams.append(datastream)
            self.things[i]["Datastreams"] = datastreams
            
    def filter_only_cycle_second_datastreams(self):
        for i in range(len(self.things)):
            datastreams = []
            thing = self.things[i]
            for datastream in thing["Datastreams"]:
                if datastream["properties"]["layerName"] == "cycle_second":
                    datastreams.append(datastream)
            self.things[i]["Datastreams"] = datastreams
            
    def filter_only_program_signal_datastreams(self):
        for i in range(len(self.things)):
            datastreams = []
            thing = self.things[i]
            for datastream in thing["Datastreams"]:
                if datastream["properties"]["layerName"] == "program_signal":
                    datastreams.append(datastream)
            self.things[i]["Datastreams"] = datastreams
        
    def get_datastreams(self):
        datastreams = []
        for thing in self.things:
            for datastream in thing["Datastreams"]:
                datastreams.append(datastream)
                
        print(f"Amount of datastreams: {len(datastreams)}")
        return datastreams
    
    def get_datastream_ids(self):
        datastream_ids = []
        for datastream in self.get_datastreams():
            datastream_ids.append(datastream["@iot.id"])
        print(f"Amount of datastream ids: {len(datastream_ids)}")
        return datastream_ids