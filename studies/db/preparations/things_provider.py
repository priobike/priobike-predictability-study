import json
import os


class ThingsProvider:
    """
    Use this to get or filter things and datastreams from the things.json file.
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
                    thing["properties"]["laneType"] == "KFZ/Bus/Radfahrer":
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

    def filter_only_signal_program_datastreams(self):
        for i in range(len(self.things)):
            datastreams = []
            thing = self.things[i]
            for datastream in thing["Datastreams"]:
                if datastream["properties"]["layerName"] == "signal_program":
                    datastreams.append(datastream)
            self.things[i]["Datastreams"] = datastreams

    def filter_only_primary_signal_and_cycle_second_datastreams(self):
        for i in range(len(self.things)):
            datastreams = []
            thing = self.things[i]
            for datastream in thing["Datastreams"]:
                if datastream["properties"]["layerName"] == "primary_signal" or datastream["properties"]["layerName"] == "cycle_second":
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

    def get_coordinates_by_thing_name(self):
        thing_locations = {}
        for thing in self.things:
            try:
                thing_locations[thing["name"]
                                ] = thing["Locations"][0]["location"]["geometry"]["coordinates"][0][0]
            except:
                location_found = False
                # Try to extract location from observed area from datastreams
                for datastream in thing["Datastreams"]:
                    if "observedArea" not in datastream:
                        continue
                    if not isinstance(datastream["observedArea"]["coordinates"][0], list):
                        thing_locations[thing["name"]
                                        ] = datastream["observedArea"]["coordinates"]
                    else:
                        thing_locations[thing["name"]
                                        ] = datastream["observedArea"]["coordinates"][0]
                    location_found = True
                    break
                if not location_found:
                    path = os.path.dirname(os.path.abspath(__file__))
                    with open(f"{path}/last_thing_with_no_location_data.json", 'w') as fp:
                        json.dump(thing, fp)
                    print(
                        f"Location not found for thing {thing['name']}. Look at last_thing_with_no_location_data.json for more information.")
        return thing_locations

    def get_map_from_datastream_ids_to_thing_names_and_layer_names(self):
        map = {}
        for thing in self.things:
            for datastream in thing["Datastreams"]:
                map[datastream["@iot.id"]
                    ] = (thing["name"], datastream["properties"]["layerName"])
        return map
