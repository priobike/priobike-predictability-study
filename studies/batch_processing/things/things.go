package things

import (
	"encoding/json"
	"os"
)

type ThingsProvider struct {
	Things []TLDThing
}

type TLDThing struct {
	Name        string          `json:"name"`
	Properties  ThingProperties `json:"properties"`
	Datastreams []Datastream    `json:"Datastreams"`
}

type ThingProperties struct {
	LaneType string `json:"laneType"`
}

type Datastream struct {
	ID         int32                `json:"@iot.id"`
	Properties DatastreamProperties `json:"properties"`
}

type DatastreamProperties struct {
	LayerName string `json:"layerName"`
}

func NewThingsProvider(testing bool) *ThingsProvider {
	p := new(ThingsProvider)
	thingsFile := "things/things.json"
	if testing {
		thingsFile = "things.json"
	}
	thingsData, fileErr := os.ReadFile(thingsFile)
	if fileErr != nil {
		panic(fileErr)
	}
	jsonErr := json.Unmarshal(thingsData, &p.Things)
	if jsonErr != nil {
		panic(jsonErr)
	}
	return p
}

func (thingsProvider *ThingsProvider) FilterOnlyPrimarySignalAndCycleSecondDatastreams() {
	for i, thing := range thingsProvider.Things {
		datastreams := []Datastream{}
		for _, datastream := range thing.Datastreams {
			layerName := &datastream.Properties.LayerName
			if *layerName == "primary_signal" || *layerName == "cycle_second" {
				datastreams = append(datastreams, datastream)
			}
		}
		thingsProvider.Things[i].Datastreams = datastreams
	}
}

func (thingsProvider *ThingsProvider) FilterOnlySecondarySignalAndCycleSecondDatastreams() {
	for i, thing := range thingsProvider.Things {
		datastreams := []Datastream{}
		for _, datastream := range thing.Datastreams {
			layerName := &datastream.Properties.LayerName
			if *layerName == "secondary_signal" || *layerName == "cycle_second" {
				datastreams = append(datastreams, datastream)
			}
		}
		thingsProvider.Things[i].Datastreams = datastreams
	}
}

func (thingsProvider *ThingsProvider) FilterOnlyPrimarySignalSecondarySignalAndCycleSecondDatastreams() {
	for i, thing := range thingsProvider.Things {
		datastreams := []Datastream{}
		for _, datastream := range thing.Datastreams {
			layerName := &datastream.Properties.LayerName
			if *layerName == "primary_signal" || *layerName == "secondary_signal" || *layerName == "cycle_second" {
				datastreams = append(datastreams, datastream)
			}
		}
		thingsProvider.Things[i].Datastreams = datastreams
	}
}
