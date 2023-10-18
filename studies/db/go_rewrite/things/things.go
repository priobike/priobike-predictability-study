package things

import (
	"encoding/json"
	"os"
)

type ThingsProvider struct {
	Things []Thing
}

type Thing struct {
	Name string `json:"name"`
	Properties ThingProperties `json:"properties"`
}

func NewThingsProvider() *ThingsProvider {
    p := new(ThingsProvider)
	thingsFile := "things.json"
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

type ThingProperties struct {
	LaneType string `json:"laneType"`
}