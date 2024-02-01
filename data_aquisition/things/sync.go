package things

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sink/env"
	"sink/log"
	"sync"
)

// A map that contains all things by their name.
var Things = &sync.Map{}

// Count the number of things that have been synced.
func CountThings() int {
	count := 0
	Things.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// A map that contains all thing names by their crossing id.
var Crossings = &sync.Map{}

// A map that contains all datastream MQTT topics to subscribe to, by their type.
var DatastreamMqttTopics = &sync.Map{}

// A map that points Datastream MQTT topics to Thing names.
var Datastreams = &sync.Map{}

// A map that contains all datastream IDs by their MQTT topic.
var DatastreamIds = &sync.Map{}

func syncThingsPage(page int) (more bool) {
	elementsPerPage := 100
	pageUrl := env.SensorThingsBaseUrl + "Things?" + url.QueryEscape(
		"$filter="+
			"Datastreams/properties/serviceName eq 'HH_STA_traffic_lights' "+
			"&$expand=Datastreams,Locations"+
			"&$skip="+fmt.Sprintf("%d", page*elementsPerPage),
	)

	resp, err := http.Get(pageUrl)
	if err != nil {
		log.Warning.Println("Could not sync things:", err)
		panic(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Warning.Println("Could not sync things:", err)
		panic(err)
	}

	var thingsResponse struct {
		Value   []Thing `json:"value"`
		NextUri *string `json:"@iot.nextLink"`
	}
	if err := json.Unmarshal(body, &thingsResponse); err != nil {
		log.Warning.Println("Could not sync things:", err)
		panic(err)
	}

	for _, t := range thingsResponse.Value {
		// Add the thing to the things map.
		Things.Store(t.Name, t)

		// Add the thing name to the crossing map.
		cs, _ := Crossings.LoadOrStore(t.Properties.TrafficLightsId, []string{})
		cs = append(cs.([]string), t.Name)
		Crossings.Store(t.Properties.TrafficLightsId, cs)

		for _, d := range t.Datastreams {
			DatastreamMqttTopics.Store(d.MqttTopic(), d.Properties.LayerName)
			Datastreams.Store(d.MqttTopic(), t.Name)
			DatastreamIds.Store(d.MqttTopic(), d.IotId)
		}
	}

	return thingsResponse.NextUri != nil
}

// Periodically sync the things from the SensorThings API.
func SyncThings() {
	log.Info.Println("Syncing things...")

	// Fetch all pages of the SensorThings query.
	var page = 0
	for {
		// Make some parallel requests to speed things up.
		var wg sync.WaitGroup
		var foundMore = false
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(page int) {
				defer wg.Done()
				more := syncThingsPage(page)
				if more {
					foundMore = true
				}
			}(page)
			page++
		}
		log.Info.Printf("Bulk syncing things from pages %d-%d...", page-10, page-1)
		wg.Wait()
		if !foundMore {
			break
		}
	}

	log.Info.Println("Synced things.")
}
