package observations

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sink/env"
	"sink/log"
	"sync"
	"time"
)

const layout = "2006-01-02T15:04:05.000Z"

func fetchRecentObservationsPage(page int, client *http.Client, resultTimeLowerBound time.Time, resultTimeUpperBound time.Time) (more bool) {
	elementsPerPage := 100
	// https://tld.iot.hamburg.de/v1.1/Datastreams?$filter=properties/serviceName%20eq%20%27HH_STA_traffic_lights%27&$expand=Thing,Observations($orderby=phenomenonTime;$top=1000)
	pageUrl := env.SensorThingsBaseUrl + "Datastreams?" + url.QueryEscape(
		"$filter="+
			"properties/serviceName eq 'HH_STA_traffic_lights' "+
			"&$expand=Thing,Observations($orderby=phenomenonTime;"+
			"$filter=resultTime ge "+fmt.Sprintf("%s", resultTimeLowerBound.Format(layout))+" and resultTime le "+fmt.Sprintf("%s", resultTimeUpperBound.Format(layout))+
			")"+
			"&$skip="+fmt.Sprintf("%d", page*elementsPerPage),
	)
	// Start timer
	resp, err := client.Get(pageUrl)
	if err != nil {
		log.Warning.Println("Could not sync observations:", err)
		panic(err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Warning.Println("Could not sync observations:", err)
		panic(err)
	}

	var observationsResponse struct {
		Value []struct {
			DatastreamId int `json:"@iot.id"`
			Properties   struct {
				LayerName string `json:"layerName"`
			}
			Thing struct {
				Name string `json:"name"`
			}
			Observations []Observation `json:"Observations"`
		} `json:"value"`
		NextUri *string `json:"@iot.nextLink"`
	}
	if err := json.Unmarshal(body, &observationsResponse); err != nil {
		log.Warning.Println("Could not sync observations:", err)
		log.Warning.Println(string(body))
		panic(err)
	}

	for _, expandedDatastream := range observationsResponse.Value {
		if len(expandedDatastream.Observations) == 0 {
			continue
		}
		// Store all observations in their respective files.
		for _, observation := range expandedDatastream.Observations {
			success := storeObservation(observation, expandedDatastream.Properties.LayerName, expandedDatastream.Thing.Name, false)
			if !success {
				panic("Could not store observation")
			}
		}
	}
	return observationsResponse.NextUri != nil
}

// Fetch the most recent observations for all datastreams.
func fetchMostRecentObservations(resultTimeLowerBound time.Time, resultTimeUpperBound time.Time) {
	log.Info.Println("Fetching most recent observations...")

	clientCount := 10
	clients := make([]*http.Client, clientCount)
	for i := 0; i < clientCount; i++ {
		tr := &http.Transport{DisableKeepAlives: false}
		clients[i] = &http.Client{Transport: tr}
	}

	// Fetch all pages of the SensorThings query.
	var page = 0
	for {
		// Start timer
		start := time.Now()
		// Make some parallel requests to speed things up.
		var wg sync.WaitGroup
		var foundMore = false
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(page int, i int) {
				defer wg.Done()
				more := fetchRecentObservationsPage(page, clients[i], resultTimeLowerBound, resultTimeUpperBound)
				if more {
					foundMore = true
				}
			}(page, i)
			page++
		}
		log.Info.Printf("Bulk fetching observations from pages %d-%d...", page-10, page-1)
		wg.Wait()
		// Stop timer
		elapsed := time.Since(start)
		log.Info.Printf("Bulk fetching observations took %s", elapsed)
		if !foundMore {
			break
		}
	}

	log.Info.Println("Fetched most recent observations.")
}

// Run the routine forever.
func FetchObservations() {
	// Get current time.
	currentTime := time.Now()
	var resultTimeUpperBound = currentTime
	var resultTimeLowerBound = currentTime.Add(-time.Minute * 5)
	var syncCounter = 0
	for {
		log.Info.Printf("Synced observations %d times.", syncCounter)
		log.Info.Printf("Fetching observations from %s to %s...", resultTimeLowerBound.Format(layout), resultTimeUpperBound.Format(layout))
		fetchMostRecentObservations(resultTimeLowerBound, resultTimeUpperBound)
		resultTimeLowerBound = resultTimeUpperBound
		resultTimeUpperBound = time.Now()
		syncCounter++
	}
}
