package observations

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sink/csv"
	"sink/db"
	"sink/env"
	"sink/log"
	"sink/structs"
	"sink/things"
	"sync"
	"sync/atomic"
	"time"
)

const layout = "2006-01-02T15:04:05.000Z"

var noPreviousObservationsCount uint64 = 0

func fetchRecentObservationsPageCsv(page int, client *http.Client, resultTimeLowerBound time.Time, resultTimeUpperBound time.Time) (more bool) {
	elementsPerPage := 100
	// https://tld.iot.hamburg.de/v1.1/Datastreams?$filter=properties/serviceName%20eq%20%27HH_STA_traffic_lights%27&$expand=Thing,Observations($orderby=phenomenonTime;$top=1000)
	pageUrl := env.SensorThingsBaseUrl + "Datastreams?" + url.QueryEscape(
		"$filter="+
			"properties/serviceName eq 'HH_STA_traffic_lights' "+
			"&$expand=Thing,Observations($orderby=phenomenonTime;"+
			"$filter=resultTime ge "+resultTimeLowerBound.Format(layout)+" and resultTime le "+resultTimeUpperBound.Format(layout)+
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
			Observations []structs.Observation `json:"Observations"`
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
			success := csv.StoreObservation(observation, expandedDatastream.Properties.LayerName, expandedDatastream.Thing.Name, false)
			if !success {
				panic("Could not store observation")
			}
		}
	}
	return observationsResponse.NextUri != nil
}

// Fetch the most recent observations for all datastreams.
func fetchMostRecentObservationsCsv(resultTimeLowerBound time.Time, resultTimeUpperBound time.Time) {
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
				more := fetchRecentObservationsPageCsv(page, clients[i], resultTimeLowerBound, resultTimeUpperBound)
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
func FetchObservationsCsv() {
	// Get current time.
	currentTime := time.Now()
	var resultTimeUpperBound = currentTime
	var resultTimeLowerBound = currentTime.Add(-time.Minute * 5)
	var syncCounter = 0
	for {
		log.Info.Printf("Synced observations %d times.", syncCounter)
		log.Info.Printf("Fetching observations from %s to %s...", resultTimeLowerBound.Format(layout), resultTimeUpperBound.Format(layout))
		fetchMostRecentObservationsCsv(resultTimeLowerBound, resultTimeUpperBound)
		resultTimeLowerBound = resultTimeUpperBound
		resultTimeUpperBound = time.Now()
		syncCounter++
	}
}

func FetchObservationsDb() {
	datastreamIds := []int{}

	things.Things.Range(func(key, value interface{}) bool {
		for _, d := range value.(things.Thing).Datastreams {
			datastreamIds = append(datastreamIds, d.IotId)
		}
		return true
	})

	log.Info.Printf("Fetching observations for %d datastreams...", len(datastreamIds))

	for {
		fetchMostRecentObservationsDb(datastreamIds)
	}
}

func fetchMostRecentObservationsDb(datastreamIds []int) {
	log.Info.Println("Fetching most recent observations...")

	clientCount := 10
	clients := make([]*http.Client, clientCount)
	for i := 0; i < clientCount; i++ {
		tr := &http.Transport{DisableKeepAlives: false}
		clients[i] = &http.Client{Transport: tr}
	}

	// Split topics into 10 lists.
	datastreamIdsLists := make([][]int, 10)
	for i := 0; i < 10; i++ {
		datastreamIdsLists[i] = []int{}
	}
	for i, datasteamId := range datastreamIds {
		datastreamIdsLists[i%10] = append(datastreamIdsLists[i%10], datasteamId)
	}

	log.Info.Printf("Splitted up datastreams into %d lists.", len(datastreamIdsLists))
	for i, datastreamIdsList := range datastreamIdsLists {
		log.Info.Printf("List %d has %d datastreams.", i, len(datastreamIdsList))
	}

	// Fetch all datastreams of the SensorThings query.
	var datastreamIdx = 0
	var cycle = 0
	// Start timer
	start := time.Now()
	for {
		// If cycle %1000 == 0, print average time per cycle.
		if cycle%1000 == 0 {
			elapsed := time.Since(start)
			log.Info.Printf("Fetching a single datastream took on average %s, %d of %d datastreams fetched. For %d of %d datastreams there were no previous observations existent.", elapsed/10000, datastreamIdx, len(datastreamIds), noPreviousObservationsCount, 10000)
			start = time.Now()
			atomic.StoreUint64(&noPreviousObservationsCount, 0)
		}
		// Make some parallel requests to speed things up.
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int, cycle int) {
				defer wg.Done()
				if cycle >= len(datastreamIdsLists[i]) {
					return
				}
				startSingle := time.Now()
				notFound := fetchRecentObservationsPageDb(clients[i], datastreamIdsLists[i][cycle])
				if notFound {
					atomic.AddUint64(&noPreviousObservationsCount, 1)
				}
				elapsedSingle := time.Since(startSingle)
				if elapsedSingle < 30*time.Millisecond {
					// Wait 30ms between each request to avoid overloading the SensorThings API.
					time.Sleep(30*time.Millisecond - elapsedSingle)
				}
			}(i, cycle)
			datastreamIdx++
		}
		wg.Wait()
		if datastreamIdx >= len(datastreamIds) {
			break
		}
		cycle++
	}

	log.Info.Println("Fetched most recent observations.")
}

// Returns a boolean that indicates whether no previous observation for this datastream id was found.
func fetchRecentObservationsPageDb(client *http.Client, datastreamId int) bool {
	latestPhenomenonTime, notFound := db.GetLatestPhenomenonTimeForDatastream(datastreamId)
	latestPhenomenonTimeObj := time.Unix(int64(latestPhenomenonTime), 0)
	pageUrl := env.SensorThingsBaseUrl + "Datastreams?" + url.QueryEscape(
		"$filter="+
			"id eq "+fmt.Sprintf("%d", datastreamId)+
			"&$expand=Thing,Observations("+
			"$filter=resultTime ge "+latestPhenomenonTimeObj.Format(layout)+
			")",
	)

	resp, err := client.Get(pageUrl)
	if err != nil {
		log.Warning.Println("Could not sync observation:", err)
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
			Observations []structs.Observation `json:"Observations"`
		} `json:"value"`
		NextUri *string `json:"@iot.nextLink"`
	}
	if err := json.Unmarshal(body, &observationsResponse); err != nil {
		log.Warning.Println("Could not sync observations:", err)
		log.Warning.Println(string(body))
		panic(err)
	}

	if len(observationsResponse.Value) > 1 {
		panic("More than one datastream (indicates wrong API call)")
	}

	for _, expandedDatastream := range observationsResponse.Value {
		if len(expandedDatastream.Observations) == 0 {
			continue
		}
		// Store all observations in the database.
		db.StoreObservationsHttp(expandedDatastream.Observations, datastreamId)
	}
	return notFound
}
