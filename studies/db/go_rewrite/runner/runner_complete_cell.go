package runner

import (
	// "time"
	"encoding/json"
	"io/ioutil"
	"sync"

	"studies/things"
	"studies/db"
	"studies/times"
)

type ObservationFromDb struct {
	PhenomenonTime int32
	Result int16
	DatastreamId int32
}

func RunCompleteCell() {
	processedThingsByRoutines := [7]map[string]*things.Thing{}
	for idx, _ := range processedThingsByRoutines {
		processedThingsByRoutines[idx] = map[string]*things.Thing{}
	}

	validationActive := true
	retrieveAllCycleCleanupStats := false

	tp := things.NewThingsProvider()
	tp.FilterOnlyPrimarySignalAndCycleSecondDatastreams()
	tldThings := tp.Things
	datastreamsByThingName := map[string]map[string]int32{}
	thingsByDatastreamId := map[int32]string{}
	for _, tldThing := range tldThings {
		for idx, _ := range processedThingsByRoutines {	
			thing := things.NewThing(
				tldThing.Name,
				validationActive,
				retrieveAllCycleCleanupStats,
			)
			processedThingsByRoutines[idx][tldThing.Name] = thing
		}
		for i := 0; i < len(tldThing.Datastreams); i++ {
			if tldThing.Datastreams[i].Properties.LayerName == "primary_signal" {
				if _, ok := datastreamsByThingName[tldThing.Name]; !ok {
					datastreamsByThingName[tldThing.Name] = map[string]int32{}
				}
				datastreamsByThingName[tldThing.Name]["primary_signal"] = tldThing.Datastreams[i].ID
				thingsByDatastreamId[tldThing.Datastreams[i].ID] = tldThing.Name
			} else if tldThing.Datastreams[i].Properties.LayerName == "cycle_second" {
				if _, ok := datastreamsByThingName[tldThing.Name]; !ok {
					datastreamsByThingName[tldThing.Name] = map[string]int32{}
				}
				datastreamsByThingName[tldThing.Name]["cycle_second"] = tldThing.Datastreams[i].ID
				thingsByDatastreamId[tldThing.Datastreams[i].ID] = tldThing.Name
			} else {
				panic("Unknown layer name")
			}
		}
	}

	pool := db.NewPool()

	var wg sync.WaitGroup
	// startTime := time.Now()
	times := times.GetCells()
	for i := 0; i < len(times); i++ {
		client := pool.GetClient()
		wg.Add(1)
		go func(things *map[string]*things.Thing, dayIdx int, day [24][4][2]int32, dbClient *db.Client) {
            defer wg.Done()
			for hourIdx, hour := range day {
				println("Processing day", dayIdx, "hour", hourIdx)
				cells := [4][2]int32{}
				for cellIdx, cell := range hour {
					// println(cell[0], " ", cell[1])
					cells[4- 1 -cellIdx] = [2]int32{cell[0], cell[1]}
				}
				query := db.GetCellsAllDatastreamsQuery(cells)
				rows := dbClient.Query(query)
				// println("Receiving observations..")
				currentCellIdx := 0
				observationCount := 0
				// rowCount := 0
				for rows.Next() {
					var phenomenon_time int32
					var result int16
					var datastream_id int32
					err := rows.Scan(&phenomenon_time, &result, &datastream_id)
					if err != nil {
						panic(err)
					}
					if phenomenon_time > cells[currentCellIdx][1] {
						currentCellIdx++
						for _, thing := range *things {
							thing.CalcCycles()
						}
						// println("Calc Cycles")
					}
					thingName := thingsByDatastreamId[datastream_id]
					thing := (*things)[thingName]
					layerName := ""
					if datastream_id == datastreamsByThingName[thingName]["primary_signal"] {
						layerName = "primary_signal"
					} else if datastream_id == datastreamsByThingName[thingName]["cycle_second"] {
						layerName = "cycle_second"
					} else {
						continue
					}
					observationCount++
					thing.AddObservation(layerName, phenomenon_time, result)
					/* rowCount++
					if rowCount % 100000 == 0 {	
						println("Processed observations: ", rowCount)
					} */
				}
				println("Observation count: ", observationCount)
				rows.Close()
				for _, thing := range *things {
					thing.CalcCycles()
					thing.CalculateMetrics(dayIdx, hourIdx)
				}
				// println("Processed observations")
				// endTime := time.Now()
				// elapsed := endTime.Sub(startTime)
				// println("Elapsed time in seconds: ", elapsed.Seconds())
			}
			dbClient.Close()
		}(&processedThingsByRoutines[i], i, times[i], client)
	}

	wg.Wait()

	pool.Close()

	processedThings := map[string]*things.Thing{}
	for dayIdx, processedThingsByRoutine := range processedThingsByRoutines {
		for thingName, thing := range processedThingsByRoutine {
			if _, ok := processedThings[thingName]; !ok {
				processedThings[thingName] = thing
			} else {	
				processedThings[thingName].PrimarySignalMissingCount += thing.PrimarySignalMissingCount
				processedThings[thingName].CycleSecondMissingCount += thing.CycleSecondMissingCount
				processedThings[thingName].TotalSkippedCycles += thing.TotalSkippedCycles
				processedThings[thingName].TotalCyclesCount += thing.TotalCyclesCount
				processedThings[thingName].TotalRemovedCycleCount += thing.TotalRemovedCycleCount
				processedThings[thingName].Metrics[dayIdx] = thing.Metrics[dayIdx]
			}
		}
	}

	// Output processed things as json file
	file, _ := json.MarshalIndent(processedThings, "", " ")
	_ = ioutil.WriteFile("processed_things.json", file, 0644)
}