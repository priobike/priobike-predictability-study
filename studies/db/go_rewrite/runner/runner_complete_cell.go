package runner

import (
	// "time"
	"encoding/json"
	"os"
	"sync"

	"studies/db"
	"studies/things"
	"studies/times"
)

type ObservationFromDb struct {
	PhenomenonTime int32
	Result         int16
	DatastreamId   int32
}

func RunCompleteCell(tldThings []things.TLDThing, suffixName string) map[string]*things.Thing {
	processedThingsByRoutines := [7]map[string]*things.Thing{}
	for idx := range processedThingsByRoutines {
		processedThingsByRoutines[idx] = map[string]*things.Thing{}
	}

	validationActive := true
	retrieveAllCycleCleanupStats := true

	datastreamsByThingName := map[string]map[string]int32{}
	thingsByDatastreamId := map[int32]string{}
	for _, tldThing := range tldThings {
		for idx := range processedThingsByRoutines {
			thing := things.NewThing(
				tldThing.Name,
				validationActive,
				retrieveAllCycleCleanupStats,
			)
			processedThingsByRoutines[idx][tldThing.Name] = thing
		}
		for i := 0; i < len(tldThing.Datastreams); i++ {
			if tldThing.Datastreams[i].Properties.LayerName == "primary_signal" || tldThing.Datastreams[i].Properties.LayerName == "secondary_signal" {
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
					cells[cellIdx] = [2]int32{cell[0], cell[1]}
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
							thing.CalcCycles(currentCellIdx)
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
					thing.CalcCycles(currentCellIdx)
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
			name := thingName + suffixName
			if _, ok := processedThings[name]; !ok {
				processedThings[name] = thing
			} else {
				processedThings[name].PrimarySignalMissingCount += thing.PrimarySignalMissingCount
				processedThings[name].CycleSecondMissingCount += thing.CycleSecondMissingCount
				processedThings[name].TotalSkippedCycles += thing.TotalSkippedCycles
				processedThings[name].TotalCyclesCount += thing.TotalCyclesCount
				processedThings[name].GapsBetweenCyclesCount += thing.GapsBetweenCyclesCount
				processedThings[name].TotalRemovedCycleCount += thing.TotalRemovedCycleCount
				processedThings[name].TotalInvalidCycleLengthCount += thing.TotalInvalidCycleLengthCount
				processedThings[name].TotalInvalidCycleTransitionCount += thing.TotalInvalidCycleTransitionCount
				processedThings[name].TotalInvalidCycleMissingCount += thing.TotalInvalidCycleMissingCount
				processedThings[name].Metrics[dayIdx] = thing.Metrics[dayIdx]
				processedThings[name].MetricsSP[dayIdx] = thing.MetricsSP[dayIdx]
				processedThings[name].MedianShifts[dayIdx] = thing.MedianShifts[dayIdx]
				processedThings[name].MetricsRelativeGreenDistance[dayIdx] = thing.MetricsRelativeGreenDistance[dayIdx]
				processedThings[name].MedianGreenLengths[dayIdx] = thing.MedianGreenLengths[dayIdx]
			}
		}
	}

	return processedThings
}

func Run() {
	processedThings := map[string]*things.Thing{}

	tp := things.NewThingsProvider(false)
	tp.FilterOnlyPrimarySignalAndCycleSecondDatastreams()
	tldThingsPrimary := tp.Things

	primaryProcessedThings := RunCompleteCell(tldThingsPrimary, "_primary")

	for thingName, thing := range primaryProcessedThings {
		processedThings[thingName] = thing
	}

	tp = things.NewThingsProvider(false)
	tp.FilterOnlySecondarySignalAndCycleSecondDatastreams()
	tldThingsSecondary := tp.Things

	secondaryProcessedThings := RunCompleteCell(tldThingsSecondary, "_secondary")

	for thingName, thing := range secondaryProcessedThings {
		processedThings[thingName] = thing
	}

	// Output processed things as json file
	file, _ := json.MarshalIndent(processedThings, "", " ")
	_ = os.WriteFile("processed_things.json", file, 0644)
}
