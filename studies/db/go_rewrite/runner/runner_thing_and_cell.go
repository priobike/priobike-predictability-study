package runner

import (
	"encoding/json"
	"os"
	"studies/db"
	"studies/things"
	"studies/times"
)

func RunThingAndCell() {
	tp := things.NewThingsProvider(false)
	tp.FilterOnlyPrimarySignalAndCycleSecondDatastreams()
	tldThings := tp.Things
	dbPool := db.NewPool()
	dbClient := dbPool.GetClient()
	processedThings := []*things.Thing{}
	validationActive := false
	retrieveAllCycleCleanupStats := false
	println("Processing", len(tldThings), "things")
	for _, tldThing := range tldThings {
		thing := things.NewThing(
			tldThing.Name,
			validationActive,
			retrieveAllCycleCleanupStats,
		)
		var primarySignalDatastreamId *int32
		var cycleSecondDatastreamId *int32
		for i := 0; i < len(tldThing.Datastreams); i++ {
			if tldThing.Datastreams[i].Properties.LayerName == "primary_signal" {
				primarySignalDatastreamId = &tldThing.Datastreams[i].ID
			} else if tldThing.Datastreams[i].Properties.LayerName == "cycle_second" {
				cycleSecondDatastreamId = &tldThing.Datastreams[i].ID
			} else {
				panic("Unknown layer name")
			}
		}

		datastreamIds := []int32{}
		if primarySignalDatastreamId != nil {
			datastreamIds = append(datastreamIds, *primarySignalDatastreamId)
		}
		if cycleSecondDatastreamId != nil {
			datastreamIds = append(datastreamIds, *cycleSecondDatastreamId)
		}
		cells := times.GetCells()
		for dayIdx, day := range cells {
			for hourIdx, hour := range day {
				println("Processing day", dayIdx, "hour", hourIdx)
				// println(" ")
				//fmt.Print("\033[s")
				observationCount := 0
				for cellIdx, cell := range hour {
					query := db.GetCellQuery(datastreamIds, cell)
					rows := dbClient.Query(query)
					for rows.Next() {
						observationCount++
						// fmt.Print("\033[u\033[K")
						// fmt.Printf("Processing observation %d", observationCount)
						var phenomenon_time int32
						var result int16
						var datastream_id int32
						err := rows.Scan(&phenomenon_time, &result, &datastream_id)
						if err != nil {
							panic(err)
						}
						layerName := ""
						if datastream_id == *primarySignalDatastreamId {
							layerName = "primary_signal"
						} else if datastream_id == *cycleSecondDatastreamId {
							layerName = "cycle_second"
						} else {
							panic("Unknown datastream id")
						}
						thing.AddObservation(layerName, phenomenon_time, result)
					}
					rows.Close()
					thing.CalcCycles(cellIdx)
				}
				println(" ")
				thing.CalculateMetrics(dayIdx, hourIdx)
			}
		}
		processedThings = append(processedThings, thing)
	}

	dbClient.Close()
	dbPool.Close()
	// Output processed things as json file
	file, _ := json.MarshalIndent(processedThings, "", " ")
	_ = os.WriteFile("processed_things.json", file, 0644)
}
