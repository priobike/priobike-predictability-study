package runner

import (
	"studies/things"
	"studies/db"
	"studies/times"
)

func RunThingAndCell() {
	tp := things.NewThingsProvider()
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
				for _, cell := range hour {
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
					thing.CalcCycles()
				}
				println(" ")
				thing.CalculateMetrics(dayIdx, hourIdx)
			}
		}
		processedThings = append(processedThings, thing)
		break
	}

	dbClient.Close()
	dbPool.Close()
}