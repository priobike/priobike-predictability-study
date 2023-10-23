package runner

import (
	"studies/things"
	"studies/db"
	"studies/times"
)

type ObservationFromDb struct {
	PhenomenonTime int32
	Result int8
	DatastreamId int32
}

func Run() {
	/* TODO Fehler  

	Processing day 1 hour 3
 
	Processing day 1 hour 4
	panic: runtime error: index out of range [0] with length 0

	goroutine 1 [running]:
	studies/things.(*Thing).reconstructCycles(0xc00797a000)
			/home/admin/priobike-observation-sink-studies/go_rewrite/things/thing.go:277 +0x839
	studies/things.(*Thing).CalcCycles(0xc00797a000)
			/home/admin/priobike-observation-sink-studies/go_rewrite/things/thing.go:179 +0x1c
	studies/runner.Run()
			/home/admin/priobike-observation-sink-studies/go_rewrite/runner/runner.go:110 +0x9ff
	main.main()
			/home/admin/priobike-observation-sink-studies/go_rewrite/main.go:8 +0xf
	exit status 2 */

	tp := things.NewThingsProvider()
	tp.FilterOnlyPrimarySignalAndCycleSecondDatastreams()
	tldThings := tp.Things
	dbClient := db.NewClient()
	defer dbClient.Close()
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

		query := db.GetThingQuery(datastreamIds)
		rows := dbClient.Query(query)

		observations := []*ObservationFromDb{}

		for rows.Next() {
			var phenomenon_time int32
			var result int8
			var datastream_id int32
			err := rows.Scan(&phenomenon_time, &result, &datastream_id)
			if err != nil {
				panic(err)
			}
			observations = append(observations, &ObservationFromDb{
				PhenomenonTime: phenomenon_time,
				Result: result,
				DatastreamId: datastream_id,
			})
		}

		rows.Close()

		cells := times.GetCells()
		observationsByCell := [7][24][4][]*ObservationFromDb{}

		for cellIdx := 0; cellIdx < 4; cellIdx++ {
			for dayIdx, day := range cells {
				for hourIdx, hour := range day {
					start := hour[cellIdx][0]
					end := hour[cellIdx][1]
					for _, observation := range observations {
						if observation.PhenomenonTime >= start && observation.PhenomenonTime < end {
							observationsByCell[dayIdx][hourIdx][cellIdx] = append(observationsByCell[dayIdx][hourIdx][cellIdx], observation)
						}
					}
				}
			}
		}
		for dayIdx, day := range observationsByCell {
			for hourIdx, hour := range day {
				println("Processing day", dayIdx, "hour", hourIdx)
				// println(" ")
				//fmt.Print("\033[s")
				observationCount := 0
				for _, cell := range hour {
					for _, observation := range cell {
						observationCount++
						// fmt.Print("\033[u\033[K")
						// fmt.Printf("Processing observation %d", observationCount)
						layerName := ""
						if (*observation).DatastreamId == *primarySignalDatastreamId {
							layerName = "primary_signal"
						} else if (*observation).DatastreamId == *cycleSecondDatastreamId {
							layerName = "cycle_second"
						} else {
							panic("Unknown datastream id")
						}
						thing.AddObservation(layerName, (*observation).PhenomenonTime, (*observation).Result)
					}
					thing.CalcCycles()
				}
				println(" ")
				thing.CalculateMetrics(dayIdx, hourIdx)
			}
		}
		processedThings = append(processedThings, thing)
		break
	}


}