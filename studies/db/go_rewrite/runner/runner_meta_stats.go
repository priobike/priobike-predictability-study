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

func RunMetaStats(tldThings []things.TLDThing) (uint64, uint64, []string) {
	statsByRoutines := [7]map[string]uint64{}
	thingsWithObservations := [7][]string{}

	for idx := range statsByRoutines {
		statsByRoutines[idx] = map[string]uint64{
			"ps_observation_count": 0,
			"cs_observation_count": 0,
		}
		thingsWithObservations[idx] = []string{}
	}

	datastreamsByThingName := map[string]map[string]int32{}
	thingsByDatastreamId := map[int32]string{}
	for _, tldThing := range tldThings {
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
		go func(stats *map[string]uint64, thingsWithObservations *[]string, dayIdx int, day [24][4][2]int32, dbClient *db.Client) {
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

						// println("Calc Cycles")
					}
					thingName := thingsByDatastreamId[datastream_id]
					if datastream_id == datastreamsByThingName[thingName]["primary_signal"] {
						(*stats)["ps_observation_count"]++
					} else if datastream_id == datastreamsByThingName[thingName]["cycle_second"] {
						(*stats)["cs_observation_count"]++
					} else {
						continue
					}
					observationCount++
					/* rowCount++
					if rowCount % 100000 == 0 {
						println("Processed observations: ", rowCount)
					} */
				}
				println("Observation count: ", observationCount)
				rows.Close()
				// println("Processed observations")
				// endTime := time.Now()
				// elapsed := endTime.Sub(startTime)
				// println("Elapsed time in seconds: ", elapsed.Seconds())
			}
			dbClient.Close()
		}(&statsByRoutines[i], &thingsWithObservations[i], i, times[i], client)
	}

	wg.Wait()

	pool.Close()

	stats := map[string]uint64{}
	for _, statsByRoutines := range statsByRoutines {
		if _, ok := stats["ps_observation_count"]; !ok {
			stats["ps_observation_count"] = statsByRoutines["ps_observation_count"]
		} else {
			stats["ps_observation_count"] += statsByRoutines["ps_observation_count"]
		}
		if _, ok := stats["cs_observation_count"]; !ok {
			stats["cs_observation_count"] = statsByRoutines["cs_observation_count"]
		} else {
			stats["cs_observation_count"] += statsByRoutines["cs_observation_count"]
		}
	}

	things := []string{}
	for _, thingsWithObservations := range thingsWithObservations {
		for _, thing := range thingsWithObservations {
			if !stringInSlice(things, thing) {
				things = append(things, thing)
			}
		}
	}

	return stats["ps_observation_count"], stats["cs_observation_count"], things
}

func stringInSlice(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func RunMeta() {
	tp := things.NewThingsProvider(false)
	tp.FilterOnlyPrimarySignalAndCycleSecondDatastreams()
	tldThingsPrimary := tp.Things

	psObservationCountTotal := uint64(0)
	csObservationCountTotal := uint64(0)
	thingsWithObservationsTotal := []string{}

	psObservationCount, csObservationCount, thingsWithObservations := RunMetaStats(tldThingsPrimary)

	thingsWithObservationsTotal = append(thingsWithObservationsTotal, thingsWithObservations...)

	psObservationCountTotal += psObservationCount
	csObservationCountTotal += csObservationCount

	tp = things.NewThingsProvider(false)
	tp.FilterOnlySecondarySignalAndCycleSecondDatastreams()
	tldThingsSecondary := tp.Things

	psObservationCount, csObservationCount, thingsWithObservations = RunMetaStats(tldThingsSecondary)

	for _, thing := range thingsWithObservations {
		if !stringInSlice(thingsWithObservationsTotal, thing) {
			thingsWithObservationsTotal = append(thingsWithObservationsTotal, thing)
		}
	}

	psObservationCountTotal += psObservationCount
	csObservationCountTotal += csObservationCount

	meta_stats := map[string]uint64{
		"ps_observation_count_total": psObservationCountTotal,
		"cs_observation_count_total": csObservationCountTotal,
		"things_with_observations":   uint64(len(thingsWithObservationsTotal)),
	}

	// Output processed things as json file
	file, _ := json.MarshalIndent(meta_stats, "", " ")
	_ = os.WriteFile("meta_stats.json", file, 0644)
}
