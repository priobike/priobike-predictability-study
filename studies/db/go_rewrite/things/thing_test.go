package things

import (
	"testing"
	"time"
)

type Observation struct {
	phenomenonTime int32
	result         int16
}

func TestNewThing(t *testing.T) {
	name := "test_name"
	validation := true
	retrieveAllCycleCleanupStats := false
	thing := NewThing(name, validation, retrieveAllCycleCleanupStats)
	if thing.name != name {
		t.Errorf("Expected %s, got %s", name, thing.name)
	}
	if thing.validation != validation {
		t.Errorf("Expected %t, got %t", validation, thing.validation)
	}
	if thing.retrieveAllCycleCleanupStats != retrieveAllCycleCleanupStats {
		t.Errorf("Expected %t, got %t", retrieveAllCycleCleanupStats, thing.retrieveAllCycleCleanupStats)
	}
	name = "test_name2"
	validation = false
	retrieveAllCycleCleanupStats = true
	thing = NewThing(name, validation, retrieveAllCycleCleanupStats)
	if thing.name != name {
		t.Errorf("Expected %s, got %s", name, thing.name)
	}
	if thing.validation != validation {
		t.Errorf("Expected %t, got %t", validation, thing.validation)
	}
	if thing.retrieveAllCycleCleanupStats != retrieveAllCycleCleanupStats {
		t.Errorf("Expected %t, got %t", retrieveAllCycleCleanupStats, thing.retrieveAllCycleCleanupStats)
	}
}

func TestAddObservation(t *testing.T) {
	name := "test_name"
	validation := false
	retrieveAllCycleCleanupStats := false
	thing := NewThing(name, validation, retrieveAllCycleCleanupStats)

	phenomenonTimesPS := []int32{1698827378, 1696148973, 1696580973}
	resultsPS := []int16{2, 3, 4}
	phenomenonTimesCS := []int32{1696494573, 1698727379, 1698827780}
	resultsCS := []int16{0, 0, 0}

	for i := 0; i < len(phenomenonTimesPS); i++ {
		thing.AddObservation("primary_signal", phenomenonTimesPS[i], resultsPS[i])
	}
	for i := 0; i < len(phenomenonTimesCS); i++ {
		thing.AddObservation("cycle_second", phenomenonTimesCS[i], resultsCS[i])
	}

	if len(thing.observationsByDatastreams) != 2 {
		t.Errorf("Expected %d, got %d", 2, len(thing.observationsByDatastreams))
	}

	if _, ok := thing.observationsByDatastreams["primary_signal"]; !ok {
		t.Errorf("Expected %t, got %t", true, ok)
	}

	if _, ok := thing.observationsByDatastreams["cycle_second"]; !ok {
		t.Errorf("Expected %t, got %t", true, ok)
	}

	if len(thing.observationsByDatastreams["primary_signal"]) != len(phenomenonTimesPS) {
		t.Errorf("Expected %d, got %d", len(phenomenonTimesPS), len(thing.observationsByDatastreams["primary_signal"]))
	}

	if len(thing.observationsByDatastreams["cycle_second"]) != len(phenomenonTimesCS) {
		t.Errorf("Expected %d, got %d", len(phenomenonTimesCS), len(thing.observationsByDatastreams["cycle_second"]))
	}

	for i := 0; i < len(phenomenonTimesPS); i++ {
		if thing.observationsByDatastreams["primary_signal"][i].phenomenonTime != phenomenonTimesPS[i] {
			t.Errorf("Expected %d, got %d", phenomenonTimesPS[i], thing.observationsByDatastreams["primary_signal"][i].phenomenonTime)
		}
		if int16(thing.observationsByDatastreams["primary_signal"][i].result) != resultsPS[i] {
			t.Errorf("Expected %d, got %d", resultsPS[i], thing.observationsByDatastreams["primary_signal"][i].result)
		}
	}

	for i := 0; i < len(phenomenonTimesCS); i++ {
		if thing.observationsByDatastreams["cycle_second"][i].phenomenonTime != phenomenonTimesCS[i] {
			t.Errorf("Expected %d, got %d", phenomenonTimesCS[i], thing.observationsByDatastreams["cycle_second"][i].phenomenonTime)
		}
		if int16(thing.observationsByDatastreams["cycle_second"][i].result) != resultsCS[i] {
			t.Errorf("Expected %d, got %d", resultsCS[i], thing.observationsByDatastreams["cycle_second"][i].result)
		}
	}
}

func TestReconstructCycles(t *testing.T) {
	name := "test_name"
	validation := false
	retrieveAllCycleCleanupStats := false
	thing := NewThing(name, validation, retrieveAllCycleCleanupStats)

	location, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		panic(err)
	}

	primarySignalObservations := []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 2},
		{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 3},
		{int32(time.Date(2023, 10, 20, 0, 0, 20, 0, location).Unix()), 4},
		{int32(time.Date(2023, 10, 20, 0, 0, 35, 0, location).Unix()), 5},
		{int32(time.Date(2023, 10, 20, 0, 0, 42, 0, location).Unix()), 6},
		{int32(time.Date(2023, 10, 20, 0, 0, 50, 0, location).Unix()), 1},
		{int32(time.Date(2023, 10, 20, 0, 0, 55, 0, location).Unix()), 3},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 4},
		{int32(time.Date(2023, 10, 20, 0, 1, 10, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 20, 0, location).Unix()), 1},
	}

	cycleSecondObservations := []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 30, 0, location).Unix()), 0},
	}

	for i := 0; i < len(primarySignalObservations); i++ {
		thing.AddObservation("primary_signal", primarySignalObservations[i].phenomenonTime, primarySignalObservations[i].result)
	}

	for i := 0; i < len(cycleSecondObservations); i++ {
		thing.AddObservation("cycle_second", cycleSecondObservations[i].phenomenonTime, cycleSecondObservations[i].result)
	}

	cycles, skippedCycles, primarySignalMissing, cycleSecondMissing := thing.reconstructCycles()

	if skippedCycles > 0 {
		t.Errorf("Expected %d skipped cycles, got %d", 0, skippedCycles)
	}

	if primarySignalMissing {
		t.Errorf("Expected %t for primarySignalMissing, got %t", false, primarySignalMissing)
	}

	if cycleSecondMissing {
		t.Errorf("Expected %t for cycleSecondMissing, got %t", false, cycleSecondMissing)
	}

	if len(cycles) != 3 {
		println("Cycle results: ")
		for _, cycle := range cycles {
			println(cycle.start, " ", cycle.end)
			for _, result := range cycle.results {
				print(result)
			}
			println()
		}
		println("Observations: ")
		println("Primary Signal: ")
		for _, observation := range thing.observationsByDatastreams["primary_signal"] {
			println(observation.phenomenonTime, " ", observation.result)
		}
		println("Cycle Second: ")
		for _, observation := range thing.observationsByDatastreams["cycle_second"] {
			println(observation.phenomenonTime, " ", observation.result)
		}

		t.Errorf("Expected %d reconstructed cycles, got %d", 3, len(cycles))
	}

	// Check start and end times of cycles
	for i := 0; i < len(cycles); i++ {
		if i+1 < len(cycles) {
			if cycles[i].start != cycleSecondObservations[i].phenomenonTime {
				t.Errorf("Expected %d as start of cycle number %d, got %d", cycleSecondObservations[i].phenomenonTime, i, cycles[i].start)
			}
		}

		if i-1 >= 0 {
			if cycles[i-1].end != cycleSecondObservations[i].phenomenonTime {
				t.Errorf("Expected %d as end of cycle number %d, got %d", cycleSecondObservations[i].phenomenonTime, i-1, cycles[i].end)
			}
		}
	}

	// First cycle:
	cycle := cycles[0]
	for i := 0; i < len(cycle.results); i++ {
		var resultIndex int8
		if i < 10 {
			resultIndex = 0
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		} else if i < 20 {
			resultIndex = 1
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		} else if i < 35 {
			resultIndex = 2
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		}
	}

	// Second cycle:
	cycle = cycles[1]
	for i := 0; i < len(cycle.results); i++ {
		var resultIndex int8
		if i < 5 {
			resultIndex = 2
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		} else if i < 12 {
			resultIndex = 3
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		} else if i < 20 {
			resultIndex = 4
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		} else if i < 25 {
			resultIndex = 5
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		} else if i < 30 {
			resultIndex = 6
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}

		}
	}

	// Third cycle:
	cycle = cycles[2]
	for i := 0; i < len(cycle.results); i++ {
		var resultIndex int8
		if i < 10 {
			resultIndex = 7
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		} else if i < 20 {
			resultIndex = 8
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		} else if i < 30 {
			resultIndex = 9
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		}
	}

	thing = NewThing(name, validation, retrieveAllCycleCleanupStats)

	cycles, skippedCycles, primarySignalMissing, cycleSecondMissing = thing.reconstructCycles()

	if skippedCycles > 0 {
		t.Errorf("Expected %d skipped cycles, got %d", 0, skippedCycles)
	}

	if !primarySignalMissing {
		t.Errorf("Expected %t for primarySignalMissing, got %t", true, primarySignalMissing)
	}

	if !cycleSecondMissing {
		t.Errorf("Expected %t for cycleSecondMissing, got %t", true, cycleSecondMissing)
	}

	if len(cycles) != 0 {
		t.Errorf("Expected %d reconstructed cycles, got %d", 0, len(cycles))
	}

	thing = NewThing(name, validation, retrieveAllCycleCleanupStats)

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 2},
		{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 3},
		{int32(time.Date(2023, 10, 20, 0, 0, 20, 0, location).Unix()), 4},
		{int32(time.Date(2023, 10, 20, 0, 0, 35, 0, location).Unix()), 5},
		{int32(time.Date(2023, 10, 20, 0, 0, 42, 0, location).Unix()), 6},
		{int32(time.Date(2023, 10, 20, 0, 0, 50, 0, location).Unix()), 1},
		{int32(time.Date(2023, 10, 20, 0, 0, 55, 0, location).Unix()), 3},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 4},
		{int32(time.Date(2023, 10, 20, 0, 1, 10, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 20, 0, location).Unix()), 1},
	}

	for i := 0; i < len(primarySignalObservations); i++ {
		thing.AddObservation("primary_signal", primarySignalObservations[i].phenomenonTime, primarySignalObservations[i].result)
	}

	cycles, skippedCycles, primarySignalMissing, cycleSecondMissing = thing.reconstructCycles()

	if skippedCycles > 0 {
		t.Errorf("Expected %d skipped cycles, got %d", 0, skippedCycles)
	}

	if primarySignalMissing {
		t.Errorf("Expected %t for primarySignalMissing, got %t", false, primarySignalMissing)
	}

	if !cycleSecondMissing {
		t.Errorf("Expected %t for cycleSecondMissing, got %t", true, cycleSecondMissing)
	}

	if len(cycles) != 0 {
		t.Errorf("Expected %d reconstructed cycles, got %d", 0, len(cycles))
	}

	thing = NewThing(name, validation, retrieveAllCycleCleanupStats)

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 21, 0, 0, 0, 0, location).Unix()), 2},
		{int32(time.Date(2023, 10, 21, 0, 0, 10, 0, location).Unix()), 3},
		{int32(time.Date(2023, 10, 21, 0, 0, 20, 0, location).Unix()), 4},
		{int32(time.Date(2023, 10, 21, 0, 0, 35, 0, location).Unix()), 5},
		{int32(time.Date(2023, 10, 21, 0, 0, 42, 0, location).Unix()), 6},
		{int32(time.Date(2023, 10, 21, 0, 0, 50, 0, location).Unix()), 1},
		{int32(time.Date(2023, 10, 21, 0, 0, 55, 0, location).Unix()), 3},
		{int32(time.Date(2023, 10, 21, 0, 1, 0, 0, location).Unix()), 4},
		{int32(time.Date(2023, 10, 21, 0, 1, 10, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 21, 0, 1, 20, 0, location).Unix()), 1},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 30, 0, location).Unix()), 0},
	}

	for i := 0; i < len(primarySignalObservations); i++ {
		thing.AddObservation("primary_signal", primarySignalObservations[i].phenomenonTime, primarySignalObservations[i].result)
	}

	for i := 0; i < len(cycleSecondObservations); i++ {
		thing.AddObservation("cycle_second", cycleSecondObservations[i].phenomenonTime, cycleSecondObservations[i].result)
	}

	cycles, skippedCycles, primarySignalMissing, cycleSecondMissing = thing.reconstructCycles()

	if skippedCycles != 3 {
		t.Errorf("Expected %d skipped cycles, got %d", 3, skippedCycles)
	}

	if primarySignalMissing {
		t.Errorf("Expected %t for primarySignalMissing, got %t", false, primarySignalMissing)
	}

	if cycleSecondMissing {
		t.Errorf("Expected %t for cycleSecondMissing, got %t", false, cycleSecondMissing)
	}

	if len(cycles) != 0 {
		t.Errorf("Expected %d reconstructed cycles, got %d", 0, len(cycles))
	}

	thing = NewThing(name, validation, retrieveAllCycleCleanupStats)

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 4},
		{int32(time.Date(2023, 10, 20, 0, 1, 10, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 20, 0, location).Unix()), 1},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 30, 0, location).Unix()), 0},
	}

	for i := 0; i < len(primarySignalObservations); i++ {
		thing.AddObservation("primary_signal", primarySignalObservations[i].phenomenonTime, primarySignalObservations[i].result)
	}

	for i := 0; i < len(cycleSecondObservations); i++ {
		thing.AddObservation("cycle_second", cycleSecondObservations[i].phenomenonTime, cycleSecondObservations[i].result)
	}

	cycles, skippedCycles, primarySignalMissing, cycleSecondMissing = thing.reconstructCycles()

	if skippedCycles != 2 {
		t.Errorf("Expected %d skipped cycles, got %d", 2, skippedCycles)
	}

	if primarySignalMissing {
		t.Errorf("Expected %t for primarySignalMissing, got %t", false, primarySignalMissing)
	}

	if cycleSecondMissing {
		t.Errorf("Expected %t for cycleSecondMissing, got %t", false, cycleSecondMissing)
	}

	if len(cycles) != 1 {
		t.Errorf("Expected %d reconstructed cycles, got %d", 1, len(cycles))
	}

	// Check start and end times of cycles
	cycleStart := cycles[0].start
	cycleEnd := cycles[0].end

	if cycleStart != cycleSecondObservations[2].phenomenonTime {
		t.Errorf("Expected %d as start of cycle, got %d", cycleSecondObservations[2].phenomenonTime, cycleStart)
	}
	if cycleEnd != cycleSecondObservations[3].phenomenonTime {
		t.Errorf("Expected %d as end of cycle, got %d", cycleSecondObservations[3].phenomenonTime, cycleEnd)
	}

	cycle = cycles[0]
	for i := 0; i < len(cycle.results); i++ {
		var resultIndex int8
		if i < 10 {
			resultIndex = 0
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		} else if i < 20 {
			resultIndex = 1
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		} else if i < 30 {
			resultIndex = 2
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		}
	}

	thing = NewThing(name, validation, retrieveAllCycleCleanupStats)

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 2},
		{int32(time.Date(2023, 10, 20, 0, 1, 20, 0, location).Unix()), 1},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 30, 0, location).Unix()), 0},
	}

	for i := 0; i < len(primarySignalObservations); i++ {
		thing.AddObservation("primary_signal", primarySignalObservations[i].phenomenonTime, primarySignalObservations[i].result)
	}

	for i := 0; i < len(cycleSecondObservations); i++ {
		thing.AddObservation("cycle_second", cycleSecondObservations[i].phenomenonTime, cycleSecondObservations[i].result)
	}

	cycles, skippedCycles, primarySignalMissing, cycleSecondMissing = thing.reconstructCycles()

	if skippedCycles != 0 {
		t.Errorf("Expected %d skipped cycles, got %d", 0, skippedCycles)
	}

	if primarySignalMissing {
		t.Errorf("Expected %t for primarySignalMissing, got %t", false, primarySignalMissing)
	}

	if cycleSecondMissing {
		t.Errorf("Expected %t for cycleSecondMissing, got %t", false, cycleSecondMissing)
	}

	if len(cycles) != 2 {
		t.Errorf("Expected %d reconstructed cycles, got %d", 2, len(cycles))
	}

	// Check start and end times of cycles
	for i := 0; i < len(cycles); i++ {
		if i+1 < len(cycles) {
			if cycles[i].start != cycleSecondObservations[i].phenomenonTime {
				t.Errorf("Expected %d as start of cycle number %d, got %d", cycleSecondObservations[i].phenomenonTime, i, cycles[i].start)
			}
		}

		if i-1 >= 0 {
			if cycles[i-1].end != cycleSecondObservations[i].phenomenonTime {
				t.Errorf("Expected %d as end of cycle number %d, got %d", cycleSecondObservations[i].phenomenonTime, i-1, cycles[i].end)
			}
		}
	}

	// First cycle:
	cycle = cycles[0]
	for i := 0; i < len(cycle.results); i++ {
		var resultIndex int8
		if i < 30 {
			resultIndex = 0
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		}
	}

	// Second cycle:
	cycle = cycles[1]
	for i := 0; i < len(cycle.results); i++ {
		var resultIndex int8
		if i < 20 {
			resultIndex = 0
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		} else if i < 30 {
			resultIndex = 1
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		}
	}

	thing = NewThing(name, validation, retrieveAllCycleCleanupStats)

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 2},
		{int32(time.Date(2023, 10, 20, 0, 1, 40, 0, location).Unix()), 1},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 30, 0, location).Unix()), 0},
	}

	for i := 0; i < len(primarySignalObservations); i++ {
		thing.AddObservation("primary_signal", primarySignalObservations[i].phenomenonTime, primarySignalObservations[i].result)
	}

	for i := 0; i < len(cycleSecondObservations); i++ {
		thing.AddObservation("cycle_second", cycleSecondObservations[i].phenomenonTime, cycleSecondObservations[i].result)
	}

	cycles, skippedCycles, primarySignalMissing, cycleSecondMissing = thing.reconstructCycles()

	if skippedCycles != 0 {
		t.Errorf("Expected %d skipped cycles, got %d", 0, skippedCycles)
	}

	if primarySignalMissing {
		t.Errorf("Expected %t for primarySignalMissing, got %t", false, primarySignalMissing)
	}

	if cycleSecondMissing {
		t.Errorf("Expected %t for cycleSecondMissing, got %t", false, cycleSecondMissing)
	}

	if len(cycles) != 2 {
		t.Errorf("Expected %d reconstructed cycles, got %d", 2, len(cycles))
	}

	// Check start and end times of cycles
	for i := 0; i < len(cycles); i++ {
		if i+1 < len(cycles) {
			if cycles[i].start != cycleSecondObservations[i].phenomenonTime {
				t.Errorf("Expected %d as start of cycle number %d, got %d", cycleSecondObservations[i].phenomenonTime, i, cycles[i].start)
			}
		}

		if i-1 >= 0 {
			if cycles[i-1].end != cycleSecondObservations[i].phenomenonTime {
				t.Errorf("Expected %d as end of cycle number %d, got %d", cycleSecondObservations[i].phenomenonTime, i-1, cycles[i].end)
			}
		}
	}

	// First cycle:
	cycle = cycles[0]
	for i := 0; i < len(cycle.results); i++ {
		var resultIndex int8
		if i < 30 {
			resultIndex = 0
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		}
	}

	// Second cycle:
	cycle = cycles[1]
	for i := 0; i < len(cycle.results); i++ {
		var resultIndex int8
		if i < 30 {
			resultIndex = 0
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		}
	}
}
