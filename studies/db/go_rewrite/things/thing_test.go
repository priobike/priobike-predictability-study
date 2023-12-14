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

func TestReconstructCyclesMissing(t *testing.T) {
	name := "test_name"
	validation := false
	retrieveAllCycleCleanupStats := false
	thing := NewThing(name, validation, retrieveAllCycleCleanupStats)

	cycles, skippedCycles, primarySignalMissing, cycleSecondMissing := thing.reconstructCycles()

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
}

func TestReconstructCyclesMissingCycle(t *testing.T) {
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

	for i := 0; i < len(primarySignalObservations); i++ {
		thing.AddObservation("primary_signal", primarySignalObservations[i].phenomenonTime, primarySignalObservations[i].result)
	}

	cycles, skippedCycles, primarySignalMissing, cycleSecondMissing := thing.reconstructCycles()

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
}

func TestReconstructCyclesSkipped(t *testing.T) {
	name := "test_name"
	validation := false
	retrieveAllCycleCleanupStats := false
	thing := NewThing(name, validation, retrieveAllCycleCleanupStats)

	location, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		panic(err)
	}

	primarySignalObservations := []Observation{
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
		} else if i < 30 {
			resultIndex = 2
			if int16(cycle.results[i]) != primarySignalObservations[resultIndex].result {
				t.Errorf("Expected result %d at cycle index %d, got %d", primarySignalObservations[resultIndex].result, i, cycle.results[i])
			}
		}
	}
}

func TestReconstructCyclesPartly(t *testing.T) {
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
		{int32(time.Date(2023, 10, 20, 0, 1, 20, 0, location).Unix()), 1},
	}

	cycleSecondObservations := []Observation{
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
	cycle := cycles[0]
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

func TestReconstructCyclesComplete(t *testing.T) {
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
}

func checkInvalidCycle(
	t *testing.T,
	primarySignalObservations []Observation,
	cycleSecondObservations []Observation,
	expectedTotalCycles int,
	expectedCleanedUpCycles int,
	expectedInvalidTransitionCount int32,
	expectedMissingObservationCount int32,
	expectedInvalidLengthCount int32,
) {
	name := "test_name"
	validation := false
	retrieveAllCycleCleanupStats := true
	thing := NewThing(name, validation, retrieveAllCycleCleanupStats)

	for i := 0; i < len(primarySignalObservations); i++ {
		thing.AddObservation("primary_signal", primarySignalObservations[i].phenomenonTime, primarySignalObservations[i].result)
	}

	for i := 0; i < len(cycleSecondObservations); i++ {
		thing.AddObservation("cycle_second", cycleSecondObservations[i].phenomenonTime, cycleSecondObservations[i].result)
	}

	cycles, skippedCycles, primarySignalMissing, cycleSecondMissing := thing.reconstructCycles()

	if skippedCycles != 0 {
		t.Errorf("Expected %d skipped cycles, got %d", 0, skippedCycles)
	}

	if primarySignalMissing {
		t.Errorf("Expected %t for primarySignalMissing, got %t", false, primarySignalMissing)
	}

	if cycleSecondMissing {
		t.Errorf("Expected %t for cycleSecondMissing, got %t", false, cycleSecondMissing)
	}

	if len(cycles) != expectedTotalCycles {
		t.Errorf("Expected %d reconstructed cycles, got %d", expectedTotalCycles, len(cycles))
	}

	cycles = thing.cleanUpCycles(cycles)

	if len(cycles) != expectedCleanedUpCycles {
		t.Errorf("Expected %d cleaned up cycles, got %d", expectedCleanedUpCycles, len(cycles))
	}

	if thing.TotalInvalidCycleMissingCount != expectedMissingObservationCount {
		cycle := cycles[0]
		println()
		for _, result := range cycle.results {
			print(result)
		}
		println()
		t.Errorf("Expected %d cycle(s) with missing observations, got %d", expectedMissingObservationCount, thing.TotalInvalidCycleMissingCount)
	}

	if thing.TotalInvalidCycleTransitionCount != expectedInvalidTransitionCount {
		t.Errorf("Expected %d cycle(s) with invalid transitions, got %d", expectedInvalidTransitionCount, thing.TotalInvalidCycleTransitionCount)
	}

	if thing.TotalInvalidCycleLengthCount != expectedInvalidLengthCount {
		t.Errorf("Expected %d cycle(s) with invalid length, got %d", expectedInvalidLengthCount, thing.TotalInvalidCycleLengthCount)
	}

}

func TestCleanUpCycles(t *testing.T) {

	location, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		panic(err)
	}

	// Red -> Amber is disallowed.
	// Amber too long.

	primarySignalObservations := []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
		{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 2},
	}

	cycleSecondObservations := []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
	}

	checkInvalidCycle(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		1,
		0,
		1,
		1,
		0,
	)

	// Red -> Amber is disallowed.

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
		{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 2},
		{int32(time.Date(2023, 10, 20, 0, 0, 34, 0, location).Unix()), 1},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
	}

	checkInvalidCycle(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		1,
		0,
		1,
		0,
		0,
	)

	// Amber -> Green is disallowed.

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 2},
		{int32(time.Date(2023, 10, 20, 0, 0, 2, 0, location).Unix()), 3},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
	}

	checkInvalidCycle(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		1,
		0,
		1,
		0,
		0,
	)

	// Amber -> RedAmber is disallowed.

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 2},
		{int32(time.Date(2023, 10, 20, 0, 0, 2, 0, location).Unix()), 4},
		{int32(time.Date(2023, 10, 20, 0, 0, 4, 0, location).Unix()), 1},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
	}

	checkInvalidCycle(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		1,
		0,
		1,
		0,
		0,
	)

	// Green -> RedAmber is disallowed.

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 3},
		{int32(time.Date(2023, 10, 20, 0, 0, 2, 0, location).Unix()), 4},
		{int32(time.Date(2023, 10, 20, 0, 0, 4, 0, location).Unix()), 1},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
	}

	checkInvalidCycle(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		1,
		0,
		1,
		0,
		0,
	)

	// RedAmber -> Red is disallowed.

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 4},
		{int32(time.Date(2023, 10, 20, 0, 0, 2, 0, location).Unix()), 1},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
	}

	checkInvalidCycle(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		1,
		0,
		1,
		0,
		0,
	)

	// RedAmber -> Red is disallowed.

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 4},
		{int32(time.Date(2023, 10, 20, 0, 0, 2, 0, location).Unix()), 2},
		{int32(time.Date(2023, 10, 20, 0, 0, 4, 0, location).Unix()), 1},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
	}

	checkInvalidCycle(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		1,
		0,
		1,
		0,
		0,
	)

	// Amber too long (more than 6 seconds).

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 3},
		{int32(time.Date(2023, 10, 20, 0, 0, 2, 0, location).Unix()), 2},
		{int32(time.Date(2023, 10, 20, 0, 0, 9, 0, location).Unix()), 1},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
	}

	checkInvalidCycle(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		1,
		0,
		0,
		1,
		0,
	)

	// Amber not too long (less (or equal) than 6 seconds).

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 3},
		{int32(time.Date(2023, 10, 20, 0, 0, 2, 0, location).Unix()), 2},
		{int32(time.Date(2023, 10, 20, 0, 0, 8, 0, location).Unix()), 1},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
	}

	checkInvalidCycle(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		1,
		1,
		0,
		0,
		0,
	)

	// Red Amber too long (more than 2 seconds).

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
		{int32(time.Date(2023, 10, 20, 0, 0, 2, 0, location).Unix()), 4},
		{int32(time.Date(2023, 10, 20, 0, 0, 5, 0, location).Unix()), 3},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
	}

	checkInvalidCycle(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		1,
		0,
		0,
		1,
		0,
	)

	// Red Amber not too long (less (or equal) than 2 seconds).

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
		{int32(time.Date(2023, 10, 20, 0, 0, 2, 0, location).Unix()), 4},
		{int32(time.Date(2023, 10, 20, 0, 0, 4, 0, location).Unix()), 3},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
	}

	checkInvalidCycle(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		1,
		1,
		0,
		0,
		0,
	)

	// One cycle too short.

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 2, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 2, 10, 0, location).Unix()), 0},
	}

	checkInvalidCycle(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		3,
		2,
		0,
		0,
		1,
	)

	// One cycle too long.

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 2, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 5, 0, 0, location).Unix()), 0},
	}

	checkInvalidCycle(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		3,
		2,
		0,
		0,
		1,
	)

	// Valid cycles.

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
		{int32(time.Date(2023, 10, 20, 0, 0, 20, 0, location).Unix()), 3},
		{int32(time.Date(2023, 10, 20, 0, 0, 40, 0, location).Unix()), 1},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 1},
		{int32(time.Date(2023, 10, 20, 0, 1, 20, 0, location).Unix()), 3},
		{int32(time.Date(2023, 10, 20, 0, 1, 40, 0, location).Unix()), 1},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 2, 0, 0, location).Unix()), 0},
	}

	checkInvalidCycle(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		2,
		2,
		0,
		0,
		0,
	)

	// Valid cycles.

	primarySignalObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
		{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 4},
		{int32(time.Date(2023, 10, 20, 0, 0, 12, 0, location).Unix()), 3},
		{int32(time.Date(2023, 10, 20, 0, 0, 40, 0, location).Unix()), 2},
		{int32(time.Date(2023, 10, 20, 0, 0, 46, 0, location).Unix()), 1},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 1},
		{int32(time.Date(2023, 10, 20, 0, 1, 10, 0, location).Unix()), 4},
		{int32(time.Date(2023, 10, 20, 0, 1, 12, 0, location).Unix()), 3},
		{int32(time.Date(2023, 10, 20, 0, 1, 40, 0, location).Unix()), 2},
		{int32(time.Date(2023, 10, 20, 0, 1, 46, 0, location).Unix()), 1},
	}

	cycleSecondObservations = []Observation{
		{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
		{int32(time.Date(2023, 10, 20, 0, 2, 0, 0, location).Unix()), 0},
	}

	checkInvalidCycle(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		2,
		2,
		0,
		0,
		0,
	)
}

func TestPhaseWiseDistance(t *testing.T) {
	name := "test_name"
	validation := false
	retrieveAllCycleCleanupStats := true
	thing := NewThing(name, validation, retrieveAllCycleCleanupStats)

	cycle1 := cycle{
		start:   0,
		end:     10,
		results: []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}

	cycle2 := cycle{
		start:   0,
		end:     10,
		results: []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}

	distance := thing.phaseWiseDistance(cycle1, cycle2)

	if distance != 0 {
		t.Errorf("Expected distance %d, got %v", 0, distance)
	}

	cycle1 = cycle{
		start:   0,
		end:     10,
		results: []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}

	cycle2 = cycle{
		start:   0,
		end:     10,
		results: []int8{1, 2, 3, 4, 5, 6, 7, 8},
	}

	distance = thing.phaseWiseDistance(cycle1, cycle2)

	if distance != 2 {
		t.Errorf("Expected distance %d, got %v", 2, distance)
	}

	cycle1 = cycle{
		start:   0,
		end:     10,
		results: []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}

	cycle2 = cycle{
		start:   0,
		end:     10,
		results: []int8{1, 2, 3, 4, 5, 6, 7, 8, 10, 11},
	}

	distance = thing.phaseWiseDistance(cycle1, cycle2)

	if distance != 2 {
		t.Errorf("Expected distance %d, got %v", 2, distance)
	}

	cycle1 = cycle{
		start:   0,
		end:     10,
		results: []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}

	cycle2 = cycle{
		start:   0,
		end:     10,
		results: []int8{1, 1, 1, 1, 1, 1, 1, 1, 11, 10, 11},
	}

	distance = thing.phaseWiseDistance(cycle1, cycle2)

	if distance != 9 {
		t.Errorf("Expected distance %d, got %v", 9, distance)
	}
}

func checkMetric(
	t *testing.T,
	primarySignalObservations [4][]Observation,
	cycleSecondObservations [4][]Observation,
	expectedMetric float64,
	dayIdx int,
	hourIdx int,
) {
	name := "test_name"
	validation := false
	retrieveAllCycleCleanupStats := true
	thing := NewThing(name, validation, retrieveAllCycleCleanupStats)

	for cellIdx := 0; cellIdx < 4; cellIdx++ {
		for i := 0; i < len(primarySignalObservations[cellIdx]); i++ {
			thing.AddObservation("primary_signal", primarySignalObservations[cellIdx][i].phenomenonTime, primarySignalObservations[cellIdx][i].result)
		}

		for i := 0; i < len(cycleSecondObservations[cellIdx]); i++ {
			thing.AddObservation("cycle_second", cycleSecondObservations[cellIdx][i].phenomenonTime, cycleSecondObservations[cellIdx][i].result)
		}

		thing.CalcCycles(cellIdx)
		if len(thing.observationsByDatastreams["primary_signal"]) != 0 {
			t.Errorf("Expected %d observations for primary_signal, got %d", 0, len(thing.observationsByDatastreams["primary_signal"]))
		}
		if len(thing.observationsByDatastreams["cycle_second"]) != 0 {
			t.Errorf("Expected %d observations for cycle_second, got %d", 0, len(thing.observationsByDatastreams["cycle_second"]))
		}
	}

	thing.CalculateMetrics(dayIdx, hourIdx)

	if thing.Metrics[dayIdx][hourIdx] != expectedMetric {
		t.Errorf("Expected metric %f, got %f", expectedMetric, thing.Metrics[dayIdx][hourIdx])
	}
}

func checkShiftsFuzyness(
	t *testing.T,
	primarySignalObservations [4][]Observation,
	cycleSecondObservations [4][]Observation,
	expectedMetric float64,
	dayIdx int,
	hourIdx int,
) {
	name := "test_name"
	validation := false
	retrieveAllCycleCleanupStats := true
	thing := NewThing(name, validation, retrieveAllCycleCleanupStats)

	for cellIdx := 0; cellIdx < 4; cellIdx++ {
		for i := 0; i < len(primarySignalObservations[cellIdx]); i++ {
			thing.AddObservation("primary_signal", primarySignalObservations[cellIdx][i].phenomenonTime, primarySignalObservations[cellIdx][i].result)
		}

		for i := 0; i < len(cycleSecondObservations[cellIdx]); i++ {
			thing.AddObservation("cycle_second", cycleSecondObservations[cellIdx][i].phenomenonTime, cycleSecondObservations[cellIdx][i].result)
		}

		thing.CalcCycles(cellIdx)
		if len(thing.observationsByDatastreams["primary_signal"]) != 0 {
			t.Errorf("Expected %d observations for primary_signal, got %d", 0, len(thing.observationsByDatastreams["primary_signal"]))
		}
		if len(thing.observationsByDatastreams["cycle_second"]) != 0 {
			t.Errorf("Expected %d observations for cycle_second, got %d", 0, len(thing.observationsByDatastreams["cycle_second"]))
		}
	}

	thing.CalculateMetrics(dayIdx, hourIdx)

	if thing.ShiftsFuzzyness[dayIdx][hourIdx] != expectedMetric {
		t.Errorf("Expected metric %f, got %f", expectedMetric, thing.ShiftsFuzzyness[dayIdx][hourIdx])
	}
}

func checkRelativeGreenDistancesMetric(
	t *testing.T,
	primarySignalObservations [4][]Observation,
	cycleSecondObservations [4][]Observation,
	expectedMetric float64,
	dayIdx int,
	hourIdx int,
) {
	name := "test_name"
	validation := false
	retrieveAllCycleCleanupStats := true
	thing := NewThing(name, validation, retrieveAllCycleCleanupStats)

	for cellIdx := 0; cellIdx < 4; cellIdx++ {
		for i := 0; i < len(primarySignalObservations[cellIdx]); i++ {
			thing.AddObservation("primary_signal", primarySignalObservations[cellIdx][i].phenomenonTime, primarySignalObservations[cellIdx][i].result)
		}

		for i := 0; i < len(cycleSecondObservations[cellIdx]); i++ {
			thing.AddObservation("cycle_second", cycleSecondObservations[cellIdx][i].phenomenonTime, cycleSecondObservations[cellIdx][i].result)
		}

		thing.CalcCycles(cellIdx)
		if len(thing.observationsByDatastreams["primary_signal"]) != 0 {
			t.Errorf("Expected %d observations for primary_signal, got %d", 0, len(thing.observationsByDatastreams["primary_signal"]))
		}
		if len(thing.observationsByDatastreams["cycle_second"]) != 0 {
			t.Errorf("Expected %d observations for cycle_second, got %d", 0, len(thing.observationsByDatastreams["cycle_second"]))
		}
	}

	thing.CalculateMetrics(dayIdx, hourIdx)

	/* if thing.MetricsRelativeGreenDistance[dayIdx][hourIdx] != expectedMetric {
		t.Errorf("Expected relative green distance metric %f, got %f", expectedMetric, thing.MetricsRelativeGreenDistance[dayIdx][hourIdx])
	} */
}

func TestCalcMetric(t *testing.T) {
	location, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		panic(err)
	}

	primarySignalObservations := [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 9, 29, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 9, 29, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 9, 29, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 9, 29, 0, 1, 46, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 6, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 6, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 6, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 6, 0, 1, 46, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 13, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 13, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 13, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 13, 0, 1, 46, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 20, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 20, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 20, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 20, 0, 1, 46, 0, location).Unix()), 1},
		},
	}

	cycleSecondObservations := [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 2, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 2, 0, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 2, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 2, 0, 0, location).Unix()), 0},
		},
	}

	checkMetric(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		0,
		1,
		1,
	)

	checkShiftsFuzyness(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		0.25,
		1,
		1,
	)

	checkRelativeGreenDistancesMetric(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		0,
		1,
		1,
	)

	primarySignalObservations = [4][]Observation{
		{ // Distance: 1
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 9, 29, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 9, 29, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 9, 29, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 9, 29, 0, 1, 46, 0, location).Unix()), 1},
		},
		{ // Distance: 1
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 6, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 6, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 6, 0, 1, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 6, 0, 1, 46, 0, location).Unix()), 1},
		},
		{ // Distance: 1
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 13, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 13, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 13, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 13, 0, 1, 45, 0, location).Unix()), 1},
		},
		{ // Distance: 0
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 20, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 20, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 20, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 20, 0, 1, 46, 0, location).Unix()), 1},
		},
	}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 2, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 2, 0, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 2, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 2, 0, 0, location).Unix()), 0},
		},
	}

	checkMetric(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		1,
		1,
		1,
	)

	checkShiftsFuzyness(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		0.5,
		1,
		1,
	)

	primarySignalObservations = [4][]Observation{
		{ // Distance: 2
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 9, 29, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 9, 29, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 9, 29, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 9, 29, 0, 1, 45, 0, location).Unix()), 1},
		},
		{ // Distance: 2
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 6, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 6, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 6, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 6, 0, 1, 45, 0, location).Unix()), 1},
		},
		{ // Distance: 1
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 13, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 13, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 13, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 13, 0, 1, 45, 0, location).Unix()), 1},
		},
		{ // Distance: 0
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 20, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 20, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 20, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 20, 0, 1, 46, 0, location).Unix()), 1},
		},
	}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 2, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 2, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 2, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 2, 0, 0, location).Unix()), 0},
		},
	}

	checkMetric(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		1.5,
		1,
		1,
	)

	checkShiftsFuzyness(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		0.25,
		1,
		1,
	)

	primarySignalObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 9, 29, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 9, 29, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 5, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 5, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 9, 29, 0, 5, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 5, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 9, 29, 0, 5, 45, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 6, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 6, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 6, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 6, 0, 1, 45, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 13, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 13, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 13, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 13, 0, 1, 45, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 20, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 20, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 5, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 5, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 20, 0, 5, 15, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 5, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 20, 0, 5, 46, 0, location).Unix()), 1},
		},
	}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 5, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 6, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 2, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 2, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 5, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 6, 0, 0, location).Unix()), 0},
		},
	}

	// Test that two cycles (second of cell 1 and 4) get removed and thus dont count into the shifts fuzzyness metric
	checkShiftsFuzyness(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		0.5,
		1,
		1,
	)
}

func checkMetricSP(
	t *testing.T,
	primarySignalObservations [4][]Observation,
	cycleSecondObservations [4][]Observation,
	expectedMetric float64,
	dayIdx int,
	hourIdx int,
) {
	name := "test_name"
	validation := false
	retrieveAllCycleCleanupStats := true
	thing := NewThing(name, validation, retrieveAllCycleCleanupStats)

	for cellIdx := 0; cellIdx < 4; cellIdx++ {
		for i := 0; i < len(primarySignalObservations[cellIdx]); i++ {
			thing.AddObservation("primary_signal", primarySignalObservations[cellIdx][i].phenomenonTime, primarySignalObservations[cellIdx][i].result)
		}

		for i := 0; i < len(cycleSecondObservations[cellIdx]); i++ {
			thing.AddObservation("cycle_second", cycleSecondObservations[cellIdx][i].phenomenonTime, cycleSecondObservations[cellIdx][i].result)
		}

		thing.CalcCycles(cellIdx)
		if len(thing.observationsByDatastreams["primary_signal"]) != 0 {
			t.Errorf("Expected %d observations for primary_signal, got %d", 0, len(thing.observationsByDatastreams["primary_signal"]))
		}
		if len(thing.observationsByDatastreams["cycle_second"]) != 0 {
			t.Errorf("Expected %d observations for cycle_second, got %d", 0, len(thing.observationsByDatastreams["cycle_second"]))
		}
	}

	thing.CalculateMetrics(dayIdx, hourIdx)

	/* if thing.MetricsSP[dayIdx][hourIdx] != expectedMetric {
		t.Errorf("Expected SP metric %f, got %f", expectedMetric, thing.Metrics[dayIdx][hourIdx])
	} */
}

func checkGreenProbabilityAndReliability(
	t *testing.T,
	primarySignalObservations [4][]Observation,
	cycleSecondObservations [4][]Observation,
	expectedProbabilites []float64,
	expectedReliabilites []float64,
) {
	name := "test_name"
	validation := false
	retrieveAllCycleCleanupStats := true
	thing := NewThing(name, validation, retrieveAllCycleCleanupStats)

	for cellIdx := 0; cellIdx < 4; cellIdx++ {
		for i := 0; i < len(primarySignalObservations[cellIdx]); i++ {
			thing.AddObservation("primary_signal", primarySignalObservations[cellIdx][i].phenomenonTime, primarySignalObservations[cellIdx][i].result)
		}

		for i := 0; i < len(cycleSecondObservations[cellIdx]); i++ {
			thing.AddObservation("cycle_second", cycleSecondObservations[cellIdx][i].phenomenonTime, cycleSecondObservations[cellIdx][i].result)
		}

		thing.CalcCycles(cellIdx)
		if len(thing.observationsByDatastreams["primary_signal"]) != 0 {
			t.Errorf("Expected %d observations for primary_signal, got %d", 0, len(thing.observationsByDatastreams["primary_signal"]))
		}
		if len(thing.observationsByDatastreams["cycle_second"]) != 0 {
			t.Errorf("Expected %d observations for cycle_second, got %d", 0, len(thing.observationsByDatastreams["cycle_second"]))
		}
	}
	cycles := []cycle{}
	for _, cell := range thing.cycles {
		cycles = append(cycles, cell...)
	}
	/* greenProbabilites := thing.getGreenProbabilities(cycles)

	if len(greenProbabilites) != len(expectedProbabilites) {
		t.Errorf("Expected %d green probabilities, got %d", len(expectedProbabilites), len(greenProbabilites))
	}

	for i := 0; i < len(greenProbabilites); i++ {
		if greenProbabilites[i] != expectedProbabilites[i] {
			t.Errorf("Expected green probability %f at index %d, got %f", expectedProbabilites[i], i, greenProbabilites[i])
		}
	}

	greenReliabilites := thing.getGreenReliabilities(greenProbabilites)

	if len(greenReliabilites) != len(expectedReliabilites) {
		t.Errorf("Expected %d green reliabilities, got %d", len(expectedReliabilites), len(greenReliabilites))
	}

	for i := 0; i < len(greenReliabilites); i++ {
		if greenReliabilites[i] != expectedReliabilites[i] {
			t.Errorf("Expected green reliability %f at index %d, got %f", expectedReliabilites[i], i, greenReliabilites[i])
		}
	} */
}

func TestCalcMetricSP(t *testing.T) {
	location, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		panic(err)
	}

	primarySignalObservations := [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 9, 29, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 9, 29, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 9, 29, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 9, 29, 0, 1, 46, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 6, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 6, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 6, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 6, 0, 1, 46, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 13, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 13, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 13, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 13, 0, 1, 46, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 20, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 20, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 20, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 20, 0, 1, 46, 0, location).Unix()), 1},
		},
	}

	cycleSecondObservations := [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 2, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 2, 0, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 2, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 2, 0, 0, location).Unix()), 0},
		},
	}

	checkMetricSP(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		0,
		1,
		1,
	)

	primarySignalObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 10, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 10, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 10, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 9, 0, location).Unix()), 3},
		},
	}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 0},
		},
	}

	greenProbabilities := [15]float64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0.25, 1, 1, 1, 1, 1}
	greenReliabilities := [15]float64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0.25, 0, 0, 0, 0, 0}

	checkGreenProbabilityAndReliability(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		greenProbabilities[:],
		greenReliabilities[:],
	)

	checkMetricSP(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		0,
		1,
		1,
	)

	primarySignalObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 13, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 13, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 13, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 1, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 14, 0, location).Unix()), 1},
		},
	}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 0},
		},
	}

	greenProbabilities = [15]float64{0, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 1, 0.75}
	greenReliabilities = [15]float64{0, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0, 0.25}

	checkGreenProbabilityAndReliability(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		greenProbabilities[:],
		greenReliabilities[:],
	)

	checkMetricSP(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		0.25,
		1,
		1,
	)

	primarySignalObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 13, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 13, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 13, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 14, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 1, 0, location).Unix()), 3},
		},
	}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 14, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 14, 0, location).Unix()), 0},
		},
	}

	greenProbabilities = [15]float64{0, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 1, 0.5}
	greenReliabilities = [15]float64{0, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0.25, 0, 0.5}

	checkGreenProbabilityAndReliability(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		greenProbabilities[:],
		greenReliabilities[:],
	)

	checkMetricSP(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		0.25,
		1,
		1,
	)
}

func checkGreenIndices(
	t *testing.T,
	primarySignalObservations [4][]Observation,
	cycleSecondObservations [4][]Observation,
	expectedIndicesPerCycle [][][]int,
	expectedMetric float64,
) {
	name := "test_name"
	validation := false
	retrieveAllCycleCleanupStats := true
	thing := NewThing(name, validation, retrieveAllCycleCleanupStats)

	for cellIdx := 0; cellIdx < 4; cellIdx++ {
		for i := 0; i < len(primarySignalObservations[cellIdx]); i++ {
			thing.AddObservation("primary_signal", primarySignalObservations[cellIdx][i].phenomenonTime, primarySignalObservations[cellIdx][i].result)
		}

		for i := 0; i < len(cycleSecondObservations[cellIdx]); i++ {
			thing.AddObservation("cycle_second", cycleSecondObservations[cellIdx][i].phenomenonTime, cycleSecondObservations[cellIdx][i].result)
		}

		thing.CalcCycles(cellIdx)
		if len(thing.observationsByDatastreams["primary_signal"]) != 0 {
			t.Errorf("Expected %d observations for primary_signal, got %d", 0, len(thing.observationsByDatastreams["primary_signal"]))
		}
		if len(thing.observationsByDatastreams["cycle_second"]) != 0 {
			t.Errorf("Expected %d observations for cycle_second, got %d", 0, len(thing.observationsByDatastreams["cycle_second"]))
		}
	}

	for cellIdx, cell := range thing.cycles {
		for cycleIdx, cycle := range cell {
			greenIndices := thing.getGreenIndices(cycle)
			if len(greenIndices) != len(expectedIndicesPerCycle[cellIdx][cycleIdx]) {
				t.Errorf("Expected %d green indices at cell %d, cycle %d, got %d", len(expectedIndicesPerCycle[cellIdx][cycleIdx]), cellIdx, cycleIdx, len(greenIndices))
			}
		}
	}

	thing.CalculateMetrics(1, 1)

	/* if expectedMetric != -999999 {
		if thing.MedianShifts[1][1] == -999999 {
			t.Errorf("Expected metric %f, got -999999", expectedMetric)
		} else if thing.MedianShifts[1][1] != expectedMetric {
			t.Errorf("Expected median shifts %f, got %f", expectedMetric, thing.MedianShifts[1][1])
		}
	} else {
		if thing.MedianShifts[1][1] != -999999 {
			t.Errorf("Expected -999999 median shifts, got %f", thing.MedianShifts[1][1])
		}
	} */
}

func TestGreenShifts(t *testing.T) {
	location, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		panic(err)
	}

	primarySignalObservations := [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 9, 29, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 9, 29, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 9, 29, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 9, 29, 0, 1, 46, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 6, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 6, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 6, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 6, 0, 1, 46, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 13, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 13, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 13, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 13, 0, 1, 46, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 20, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 20, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 20, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 20, 0, 1, 46, 0, location).Unix()), 1},
		},
	}

	expectedGreenIndicesPerCycle := [][][]int{
		{
			{12, 39},
			{12, 39},
		},
		{
			{12, 39},
			{12, 39},
		},
		{
			{12, 39},
			{12, 39},
		},
		{
			{12, 39},
			{12, 39},
		},
	}

	cycleSecondObservations := [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 2, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 2, 0, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 2, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 2, 0, 0, location).Unix()), 0},
		},
	}

	expectedMetric := 0.0
	checkGreenIndices(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		expectedGreenIndicesPerCycle,
		expectedMetric,
	)

	primarySignalObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 10, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 9, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 8, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 7, 0, location).Unix()), 3},
		},
	}

	expectedGreenIndicesPerCycle = [][][]int{
		{
			{10, 14},
		},
		{
			{9, 14},
		},
		{
			{8, 14},
		},
		{
			{7, 14},
		},
	}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 0},
		},
	}

	checkGreenIndices(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		expectedGreenIndicesPerCycle,
		-999999,
	)

	primarySignalObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 13, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 22, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 23, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 24, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 25, 0, location).Unix()), 3},
		},
	}

	expectedGreenIndicesPerCycle = [][][]int{
		{
			{13, 14, 22, 29},
		},
		{
			{12, 14, 23, 29},
		},
		{
			{11, 14, 24, 29},
		},
		{
			{10, 14, 25, 29},
		},
	}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 30, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 0},
		},
	}

	checkGreenIndices(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		expectedGreenIndicesPerCycle,
		-999999,
	)

	primarySignalObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 10, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 7, 0, location).Unix()), 3},
		},
	}

	expectedGreenIndicesPerCycle = [][][]int{
		{
			{10, 14},
		},
		{
			{},
		},
		{
			{},
		},
		{
			{7, 14},
		},
	}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 0},
		},
	}

	checkGreenIndices(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		expectedGreenIndicesPerCycle,
		-999999,
	)

	primarySignalObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
		},
	}

	expectedGreenIndicesPerCycle = [][][]int{
		{
			{},
		},
		{
			{},
		},
		{
			{},
		},
		{
			{},
		},
	}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 0},
		},
	}

	checkGreenIndices(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		expectedGreenIndicesPerCycle,
		-999999,
	)

	checkRelativeGreenDistancesMetric(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		-1.0,
		1,
		1,
	)

	primarySignalObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 20, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 29, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 9, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 20, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 28, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 8, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 20, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 27, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 7, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 20, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 26, 0, location).Unix()), 3},
		},
	}

	expectedGreenIndicesPerCycle = [][][]int{
		{
			{10, 19},
			{9, 19},
		},
		{
			{9, 19},
			{8, 19},
		},
		{
			{8, 19},
			{7, 19},
		},
		{
			{7, 19},
			{6, 19},
		},
	}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 20, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 40, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 20, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 40, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 20, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 40, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 20, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 40, 0, location).Unix()), 0},
		},
	}

	expectedMetric = -0.5
	checkGreenIndices(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		expectedGreenIndicesPerCycle,
		expectedMetric,
	)

	primarySignalObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 12, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 24, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 28, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 23, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 12, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 24, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 28, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 25, 0, location).Unix()), 3},
		},
	}

	expectedGreenIndicesPerCycle = [][][]int{
		{
			{10, 11},
			{9, 12},
		},
		{
			{12, 14, 23, 29},
		},
		{
			{10, 11},
			{9, 12},
		},
		{
			{10, 14, 25, 29},
		},
	}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 30, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 0},
		},
	}

	expectedMetric = 0.0
	checkGreenIndices(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		expectedGreenIndicesPerCycle,
		expectedMetric,
	)

	primarySignalObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 9, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 12, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 13, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 23, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 12, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 9, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 13, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 25, 0, location).Unix()), 3},
		},
	}

	expectedGreenIndicesPerCycle = [][][]int{
		{
			{9, 11},
			{},
		},
		{
			{12, 14, 23, 29},
		},
		{
			{9, 11},
			{},
		},
		{
			{10, 14, 25, 29},
		},
	}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 30, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 0},
		},
	}

	checkGreenIndices(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		expectedGreenIndicesPerCycle,
		-999999,
	)

	primarySignalObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 1},
		},
	}

	expectedGreenIndicesPerCycle = [][][]int{
		{
			{10, 14},
			{},
		},
		{
			{10, 14},
			{},
		},
		{
			{10, 14},
			{},
		},
		{
			{10, 14},
			{},
		},
	}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 30, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 0},
		},
	}

	checkGreenIndices(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		expectedGreenIndicesPerCycle,
		-999999,
	)

	primarySignalObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 13, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 22, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 22, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 20, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 25, 0, location).Unix()), 3},
		},
	}

	expectedGreenIndicesPerCycle = [][][]int{
		{ // -6, 0
			{13, 14},
			{7, 14},
		},
		{ // -4, 0
			{11, 14},
			{7, 14},
		},
		{ // -6, 0
			{11, 14},
			{5, 14},
		},
		{ // 0, 0
			{10, 14},
			{10, 14},
		},
	}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 30, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 0},
		},
	}

	checkGreenIndices(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		expectedGreenIndicesPerCycle,
		0, // -6 -6 -4 0 0 0 0 0
	)
}

func TestRelativeGreenDistancesMetric(t *testing.T) {
	location, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		panic(err)
	}

	primarySignalObservations := [4][]Observation{
		{ // Max length: 8, Distance: 6 (0.75)
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 13, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 22, 0, location).Unix()), 3},
		},
		{ // Max length: 8, Distance: 4 (0.5)
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 22, 0, location).Unix()), 3},
		},
		{ // Max length: 10, Distance: 6 (0.6)
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 20, 0, location).Unix()), 3},
		},
		{ // Max length: 5, Distance: 0 (0)
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 25, 0, location).Unix()), 3},
		},
	}

	cycleSecondObservations := [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 30, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 0},
		},
	}

	checkRelativeGreenDistancesMetric(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		0.55,
		1,
		1,
	)

	primarySignalObservations = [4][]Observation{
		{ // Max length: 8, Distance: 8 (1)
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 22, 0, location).Unix()), 3},
		},
		{ // Max length: 8, Distance: 8 (1)
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 22, 0, location).Unix()), 3},
		},
		{ // Max length: 10, Distance: 6 (0.6)
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 20, 0, location).Unix()), 3},
		},
		{ // Max length: 5, Distance: 5 (1)
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 25, 0, location).Unix()), 3},
		},
	}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 30, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 0},
		},
	}

	checkRelativeGreenDistancesMetric(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		1,
		1,
		1,
	)
}

func checkMedianGreenLength(
	t *testing.T,
	primarySignalObservations [4][]Observation,
	cycleSecondObservations [4][]Observation,
	expectedGreenLengthsPerCycle [][]float64,
	expectedMetric float64,
) {
	name := "test_name"
	validation := false
	retrieveAllCycleCleanupStats := true
	thing := NewThing(name, validation, retrieveAllCycleCleanupStats)

	for cellIdx := 0; cellIdx < 4; cellIdx++ {
		for i := 0; i < len(primarySignalObservations[cellIdx]); i++ {
			thing.AddObservation("primary_signal", primarySignalObservations[cellIdx][i].phenomenonTime, primarySignalObservations[cellIdx][i].result)
		}

		for i := 0; i < len(cycleSecondObservations[cellIdx]); i++ {
			thing.AddObservation("cycle_second", cycleSecondObservations[cellIdx][i].phenomenonTime, cycleSecondObservations[cellIdx][i].result)
		}

		thing.CalcCycles(cellIdx)
		if len(thing.observationsByDatastreams["primary_signal"]) != 0 {
			t.Errorf("Expected %d observations for primary_signal, got %d", 0, len(thing.observationsByDatastreams["primary_signal"]))
		}
		if len(thing.observationsByDatastreams["cycle_second"]) != 0 {
			t.Errorf("Expected %d observations for cycle_second, got %d", 0, len(thing.observationsByDatastreams["cycle_second"]))
		}
	}

	for cellIdx, cell := range thing.cycles {
		for cycleIdx, cycle := range cell {
			greenLength := thing.getGreenLength(cycle)
			if greenLength != expectedGreenLengthsPerCycle[cellIdx][cycleIdx] {
				t.Errorf("Expected green length %f for cycle %d, got %f", expectedGreenLengthsPerCycle[cellIdx][cycleIdx], cycleIdx, greenLength)
			}
		}
	}

	thing.CalculateMetrics(1, 1)

	if expectedMetric != -1 {
		if thing.MedianGreenLengths[1][1] == -1 {
			t.Errorf("Expected median green length %f, got -1", expectedMetric)
		} else if thing.MedianGreenLengths[1][1] != expectedMetric {
			t.Errorf("Expected median green length %f, got %f", expectedMetric, thing.MedianGreenLengths[1][1])
		}
	} else {
		if thing.MedianGreenLengths[1][1] != -1 {
			t.Errorf("Expected -1 median green length, got %f", thing.MedianGreenLengths[1][1])
		}
	}
}

func TestMedianGreenLength(t *testing.T) {
	location, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		panic(err)
	}

	primarySignalObservations := [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 13, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 22, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 22, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 20, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 25, 0, location).Unix()), 3},
		},
	}

	greenLengthPerCycles := [][]float64{
		{2, 8},
		{4, 8},
		{4, 10},
		{5, 5},
	} // 2 4 4 5 5 8 8 10

	cycleSecondObservations := [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 30, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 0},
		},
	}

	checkMedianGreenLength(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		greenLengthPerCycles,
		5,
	)

	primarySignalObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 1},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 20, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 20, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 24, 0, location).Unix()), 3},
		},
	}

	greenLengthPerCycles = [][]float64{
		{0, 0},
		{4, 10},
		{4, 10},
		{5, 6},
	} // 0 0 4 4 5 6 10 10

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 30, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 0},
		},
	}

	checkMedianGreenLength(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		greenLengthPerCycles,
		4.5,
	)
}

func checkUniqueResults(
	t *testing.T,
	primarySignalObservations [4][]Observation,
	cycleSecondObservations [4][]Observation,
	expectedResults []int8,
) {
	name := "test_name"
	validation := false
	retrieveAllCycleCleanupStats := true
	thing := NewThing(name, validation, retrieveAllCycleCleanupStats)

	for cellIdx := 0; cellIdx < 4; cellIdx++ {
		for i := 0; i < len(primarySignalObservations[cellIdx]); i++ {
			thing.AddObservation("primary_signal", primarySignalObservations[cellIdx][i].phenomenonTime, primarySignalObservations[cellIdx][i].result)
		}

		for i := 0; i < len(cycleSecondObservations[cellIdx]); i++ {
			thing.AddObservation("cycle_second", cycleSecondObservations[cellIdx][i].phenomenonTime, cycleSecondObservations[cellIdx][i].result)
		}

		thing.CalcCycles(cellIdx)
		if len(thing.observationsByDatastreams["primary_signal"]) != 0 {
			t.Errorf("Expected %d observations for primary_signal, got %d", 0, len(thing.observationsByDatastreams["primary_signal"]))
		}
		if len(thing.observationsByDatastreams["cycle_second"]) != 0 {
			t.Errorf("Expected %d observations for cycle_second, got %d", 0, len(thing.observationsByDatastreams["cycle_second"]))
		}
	}

	thing.CalculateMetrics(1, 1)

	if len(thing.Results[1][1]) != len(expectedResults) {
		t.Errorf("Expected %d unique results, got %d", len(expectedResults), len(thing.Results[1][1]))
	}

	for _, result := range thing.Results[1][1] {
		contains := false
		for _, expectedResult := range expectedResults {
			if result == expectedResult {
				contains = true
				break
			}
		}
		if !contains {
			t.Errorf("Did not expect result %d", result)
		}
	}
}

func TestUniqueResults(t *testing.T) {
	location, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		panic(err)
	}

	primarySignalObservations := [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 13, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 22, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 22, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 20, 0, location).Unix()), 3},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 25, 0, location).Unix()), 3},
		},
	}

	uniqueResults := []int8{1, 3}

	cycleSecondObservations := [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 0, 30, 0, location).Unix()), 0},
		},

		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 0, 30, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 15, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 0, 30, 0, location).Unix()), 0},
		},
	}

	checkUniqueResults(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		uniqueResults,
	)

	primarySignalObservations = [4][]Observation{
		{ // Distance: 2
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 9, 29, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 9, 29, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 9, 29, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 9, 29, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 9, 29, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 9, 29, 0, 1, 45, 0, location).Unix()), 1},
		},
		{ // Distance: 2
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 6, 0, 0, 11, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 6, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 6, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 6, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 6, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 6, 0, 1, 45, 0, location).Unix()), 1},
		},
		{ // Distance: 1
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 13, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 13, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 13, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 13, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 13, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 13, 0, 1, 45, 0, location).Unix()), 1},
		},
		{ // Distance: 0
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 0, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 20, 0, 0, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 0, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 20, 0, 0, 46, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 1},
			{int32(time.Date(2023, 10, 20, 0, 1, 10, 0, location).Unix()), 4},
			{int32(time.Date(2023, 10, 20, 0, 1, 12, 0, location).Unix()), 3},
			{int32(time.Date(2023, 10, 20, 0, 1, 40, 0, location).Unix()), 2},
			{int32(time.Date(2023, 10, 20, 0, 1, 46, 0, location).Unix()), 1},
		},
	}

	uniqueResults = []int8{1, 2, 3, 4}

	cycleSecondObservations = [4][]Observation{
		{
			{int32(time.Date(2023, 9, 29, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 9, 29, 0, 2, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 6, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 6, 0, 2, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 13, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 13, 0, 2, 0, 0, location).Unix()), 0},
		},
		{
			{int32(time.Date(2023, 10, 20, 0, 0, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 1, 0, 0, location).Unix()), 0},
			{int32(time.Date(2023, 10, 20, 0, 2, 0, 0, location).Unix()), 0},
		},
	}

	checkUniqueResults(
		t,
		primarySignalObservations,
		cycleSecondObservations,
		uniqueResults,
	)
}
