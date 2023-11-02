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
	}

	thing.CalculateMetrics(dayIdx, hourIdx)

	if thing.Metrics[dayIdx][hourIdx] != expectedMetric {
		t.Errorf("Expected metric %f, got %f", expectedMetric, thing.Metrics[dayIdx][hourIdx])
	}
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
}
