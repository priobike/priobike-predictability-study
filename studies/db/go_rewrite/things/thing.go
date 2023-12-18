package things

import (
	"encoding/json"
	"fmt"
	"math"
	"math/cmplx"
	"math/rand"
	"os"
	"sort"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"

	"github.com/davidkleiven/gosfft/sfft"
	"github.com/montanaflynn/stats"
)

type cycle struct {
	start   int32
	end     int32
	results []int8
}

type observation struct {
	phenomenonTime int32
	result         int8
}

// Map where the values are sets
var INVALID_STATE_TRANSITIONS = map[int8]map[int8]struct{}{
	1: {2: struct{}{}},
	2: {3: struct{}{}, 4: struct{}{}},
	3: {4: struct{}{}},
	4: {1: struct{}{}, 2: struct{}{}},
}

var MAX_STATE_LENGTHS = map[int8]int8{
	2: 6,
	4: 2,
}

type greenShiftsSet struct {
	list map[int64]struct{}
}

func (s *greenShiftsSet) add(v int64) {
	s.list[v] = struct{}{}
}

func newGreenShiftsSet() *greenShiftsSet {
	s := &greenShiftsSet{}
	s.list = make(map[int64]struct{})
	return s
}

type resultsSet struct {
	list map[int8]struct{}
}

func (s *resultsSet) add(v int8) {
	s.list[v] = struct{}{}
}

func newResultsSet() *resultsSet {
	s := &resultsSet{}
	s.list = make(map[int8]struct{})
	return s
}

type Thing struct {
	// Meta
	name string

	// Settings
	validation                   bool
	retrieveAllCycleCleanupStats bool

	// Data
	observationsByDatastreams map[string][]observation
	cycles                    [4][]cycle

	// Reconstruction stats
	PrimarySignalMissingCount int32
	CycleSecondMissingCount   int32
	TotalSkippedCycles        int32

	// General stats
	TotalCyclesCount       int32
	GapsBetweenCyclesCount int32

	// Cleanup stats
	TotalRemovedCycleCount           int32
	TotalInvalidCycleLengthCount     int32
	TotalInvalidCycleTransitionCount int32
	TotalInvalidCycleMissingCount    int32

	// Metrics
	Metrics            [7][24]float64
	MedianGreenLengths [7][24]float64
	FourierFuzzyness   [7][24][4]float64
	ShiftsFuzzyness    [7][24]float64
	Results            [7][24][]int8
}

func NewThing(name string, validation bool, retrieveAllCycleCleanupStats bool) *Thing {
	thing := new(Thing)
	thing.name = name
	thing.validation = validation
	thing.retrieveAllCycleCleanupStats = retrieveAllCycleCleanupStats
	thing.observationsByDatastreams = map[string][]observation{
		"primary_signal": make([]observation, 0),
		"cycle_second":   make([]observation, 0),
	}
	thing.cycles = [4][]cycle{
		{},
		{},
		{},
		{},
	}
	thing.PrimarySignalMissingCount = 0
	thing.CycleSecondMissingCount = 0
	thing.TotalSkippedCycles = 0
	thing.TotalCyclesCount = 0
	thing.GapsBetweenCyclesCount = 0
	thing.TotalRemovedCycleCount = 0
	thing.TotalInvalidCycleLengthCount = 0
	thing.TotalInvalidCycleTransitionCount = 0
	thing.TotalInvalidCycleMissingCount = 0
	thing.Metrics = [7][24]float64{}
	thing.MedianGreenLengths = [7][24]float64{}
	thing.FourierFuzzyness = [7][24][4]float64{}
	thing.ShiftsFuzzyness = [7][24]float64{}
	thing.Results = [7][24][]int8{}
	return thing
}

func (thing *Thing) AddObservation(layerName string, phenomenonTime int32, result int16) {
	if result > 127 {
		panic("Result is too large for int8.")
	}
	thing.observationsByDatastreams[layerName] = append(thing.observationsByDatastreams[layerName], observation{phenomenonTime, int8(result)})
}

func (thing *Thing) validateCycles(cycles []cycle) {
	if len(cycles) == 0 {
		// print("No cycles to validate.")
		return
	}

	// --------------------------------
	// FIRST: Check if the count of results in the cycles is equal to the difference between the start and end time.
	// --------------------------------

	// List of bools where each bool indicates if a problem was found for a cycle.
	// Thus, if all bools are False, then there are no problems
	secondsInCycleDiffering := make([]bool, len(cycles))
	for i, cycle := range cycles {
		diff := cycle.end - cycle.start
		secondsInCycleDiffering[i] = diff != int32(len(cycle.results))
	}

	// Count true values in secondsInCycleDiffering
	diffCount := 0
	for _, value := range secondsInCycleDiffering {
		if value {
			diffCount++
		}
	}
	if diffCount > 0 {
		panic("Attention: Number of cycles with differing start/end times. This should not happen and is a bug.")
	}

	// --------------------------------
	// SECOND: Check for the following, if there exist a corresponding observation:
	// 1. Random cycle start time: Is there a corresponding cycle_second observation for that thing?
	// 2. Random result in cycle: Is there a corresponding primary_signal observation for that thing?
	// Do this multiple times, just to make sure..
	// --------------------------------

	// Count of how many checks we made (primary signal and cycle second are not counted seperatly).
	checked_count := 0

	for checked_count < 50 {
		checked_count++

		// Random cycle
		cycle := cycles[rand.Intn(len(cycles))]

		cycleStart := cycle.start
		cycleSecondObservationsFound := 0
		for _, observation := range thing.observationsByDatastreams["cycle_second"] {
			if observation.phenomenonTime == cycleStart {
				cycleSecondObservationsFound++
			}
		}
		if cycleSecondObservationsFound != 1 {
			panic("Attention: No or multiple corresponding cycle second observations found. This should not happen and is a bug.")
		}

		// Find all state changes (e.g. when result changes from 2 to 1)
		stateChanges := make([][2]int32, 0)
		var previousResult *int8
		for i := int32(0); i < int32(len(cycle.results)); i++ {
			if previousResult != nil && *previousResult != cycle.results[i] {
				stateChange := [2]int32{int32(cycle.results[i]), cycle.start + i}
				stateChanges = append(stateChanges, stateChange)
			}
			previousResult = &cycle.results[i]
		}

		// Random state change (result and exact time of result)
		if len(stateChanges) == 0 {
			// No state changes in cycle
			continue
		}
		stateChangeIdx := rand.Intn(len(stateChanges))
		result := stateChanges[stateChangeIdx][0]

		// Check if there is a corresponding primary signal observation
		primarySignalObservationsFound := 0
		for _, observation := range thing.observationsByDatastreams["primary_signal"] {
			if observation.phenomenonTime == stateChanges[stateChangeIdx][1] && observation.result == int8(result) {
				primarySignalObservationsFound++
			}
		}
		if primarySignalObservationsFound != 1 {
			panic("Attention: No or multiple corresponding primary signal observations found. This should not happen and is a bug.")
		}
	}
}

func (thing *Thing) CalcCycles(cellIdx int) {
	cycles, skippedCycles, primarySignalMissing, cycleSecondMissing := thing.reconstructCycles()
	// println("Cycle count: ", len(cycles))

	if thing.validation {
		thing.validateCycles(cycles)
	}

	cycles = thing.cleanUpCycles(cycles)
	// println("Cycle count after cleanup: ", len(cycles))

	thing.cycles[cellIdx] = cycles
	thing.TotalSkippedCycles += skippedCycles
	if primarySignalMissing {
		thing.PrimarySignalMissingCount++
	}
	if cycleSecondMissing {
		thing.CycleSecondMissingCount++
	}
	thing.observationsByDatastreams["primary_signal"] = make([]observation, 0)
	thing.observationsByDatastreams["cycle_second"] = make([]observation, 0)
}

func (thing *Thing) phaseWiseDistance(cycle1 cycle, cycle2 cycle) float64 {
	distance := 0.0
	length := max(len(cycle1.results), len(cycle2.results))
	for i := 0; i < length; i++ {
		if i >= len(cycle1.results) {
			distance += 1.0
			continue
		}
		if i >= len(cycle2.results) {
			distance += 1.0
			continue
		}
		if cycle1.results[i] != cycle2.results[i] {
			distance += 1.0
		}
	}

	return distance
}

func (thing *Thing) getGreenIndices(cycle cycle) []int {
	// 3 = green
	// Looking for the indices where the result changes from something else to green or from green to something else.
	indices := make([]int, 0)
	previousResult := int8(-1)
	for idx, result := range cycle.results {
		if previousResult != result && result == 3 {
			indices = append(indices, idx)
		} else if previousResult == 3 && result != 3 {
			indices = append(indices, idx-1)
		}
		if idx == len(cycle.results)-1 && result == 3 {
			indices = append(indices, idx)
		}
		previousResult = result
	}

	return indices
}

func (thing *Thing) getGreenLength(cycle cycle) float64 {
	greenLength := 0.0
	for _, result := range cycle.results {
		if result == 3 {
			greenLength++
		}
	}

	return greenLength
}

func (thing *Thing) CalculateFourierFuzyness(day int, hour int, cellIdx int, cellStartTime int32, cellEndTime int32) {
	if len(thing.cycles[cellIdx]) == 0 {
		thing.FourierFuzzyness[day][hour][cellIdx] = -1.0
		return
	}

	denseGreenStates := make([]float64, 0)
	previousEnd := int32(0)
	for idx, cycle := range thing.cycles[cellIdx] {
		if idx == 0 && cycle.start != cellStartTime {
			timeDiff := cycle.start - cellStartTime
			for i := int32(0); i < timeDiff; i++ {
				denseGreenStates = append(denseGreenStates, 0)
			}
		}
		if previousEnd != 0 && previousEnd != cycle.start {
			timeDiff := cycle.start - previousEnd
			for i := int32(0); i < timeDiff; i++ {
				denseGreenStates = append(denseGreenStates, 0)
			}
		}
		for _, result := range cycle.results {
			if result == 3 {
				denseGreenStates = append(denseGreenStates, 1)
			} else {
				denseGreenStates = append(denseGreenStates, 0)
			}
		}
		if idx == len(thing.cycles[cellIdx])-1 && cycle.end != cellEndTime {
			timeDiff := cellEndTime - cycle.end
			for i := int32(0); i < timeDiff; i++ {
				denseGreenStates = append(denseGreenStates, 0)
			}
		}
		previousEnd = cycle.end
	}

	if len(denseGreenStates) != 60*60 {
		println("Cell start time: ", cellStartTime)
		println("Cell end time: ", cellEndTime)
		println("Cycle count: ", len(thing.cycles[cellIdx]))
		println("Cycles: ")
		for _, cycle := range thing.cycles[cellIdx] {
			println("Start: ", cycle.start)
			println("End: ", cycle.end)
			for _, result := range cycle.results {
				print(result)
			}
			println(" ")
		}
		panic("Length of dense green states is not 60*60. Length: " + fmt.Sprint(len(denseGreenStates)))
	}

	fft := sfft.NewFFT1(len(denseGreenStates))
	ftData := fft.FFT(denseGreenStates)
	ftAmp := make([]float64, len(denseGreenStates)/2+1)
	for i := range ftData {
		ftAmp[i] = cmplx.Abs(ftData[i])
	}

	fuzzyness, err := stats.Mean(ftAmp)
	if err != nil {
		panic(err)
	}

	thing.FourierFuzzyness[day][hour][cellIdx] = fuzzyness

	denseGreenStatesString := ""
	for _, denseGreenState := range denseGreenStates {
		denseGreenStatesString += fmt.Sprint(denseGreenState)
	}

	cyclesDebugStruct := make([]struct {
		Start   int32
		End     int32
		Results string
	}, 0)
	for _, cycle := range thing.cycles[cellIdx] {
		cyclesDebugStruct = append(cyclesDebugStruct, struct {
			Start   int32
			End     int32
			Results string
		}{cycle.start, cycle.end, fmt.Sprint(cycle.results)})
	}

	debugMetaStruct := struct {
		Name             string
		Day              int
		Hour             int
		CellIdx          int
		CellStart        int32
		CellEnd          int32
		Fuzzyness        float64
		DenseGreenStates string
		Cycles           []struct {
			Start   int32
			End     int32
			Results string
		}
	}{
		Name:             thing.name,
		Day:              day,
		Hour:             hour,
		CellIdx:          cellIdx,
		CellStart:        cellStartTime,
		CellEnd:          cellEndTime,
		Fuzzyness:        fuzzyness,
		DenseGreenStates: denseGreenStatesString,
		Cycles:           cyclesDebugStruct,
	}

	file, _ := json.MarshalIndent(debugMetaStruct, "", " ")
	path := "things/outputs/" + thing.name + "_" + fmt.Sprint(day) + "_" + fmt.Sprint(hour) + "_" + fmt.Sprint(cellIdx) + ".json"
	_ = os.WriteFile(path, file, 0644)

	// Plot the signal and the result
	pltSignal := plot.New()
	pltSignal.X.Label.Text = "Time (s)"
	pltSignal.Y.Label.Text = "Amplitude"
	pts := make(plotter.XYs, len(denseGreenStates))
	for i := range denseGreenStates {
		pts[i] = plotter.XY{X: float64(i), Y: denseGreenStates[i]}
	}

	l, err := plotter.NewLine(pts)
	if err != nil {
		panic(err)
	}
	pltSignal.Add(l)
	// Plot the frequency spectrum
	pltFreq := plot.New()
	if err != nil {
		panic(err)
	}

	pltFreq.X.Label.Text = "Frequency (Hz)"
	pltFreq.Y.Label.Text = "Amplitude"
	freqData := make(plotter.XYs, len(ftAmp))
	for i := range ftAmp {
		freqData[i] = plotter.XY{X: fft.Freq(i), Y: ftAmp[i]}
	}

	l, err = plotter.NewLine(freqData)
	if err != nil {
		panic(err)
	}
	pltFreq.Add(l)

	// Save result
	signal_fig_path := "things/outputs/" + thing.name + "_" + fmt.Sprint(day) + "_" + fmt.Sprint(hour) + "_" + fmt.Sprint(cellIdx) + "_signal.png"
	if err := pltSignal.Save(4*vg.Inch, 4*vg.Inch, signal_fig_path); err != nil {
		panic(err)
	}

	freq_fig_path := "things/outputs/" + thing.name + "_" + fmt.Sprint(day) + "_" + fmt.Sprint(hour) + "_" + fmt.Sprint(cellIdx) + "_freq.png"
	if err := pltFreq.Save(4*vg.Inch, 4*vg.Inch, freq_fig_path); err != nil {
		panic(err)
	}
}

func (thing *Thing) GetDurationsBetweenGreenPhases(results []int8) []int64 {
	indexOfEndOfLastGreen := -1
	durationsBetweenGreenphases := make([]int64, 0)
	previousResult := int8(-1)
	for idx, result := range results {
		if previousResult == 3 && result != 3 {
			// End of green phase
			indexOfEndOfLastGreen = idx - 1
		} else if previousResult != 3 && result == 3 {
			// Start of green phase
			if indexOfEndOfLastGreen != -1 {
				durationsBetweenGreenphases = append(durationsBetweenGreenphases, int64(idx-indexOfEndOfLastGreen))
			}
		}

		previousResult = result
	}

	return durationsBetweenGreenphases
}

func (thing *Thing) CalculateMetrics(day int, hour int) {
	distances := make([]float64, 0)
	greenLengths := make([]float64, 0)
	greenShiftsSet := newGreenShiftsSet()
	cycles := []cycle{}
	cyclesCutByCellsAndGaps := [][]cycle{}
	totalGreenShiftCount := 0
	uniqueResults := newResultsSet()
	for _, cellCycles := range thing.cycles {
		currentCycles := []cycle{}
		continuousResults := make([]int8, 0)
		for idx, currentCycle := range cellCycles {
			for _, result := range currentCycle.results {
				uniqueResults.add(result)
				continuousResults = append(continuousResults, result)
			}
			currentCycles = append(currentCycles, currentCycle)
			cycles = append(cycles, currentCycle)
			greenLengths = append(greenLengths, thing.getGreenLength(currentCycle))
			if idx >= len(cellCycles)-1 {
				break
			}
			// There is a gap between the cycles
			if currentCycle.end != cellCycles[idx+1].start {
				thing.GapsBetweenCyclesCount++

				if len(continuousResults) != 0 {
					durationsBetweenGreenphases := thing.GetDurationsBetweenGreenPhases(continuousResults)
					for _, duration := range durationsBetweenGreenphases {
						greenShiftsSet.add(duration)
						totalGreenShiftCount++
					}
				}

				continuousResults = make([]int8, 0)
				if len(currentCycles) != 0 {
					cyclesCutByCellsAndGaps = append(cyclesCutByCellsAndGaps, currentCycles)
					currentCycles = []cycle{}
				}
				continue
			}

			distances = append(distances, thing.phaseWiseDistance(currentCycle, cellCycles[idx+1]))
		}

		if len(continuousResults) != 0 {
			durationsBetweenGreenphases := thing.GetDurationsBetweenGreenPhases(continuousResults)
			for _, duration := range durationsBetweenGreenphases {
				greenShiftsSet.add(duration)
				totalGreenShiftCount++
			}
		}

		if len(currentCycles) != 0 {
			cyclesCutByCellsAndGaps = append(cyclesCutByCellsAndGaps, currentCycles)
		}
	}

	IcyPD := -1.0
	IgpTD := -1.0

	results := []int8{}
	for result := range uniqueResults.list {
		results = append(results, result)
	}
	thing.Results[day][hour] = results

	greenShifts := []float64{}
	for greenShift := range greenShiftsSet.list {
		greenShifts = append(greenShifts, float64(greenShift))
	}
	if len(greenShifts) == 0 {
		thing.ShiftsFuzzyness[day][hour] = -1.0
	} else {
		shiftsFuzzyness := float64(len(greenShifts)) / float64(totalGreenShiftCount)
		thing.ShiftsFuzzyness[day][hour] = shiftsFuzzyness

		IgpTD = shiftsFuzzyness

		/* cyclesDebugStruct := make([]struct {
			Start   int32
			End     int32
			Results string
		}, 0)
		for _, cycle := range cycles {
			cyclesDebugStruct = append(cyclesDebugStruct, struct {
				Start   int32
				End     int32
				Results string
			}{cycle.start, cycle.end, fmt.Sprint(cycle.results)})
		}
		debugMetaStruct := struct {
			Name             string
			Day              int
			Hour             int
			CellIdx          int
			CellStart        int32
			CellEnd          int32
			Fuzzyness        float64
			DenseGreenStates string
			Cycles           []struct {
				Start   int32
				End     int32
				Results string
			}
		}{
			Name:      thing.name,
			Day:       day,
			Hour:      hour,
			Fuzzyness: shiftsFuzzyness,
			Cycles:    cyclesDebugStruct,
		}

		file, _ := json.MarshalIndent(debugMetaStruct, "", " ")
		path := "things/outputs/" + thing.name + "_" + fmt.Sprint(day) + "_" + fmt.Sprint(hour) + ".json"
		_ = os.WriteFile(path, file, 0644) */
	}

	if len(greenLengths) == 0 {
		thing.MedianGreenLengths[day][hour] = -1.0
	} else {
		medianGreenLength, err := stats.Median(greenLengths)
		if err != nil {
			panic(err)
		}
		thing.MedianGreenLengths[day][hour] = medianGreenLength
	}

	if len(distances) == 0 {
		thing.Metrics[day][hour] = -1.0
	} else {
		medianDistance, err := stats.Median(distances)
		if err != nil {
			panic(err)
		}
		thing.Metrics[day][hour] = medianDistance

		IcyPD = medianDistance
	}

	if IcyPD != -1.0 && IgpTD != -1.0 {
		name := ""
		if IcyPD < 5 && IgpTD < 0.1 && len(cycles) > 150 {
			name = ""
		} else if IcyPD < 5 && IgpTD >= 0.9 && len(cycles) > 150 {
			name = ""
		} else if IcyPD >= 25 && IgpTD < 0.1 && len(cycles) > 10 {
			name = "bottom_right"
		} else if IcyPD >= 25 && IgpTD >= 0.9 && len(cycles) > 50 {
			name = "top_right"
		}

		if name != "" {
			cyclesDebugStruct := make([][]struct {
				Start   int32
				End     int32
				Results string
			}, 0)
			for _, cycles := range cyclesCutByCellsAndGaps {
				cyclesDebugStructCell := make([]struct {
					Start   int32
					End     int32
					Results string
				}, 0)
				for _, cycle := range cycles {
					cyclesDebugStructCell = append(cyclesDebugStructCell, struct {
						Start   int32
						End     int32
						Results string
					}{cycle.start, cycle.end, fmt.Sprint(cycle.results)})
				}
				cyclesDebugStruct = append(cyclesDebugStruct, cyclesDebugStructCell)
			}
			debugMetaStruct := struct {
				Name      string
				Day       int
				Hour      int
				CellIdx   int
				CellStart int32
				CellEnd   int32
				IgpTD     float64
				IcyPD     float64
				Cycles    [][]struct {
					Start   int32
					End     int32
					Results string
				}
			}{
				Name:   thing.name,
				Day:    day,
				Hour:   hour,
				IgpTD:  IgpTD,
				IcyPD:  IcyPD,
				Cycles: cyclesDebugStruct,
			}

			name = name + "_" + thing.name + "_" + fmt.Sprint(day) + "_" + fmt.Sprint(hour) + ".json"

			file, _ := json.MarshalIndent(debugMetaStruct, "", " ")
			path := "things/case_studies/" + name
			_ = os.WriteFile(path, file, 0644)
		}

	}

	thing.cycles = [4][]cycle{
		{},
		{},
		{},
		{},
	}
}

func (thing *Thing) reconstructCycles() ([]cycle, int32, bool, bool) {
	// Primary signal observations and cycle second observations are required.
	primarySignalMissing := false
	cycleSecondMissing := false

	// Check if required datastreams are present. Early return if not.
	if _, ok := thing.observationsByDatastreams["primary_signal"]; !ok {
		primarySignalMissing = true
	}
	if _, ok := thing.observationsByDatastreams["cycle_second"]; !ok {
		cycleSecondMissing = true
	}
	if primarySignalMissing || cycleSecondMissing {
		return nil, 0, primarySignalMissing, cycleSecondMissing
	}

	if len(thing.observationsByDatastreams["primary_signal"]) == 0 {
		primarySignalMissing = true
	}
	if len(thing.observationsByDatastreams["cycle_second"]) == 0 {
		cycleSecondMissing = true
	}
	if primarySignalMissing || cycleSecondMissing {
		return nil, 0, primarySignalMissing, cycleSecondMissing
	}

	// Sort observations by phenomenon time.
	sort.Slice(thing.observationsByDatastreams["primary_signal"], func(i, j int) bool {
		return thing.observationsByDatastreams["primary_signal"][i].phenomenonTime <
			thing.observationsByDatastreams["primary_signal"][j].phenomenonTime
	})
	sort.Slice(thing.observationsByDatastreams["cycle_second"], func(i, j int) bool {
		return thing.observationsByDatastreams["cycle_second"][i].phenomenonTime <
			thing.observationsByDatastreams["cycle_second"][j].phenomenonTime
	})

	primarySignalObservations := thing.observationsByDatastreams["primary_signal"]
	cycleSecondObservations := thing.observationsByDatastreams["cycle_second"]

	primarySignalObserationsCount := len(primarySignalObservations)
	cycleSecondObservationsCount := len(cycleSecondObservations)

	// Current looked at primary signal observation.
	primarySignalIndex := 0
	// Current looked at cycle second observation.
	cycleSecondIndex := 0

	firstPrimarySignalPhenonmenonTime := primarySignalObservations[primarySignalIndex].phenomenonTime
	firstCycleSecondPhenonmenonTime := cycleSecondObservations[cycleSecondIndex].phenomenonTime

	// The chances are very low that we only receive one primary signal (if none are received at all we already have an early return).
	// Thus if this happens we throw an exception to indicate that there might be a bug in the code leading to this.
	/* if len(primarySignalObservations) == 1 {
		primarySignalMissing = true
	}
	if len(cycleSecondObservations) == 1 {
		cycleSecondMissing = true
	}
	if primarySignalMissing && cycleSecondMissing {
		return nil, 0, primarySignalMissing, cycleSecondMissing
	}
	if primarySignalMissing {
		println(" ")
		println(thing.name)
		println("Attention: Only one primary signal observation received. This should not happen and might be a bug. Count of cycle second observations: ", len(cycleSecondObservations))
		return nil, 0, primarySignalMissing, cycleSecondMissing
	}
	if cycleSecondMissing {
		println(" ")
		println(thing.name)
		println("Attention: Only one cycle second observation received. This should not happen and might be a bug. Count of primary signal observations: ", len(primarySignalObservations))
		return nil, 0, primarySignalMissing, cycleSecondMissing
	} */

	// The result of the current primary signal
	result := primarySignalObservations[primarySignalIndex].result
	// The phenomenon time of the next primary signal observation (used to look ahead when we switch to the next primary signal observation).
	var upcomingPrimarySignalObservationPhenomenonTime *int32
	if primarySignalIndex+1 < primarySignalObserationsCount {
		upcomingPrimarySignalObservationPhenomenonTime = &primarySignalObservations[primarySignalIndex+1].phenomenonTime
	} else {
		upcomingPrimarySignalObservationPhenomenonTime = nil
	}

	// We start at the first received primary signal or cycle second observation and go on second by second.
	// During this process we construct cycles and throw away primary signals that don't belong to a cycle.
	// If the primary signal came before the cycle it's important to start there such that we know the result one the cycle starts.
	// If the cycle came before the primary signal we start there because we don't know the result of the primary signal before the cycle starts.
	// We only try to use the result last primary signal of the previous window.
	tickerSecond := min(firstPrimarySignalPhenonmenonTime, firstCycleSecondPhenonmenonTime)

	// Before we reconstruct the programs we first reconstruct all cycles regardless of the programs.
	cycles := make([]cycle, 0)

	// Where we save the data (start time, end time, primary signal observation results) of the current cycle.
	var currentCycle *cycle

	// Start and end phenomenon time of the currently looked at cycle
	var cycleTimeStart *int32
	var cycleTimeEnd *int32

	// How many times we skipped cycles where the primary signals were missing
	skippedCycles := int32(0)

	for tickerSecond <= cycleSecondObservations[len(cycleSecondObservations)-1].phenomenonTime {
		// First cycle
		if cycleTimeStart == nil {
			if cycleSecondIndex+1 >= cycleSecondObservationsCount {
				// End of data ("+ 1") because we also need to have an end for the cycle
				break
			}
			cycleTimeStart = &cycleSecondObservations[cycleSecondIndex].phenomenonTime
		}
		if cycleTimeEnd == nil {
			if cycleSecondIndex+1 >= cycleSecondObservationsCount {
				// End of data ("+ 1") because we also need to have an end for the cycle
				break
			}
			cycleTimeEnd = &cycleSecondObservations[cycleSecondIndex+1].phenomenonTime
		}

		// Update current cycle for all upcoming cycles after the first cycle.
		if tickerSecond >= *cycleTimeEnd {
			// If we proceed to the next cycle without having saved any data for the current cycle this means that there we no corresponding primary signals observations.
			// Thus we skip this cycle.
			if currentCycle == nil {
				skippedCycles++
			} else {
				// Save the current cycle
				cycles = append(cycles, *currentCycle)
				currentCycle = nil
			}

			// Update cycle time
			cycleSecondIndex++
			if cycleSecondIndex+1 >= cycleSecondObservationsCount {
				// End of data ("+ 1") because we also need to have an end for the cycle
				break
			}
			cycleTimeStart = &cycleSecondObservations[cycleSecondIndex].phenomenonTime
			cycleTimeEnd = &cycleSecondObservations[cycleSecondIndex+1].phenomenonTime
		}

		// We reached a time with the ticker where we have a new primary signal observation.
		if upcomingPrimarySignalObservationPhenomenonTime != nil && tickerSecond >= *upcomingPrimarySignalObservationPhenomenonTime {
			// Update the current primary signal.
			primarySignalIndex++
			result = primarySignalObservations[primarySignalIndex].result

			// Check if there are still primary signal observations left and if so update the upcoming primary signal observation phenomenon time.
			if primarySignalIndex+1 < primarySignalObserationsCount {
				upcomingPrimarySignalObservationPhenomenonTime = &primarySignalObservations[primarySignalIndex+1].phenomenonTime
			} else {
				upcomingPrimarySignalObservationPhenomenonTime = nil
			}
		}

		// If the current cycle is none (either because it is the first cycle or because we just saved the last cycle) we create a new cycle,
		// but only if the ticker is at the start of the current cycle.
		// This is checked to assure that we only create cycles where we have corresponding primary signal observation data.
		if currentCycle == nil && tickerSecond == *cycleTimeStart {
			// Check is important to make sure that we don't create a cycle when the phenomenon time of our current primary signal observation
			// is after the start of the current cycle.
			if primarySignalObservations[primarySignalIndex].phenomenonTime <= *cycleTimeStart {
				currentCycle = &cycle{*cycleTimeStart, *cycleTimeEnd, make([]int8, 0)}
			}
		}

		// Fill up the results of the current cycle with the current primary signal observation result until:
		// option 1: we reach the end of the current cycle
		// option 2: we reach the next primary signal observation
		// The option that comes first is the one that is executed.
		if currentCycle != nil && tickerSecond >= *cycleTimeStart {
			var diffUpcoming int32
			if upcomingPrimarySignalObservationPhenomenonTime == nil {
				diffUpcoming = math.MaxInt32
			} else {
				diffUpcoming = *upcomingPrimarySignalObservationPhenomenonTime - tickerSecond
			}

			diffCycleEnd := *cycleTimeEnd - tickerSecond

			diff := min(diffUpcoming, diffCycleEnd)
			resultsToAdd := make([]int8, diff)
			for i := int32(0); i < diff; i++ {
				resultsToAdd[i] = result
			}
			currentCycle.results = append(currentCycle.results, resultsToAdd...)

			tickerSecond += diff
		} else {
			tickerSecond++
		}
	}

	return cycles, skippedCycles, primarySignalMissing, cycleSecondMissing
}

func (thing *Thing) cleanUpCycles(cycles []cycle) []cycle {
	if len(cycles) == 0 {
		return []cycle{}
	}

	/* Goal: Remove cycles that are invalid or too long/short.

	Color encoding:
	0 = dark
	1 = red
	2 = amber
	3 = green
	4 = red amber
	5 = amber flashing
	6 = green flashing

	1. Find invalid state transitions.

		Typical cycles:
		1. -> Red -> RedAmber -> Green -> Amber -> Red ->
		2. -> Red -> Green -> Red ->

		Thus, we can safely say that the following state transitions are invalid:
		Red -> Amber
		Amber -> Green
		Amber -> RedAmber
		Green -> RedAmber
		RedAmber -> Red
		RedAmber -> Amber

	2. Find missing observations.

		We can do that by looking at the length of amber and red amber phases.
		By definition, amber is maximum 6 seconds long and red amber is maximum 2 seconds long. */

	// Stats
	cyclesCount := int32(len(cycles))
	removedCycleCount := int32(0)
	invalidCycleLengthCount := int32(0)
	invalidCycleTransitionCount := int32(0)
	invalidCycleMissingCount := int32(0)

	cleanedUpCycles := make([]cycle, 0)
	cycleLengths := make([]float64, 0)
	for _, cycle := range cycles {
		cycleLengths = append(cycleLengths, float64(len(cycle.results)))
	}

	medianCycleLength, err := stats.Median(cycleLengths)
	if err != nil {
		panic(err)
	}
	for _, cycle := range cycles {
		results := &cycle.results
		// Check for too long or too short cycles
		wrongLength := false
		if float64(len(*results)) > medianCycleLength*1.5 || float64(len(*results)) < medianCycleLength*0.5 {
			wrongLength = true
		}

		if !thing.retrieveAllCycleCleanupStats && wrongLength {
			// DEBUGGING ONLY
			/* println("Wrong length")
			println("Median cycle length: ", medianCycleLength)
			println(" ")
			for _, result := range *results {
				print(result)
			}
			println(" ")
			println("Cycle length: ", len(*results)) */
			removedCycleCount++
			continue
		}

		// Check for invalid state transitions
		invalidTransition := false
		var currentState *int8
		for i := 0; i < len(*results); i++ {
			if currentState != nil {
				if _, ok := INVALID_STATE_TRANSITIONS[*currentState][(*results)[i]]; ok {
					invalidTransition = true
					break
				}
			}
			if _, ok := INVALID_STATE_TRANSITIONS[(*results)[i]]; ok {
				currentState = &(*results)[i]
			} else {
				currentState = nil
			}
		}

		if !thing.retrieveAllCycleCleanupStats && invalidTransition {
			// DEBUGGING ONLY
			/* println("Invalid transition")
			println(" ")
			for _, result := range *results {
				print(result)
			}
			println(" ") */
			removedCycleCount++
			continue
		}

		// Check for missing observations
		missingObservation := false
		var maxStateLength *int8
		maxStateLengthCounter := int8(0)

		for i := 0; i < len(*results); i++ {
			if maxStateLength != nil {
				if (*results)[i] == (*results)[i-1] {
					maxStateLengthCounter++
					if maxStateLengthCounter > *maxStateLength {
						missingObservation = true
						break
					}
				} else {
					maxStateLengthCounter = 1
					maxStateLength = nil
				}
			}
			if maxStateLength == nil {
				if length, ok := MAX_STATE_LENGTHS[(*results)[i]]; ok {
					maxStateLength = &length
					maxStateLengthCounter = 1
				}
			}
		}

		if !thing.retrieveAllCycleCleanupStats && missingObservation {
			removedCycleCount++
			continue
		}

		if wrongLength || invalidTransition || missingObservation {
			removedCycleCount++
			if wrongLength {
				invalidCycleLengthCount++
			}
			if invalidTransition {
				invalidCycleTransitionCount++
			}
			if missingObservation {
				invalidCycleMissingCount++
			}
		} else {
			cleanedUpCycles = append(cleanedUpCycles, cycle)
		}
	}

	thing.TotalCyclesCount += cyclesCount
	thing.TotalRemovedCycleCount += removedCycleCount

	thing.TotalInvalidCycleLengthCount += invalidCycleLengthCount
	thing.TotalInvalidCycleTransitionCount += invalidCycleTransitionCount
	thing.TotalInvalidCycleMissingCount += invalidCycleMissingCount

	return cleanedUpCycles
}
