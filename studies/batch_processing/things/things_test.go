package things

import (
	"testing"
)

func TestThingsProvider(t *testing.T) {
	tp := NewThingsProvider(true)
	if len(tp.Things) == 0 {
		t.Errorf("Expected multiple things, got %v", len(tp.Things))
	}

	tp.FilterOnlyPrimarySignalAndCycleSecondDatastreams()

	things := tp.Things
	for _, thing := range things {
		for _, datastream := range thing.Datastreams {
			if datastream.Properties.LayerName != "primary_signal" && datastream.Properties.LayerName != "cycle_second" {
				t.Errorf("Expected only primary_signal and cycle_second datastreams, got %v", datastream.Properties.LayerName)
			}
		}
	}

	tp = NewThingsProvider(true)
	if len(tp.Things) == 0 {
		t.Errorf("Expected multiple things, got %v", len(tp.Things))
	}

	tp.FilterOnlySecondarySignalAndCycleSecondDatastreams()

	things = tp.Things
	for _, thing := range things {
		for _, datastream := range thing.Datastreams {
			if datastream.Properties.LayerName != "secondary_signal" && datastream.Properties.LayerName != "cycle_second" {
				t.Errorf("Expected only secondary_signal and cycle_second datastreams, got %v", datastream.Properties.LayerName)
			}
		}
	}
}
