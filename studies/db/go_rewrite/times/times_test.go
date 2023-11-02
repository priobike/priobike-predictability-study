package times

import (
	"testing"
	"time"
)

func TestGetCells(t *testing.T) {
	cells := GetCells()

	if len(cells) != 7 {
		t.Errorf("Expected 7 days, got %v", len(cells))
	}

	for _, day := range cells {
		if len(day) != 24 {
			t.Errorf("Expected 24 hours, got %v", len(day))
		}
		for _, hour := range day {
			if len(hour) != 4 {
				t.Errorf("Expected 4 cells, got %v", len(hour))
			}
			firstStart := hour[0][0]
			firstEnd := hour[0][1]
			firstStartDate := time.Unix(int64(firstStart), 0)
			firstEndDate := time.Unix(int64(firstEnd), 0)
			for cellIdx, cell := range hour {
				if len(cell) != 2 {
					t.Errorf("Expected 2 timestamps, got %v", len(cell))
				}
				cellStartDate := time.Unix(int64(cell[0]), 0)
				cellEndDate := time.Unix(int64(cell[1]), 0)
				differenceStart := firstStartDate.AddDate(0, 0, 7*cellIdx)
				differenceEnd := firstEndDate.AddDate(0, 0, 7*cellIdx)
				if differenceStart != cellStartDate {
					t.Errorf("Expected start date %v, got %v", firstStartDate, differenceStart)
				}
				if differenceEnd != cellEndDate {
					t.Errorf("Expected end date %v, got %v", firstEndDate, differenceEnd)
				}
			}
		}
	}
}
