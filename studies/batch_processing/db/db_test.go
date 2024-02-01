package db

import (
	"fmt"
	"strings"
	"testing"
)

func TestDBQuery(t *testing.T) {
	cells := [4][2]int32{
		{24563456, 34563456},
		{34578368, 23475284},
		{96392854, 28345794},
		{82345927, 23457815},
	}

	query := GetCellsAllDatastreamsQuery(cells)

	for _, cell := range cells {
		if !strings.Contains(query, fmt.Sprint(cell[0])) {
			t.Errorf("Expected query to contain %v", fmt.Sprint(cell[0]))
		}
		if !strings.Contains(query, fmt.Sprint(cell[1])) {
			t.Errorf("Expected query to contain %v", fmt.Sprint(cell[1]))
		}
	}
}
