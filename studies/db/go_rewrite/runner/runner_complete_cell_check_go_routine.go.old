/* package runner

import (
	"sync"
)

type BlaNumber struct {
	Values [7]int
}

func NewNumber() *BlaNumber {
	blaNumber := new(BlaNumber)
	for i := 0; i < 7; i++ {
		blaNumber.Values[i] = -1
	}
	return blaNumber
}

func (blaNumber *BlaNumber) add(amount int, idx int) {
	oldValue := blaNumber.Values[idx]
	blaNumber.Values[idx] = oldValue + amount
}

func check() {
	mapBla := [7]map[string]*BlaNumber{}
	for idx := range mapBla {
		mapBla[idx] = map[string]*BlaNumber{}
	}
	for idx := range mapBla {
		numberA := NewNumber()
		numberB := NewNumber()
		numberC := NewNumber()
		numberD := NewNumber()
		mapBla[idx]["a"] = numberA
		mapBla[idx]["b"] = numberB
		mapBla[idx]["c"] = numberC
		mapBla[idx]["d"] = numberD
	}
	var wg sync.WaitGroup
	for i := 0; i < 7; i++ {
		var pointer *int
		value := 8
		pointer = &value
		wg.Add(1)
		go func(index int, pointer *int, numbers *map[string]*BlaNumber) {
			defer wg.Done()
			//for i := 0; i < 7; i++ {
			//time.Sleep(1 * time.Second)
			// println("Index ", index, "Pointer ", pointer)
			//}
			i := 1
			for _, blaNumber := range *numbers {
				// println("Index ", index, "Key ", key)
				blaNumber.add(i, index)
				i += 1
			}
		}(i, pointer, &mapBla[i])
	}

	wg.Wait()

	println("Fertsch")

	for idx := range mapBla {
		for key2, blaNumber2 := range mapBla[idx] {
			for valueIdx, value := range blaNumber2.Values {
				println("Index ", idx, "Key2 ", key2, "valueIdx", valueIdx, "value", value)
			}
		}
	}
}
*/