package structs

import (
	"encoding/json"
	"fmt"
	"time"
)

// The observation model.
type Observation struct {
	// The time when the observation was made (at the site).
	PhenomenonTime time.Time `json:"phenomenonTime"`
	// The time when the observation was processed by the UDP.
	ResultTime time.Time `json:"resultTime"`
	// The time when we received the observation.
	// Note: This isn't actually in the JSON, but we add it ourselves.
	// With this we can calculate the delay of the observation.
	ReceivedTime time.Time `json:"receivedTime"`
	// The result of the observation.
	Result int16 `json:"result"`
}

// Unmarshal an observation from JSON.
func (o *Observation) UnmarshalJSON(data []byte) error {
	receivedTime := time.Now()
	var temp struct {
		PhenomenonTime time.Time `json:"phenomenonTime"`
		ResultTime     time.Time `json:"resultTime"`
	}

	// Check if the result field in the JSON is of boolean type.
	var dataMap map[string]interface{}
	err := json.Unmarshal(data, &dataMap)
	if err != nil {
		panic(err)
	}

	var result, ok = dataMap["result"]
	if !ok {
		panic("Result is nil")
	}

	err2 := json.Unmarshal(data, &temp)
	if err2 != nil {
		return err
	}

	switch r := result.(type) {
	case bool:
		if r {
			o.Result = 1
		} else {
			o.Result = 0
		}
	case int:
		o.Result = int16(r)
	case float64:
		o.Result = int16(r)
	default:
		panic("Result is not a bool, float64 or int. Result is of type: " + fmt.Sprintf("%T", result) + " and has value: " + fmt.Sprintf("%v", result))
	}

	o.PhenomenonTime = temp.PhenomenonTime
	o.ResultTime = temp.ResultTime
	o.ReceivedTime = receivedTime
	return nil
}
