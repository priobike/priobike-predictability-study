package times

import (
	"time"
	"fmt"
)

func DebugPrint() {
	monday := GetMondayHours()
	tuesday := GetTuesdayHours()
	wednesday := GetWednesdayHours()
	thursday := GetThursdayHours()
	friday := GetFridayHours()
	saturday := GetSaturdayHours()
	sunday := GetSundayHours()

	for i := 0; i < 24; i++ {
		println("Hour: ", i)
		println("Monday: ")
		fmt.Printf("%v\n", monday[i])
		println("Tuesday: ")
		fmt.Printf("%v\n", tuesday[i])
		println("Wednesday: ")
		fmt.Printf("%v\n", wednesday[i])
		println("Thursday: ")
		fmt.Printf("%v\n", thursday[i])
		println("Friday: ")
		fmt.Printf("%v\n", friday[i])
		println("Saturday: ")
		fmt.Printf("%v\n", saturday[i])
		println("Sunday: ")
		fmt.Printf("%v\n", sunday[i])
		println()
		print("---------------------------------------------------")
		println()
	}
}

func GetCells() [7][24][4][2]int32 {
	var result [7][24][4][2]int32
	for i := 0; i < 7; i++ {
		result[i] = GetDayHours(i)
	}
	return result
}

func GetSundayHours() [24][4][2]int32 {
	return GetDayHours(0)
}

func GetMondayHours() [24][4][2]int32 {
	return GetDayHours(1)
}

func GetTuesdayHours() [24][4][2]int32 {
	return GetDayHours(2)
}

func GetWednesdayHours() [24][4][2]int32 {
	return GetDayHours(3)
}

func GetThursdayHours() [24][4][2]int32 {
	return GetDayHours(4)
}

func GetFridayHours() [24][4][2]int32 {
	return GetDayHours(5)
}

func GetSaturdayHours() [24][4][2]int32 {
	return GetDayHours(6)
}

// Function that returns a list that containts 24 lists of 4 tuples.package times
// Each of the 24 lists stands for a hour of the day.
// Each of the 4 tuples stands for the corresponding hour on the last 5 days (e.g. mondays).
// The first element of the tuple stands for the first timestamp of the hour and the second element for the last.
// 0 = Sunday, 1 = Monday, 2 = Tuesday, 3 = Wednesday, 4 = Thursday, 5 = Friday, 6 = Saturday
func GetDayHours(targetWeekday int)[24][4][2]int32 {
	if targetWeekday < 0 || targetWeekday > 6 {
		panic("Invalid weekday. Please provide a value between 0 and 6.")
	}

	var result [24][4][2]int32

	location, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		panic(err)
	}
	endDate := time.Date(2023, 10, 20, 0, 0, 0, 0, location)

	// Find the last 4 occurrences of the target weekday
	var weekdays [4]time.Time
	for i := 0; i < 4; i++ {
		weekday := int(endDate.Weekday())
		daysAgo := (weekday + 7 - targetWeekday) % 7 // days ago of the most recent target weekday
		occurrence := endDate.AddDate(0, 0, -daysAgo-7*i)
		weekdays[i] = occurrence
	}

	// Iterate through each hour of the day
	for hour := 0; hour < 24; hour++ {
		// Populate the result with the corresponding hours of the last five occurrences of the target weekday
		for i := 0; i < 4; i++ {
			weekdayHourStart := weekdays[i].Add(time.Duration(hour) * time.Hour)
			weekdayHourEnd := weekdayHourStart.Add(time.Hour)
			result[hour][i] = [2]int32{int32(weekdayHourStart.Unix()), int32(weekdayHourEnd.Unix())}
		}
	}
	return result
}
