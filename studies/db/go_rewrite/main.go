package main

import (
	"studies/things"
)

func main() {
	// runner.Run()
	// times.DebugPrint()
	//times.DebugPrint()
	// Wait forever.
	// select {}

	tp := things.NewThingsProvider(false)
	tp.FilterOnlyPrimarySignalSecondarySignalAndCycleSecondDatastreams()
	tldThings := tp.Things
	println("Processing", len(tldThings), "things")

}
