package main

import (
	"sink/db"
	"sink/observations"
	"sink/things"
)

func main() {
	// Open the database.
	db.Open()
	defer db.Close()

	// Sync the things.
	things.SyncThings()
	// Start the routing for fetching the observations via HTPP.
	go observations.FetchObservationsDb()
	// Connect to the mqtt broker and listen for observations.
	observations.ConnectObservationListener()
	// Check periodically how many messages were received.
	go observations.CheckReceivedMessagesPeriodically()

	// Wait forever.
	select {}
}
