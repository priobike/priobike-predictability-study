package env

import "os"

// Load a *required* string environment variable.
// This will panic if the variable is not set.
func loadRequired(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic("Environment variable " + name + " not set.")
	}
	return value
}

var PostgresUser = loadRequired("POSTGRES_USER")
var PostgresPassword = loadRequired("POSTGRES_PASSWORD")
var PostgresDb = loadRequired("POSTGRES_DB")
var PostgresHost = loadRequired("POSTGRES_HOST")
