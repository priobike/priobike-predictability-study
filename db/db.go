package db

import (
	"database/sql"
	"errors"
	"log"
	"os"
	"sink/env"
	"sink/structs"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// The observation database model (for gorm).
type ObservationDB struct {
	// The time when the observation was made (at the site), in milliseconds since epoch.
	PhenomenonTime int32 `gorm:"type:integer;primaryKey"`
	// The time when the observation was processed by the UDP, in milliseconds since epoch.
	ResultTime int64
	// The time when we received the observation, in milliseconds since epoch.
	ReceivedTime int64
	// The result of the observation.
	Result int16 `gorm:"type:smallint"`
	// The datastream id.
	DatastreamID int32 `gorm:"type:integer;primaryKey"`
	// Whether its a mqtt observation or not.
	Mqtt bool `gorm:"type:boolean"`
}

// The model holding the latest phenomenon time for a datastream.
type LatestHttpPhenomenonTime struct {
	// The datastream id.
	DatastreamID int32 `gorm:"type:integer;index:idx_latest_phenomenon_time,unique"`
	// The latest phenomenon time.
	LatestHttpPhenomenonTime int32 `gorm:"type:integer"`
}

var db *gorm.DB
var sqlDb *sql.DB

func Open() {
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			IgnoreRecordNotFoundError: true, // Ignore ErrRecordNotFound error for logger
		},
	)

	// Connect to the database.
	dsn := "host=" + env.PostgresHost + " user=" + env.PostgresUser + " password=" + env.PostgresPassword + " dbname=" + env.PostgresDb + " port=5432 sslmode=disable TimeZone=Europe/Berlin"
	var err error
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: newLogger})
	if err != nil {
		panic("Failed to connect database, error: " + err.Error())
	}
	var err2 error
	sqlDb, err2 = db.DB()
	if err2 != nil {
		panic("Failed to connect database, error: " + err2.Error())
	}

	// SetMaxOpenConns sets the maximum number of open connections to the database (our database allows 100 open connections, we leave some headroom for debugging/data exploration clients).
	sqlDb.SetMaxIdleConns(10)
	sqlDb.SetMaxOpenConns(85)

	// Migration.
	db.AutoMigrate(&ObservationDB{})
	db.AutoMigrate(&LatestHttpPhenomenonTime{})
}

func Close() {
	sqlDb.Close()
}

// Store observations in the db.
func StoreObservationsHttp(observations []structs.Observation, datastreamId int) {
	// Convert the observations to the database models.
	var observationDbs []ObservationDB
	var latestPhenomenonTime int32 = 0

	for _, observation := range observations {
		observationDb := ObservationDB{
			PhenomenonTime: int32(observation.PhenomenonTime.Unix()),
			ResultTime:     observation.ResultTime.UnixMilli(),
			ReceivedTime:   observation.ReceivedTime.UnixMilli(),
			Result:         observation.Result,
			Mqtt:           false,
			DatastreamID:   int32(datastreamId),
		}
		observationDbs = append(observationDbs, observationDb)

		// Update the latest phenomenon time for the datastream.
		if observationDb.PhenomenonTime > latestPhenomenonTime {
			latestPhenomenonTime = observationDb.PhenomenonTime
		}
	}

	db.Table("observation_dbs").Clauses(clause.OnConflict{DoNothing: true}).Create(&observationDbs)

	// Only upsert the latest phenomenon time if it is greater than the current one.
	if latestPhenomenonTime > 0 {
		var latestHttpPhenomenonTime LatestHttpPhenomenonTime = LatestHttpPhenomenonTime{
			DatastreamID:             int32(datastreamId),
			LatestHttpPhenomenonTime: latestPhenomenonTime,
		}
		// Try to create a new row, on conflict (if the datastream id already exists), update the row, but only if the new phenomenon time is greater than the current one.
		db.Table("latest_http_phenomenon_times").Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "datastream_id"}},
			DoUpdates: clause.Assignments(map[string]interface{}{"latest_http_phenomenon_time": gorm.Expr("GREATEST(EXCLUDED.latest_http_phenomenon_time, latest_http_phenomenon_times.latest_http_phenomenon_time)")}),
		}).Create(&latestHttpPhenomenonTime)
	}
}

func StoreObservationMqtt(observations structs.Observation, datastreamId int) {
	// Convert the observation to the database model.
	observationDb := ObservationDB{
		PhenomenonTime: int32(observations.PhenomenonTime.Unix()),
		ResultTime:     observations.ResultTime.UnixMilli(),
		ReceivedTime:   observations.ReceivedTime.UnixMilli(),
		Result:         observations.Result,
		Mqtt:           true,
		DatastreamID:   int32(datastreamId),
	}

	db.Table("observation_dbs").Clauses(clause.OnConflict{DoNothing: true}).Create(&observationDb)
}

func GetLatestPhenomenonTimeForDatastream(datastreamId int) (int32, bool) {
	var latestPhenomenonTime LatestHttpPhenomenonTime
	err := db.Table("latest_http_phenomenon_times").Where("datastream_id = ?", datastreamId).First(&latestPhenomenonTime).Error
	// If no observation was found, return the timestamp 5 minutes ago.
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			panic(err)
		}
		return int32(time.Now().Add(-5 * time.Minute).Unix()), true
	}
	return latestPhenomenonTime.LatestHttpPhenomenonTime, false
}
