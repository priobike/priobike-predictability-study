package csv

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"sink/env"
	"sink/structs"
	"sync"
	"time"
)

// The locks for the sink files.
var sinkFileLocks = sync.Map{}

// The existing file paths and their last update time.
// The cache is used to speedup the creation of the index.
var filePaths = sync.Map{}

// Store an observation in a file.
func StoreObservation(observation structs.Observation, layerName string, thingName string, mqttObservation bool) bool {
	protocol := "http"

	if mqttObservation {
		protocol = "mqtt"
	}

	fileName := fmt.Sprintf("%s-%s.csv", thingName, layerName)

	// Check if directory for thing exists
	directory_path := fmt.Sprintf("%s/sink/%s/", env.StaticPath, thingName)
	_, err := os.Stat(directory_path)
	if os.IsNotExist(err) {
		panic("Directory for thing does not exist: " + directory_path)
	}

	// Write to file and create if not exists
	var fileFlag int
	if mqttObservation {
		fileFlag = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	} else {
		fileFlag = os.O_APPEND | os.O_CREATE | os.O_RDWR
	}
	filePath := fmt.Sprintf("%s/sink/%s/%s", env.StaticPath, thingName, fileName)
	lock, _ := sinkFileLocks.LoadOrStore(filePath, &sync.Mutex{})
	lock.(*sync.Mutex).Lock()
	file, openErr := os.OpenFile(filePath, fileFlag, 0666)
	if openErr != nil {
		panic("Could not open file " + filePath + " - error: " + openErr.Error())
	}

	// Check number of lines in file
	fileInfo, statErr := file.Stat()
	if statErr != nil {
		panic("Could not stat file " + filePath + " - error: " + openErr.Error())
	}

	// If file is empty, write header
	csvHeader := "phenomenonTime,resultTime,receivedTime,result,source"
	if fileInfo.Size() == 0 {
		if _, writeErr := file.WriteString(csvHeader + "\n"); writeErr != nil {
			panic("Could not write header to file " + filePath + " - error: " + writeErr.Error())
		}
	}

	phenonemonTime := observation.PhenomenonTime.Format(time.RFC3339Nano)
	// Add a unique string to the phenomenon time to avoid duplicates with resultTimes or receivedTimes.
	phenonemonTimeUniqueString := fmt.Sprintf("%s%s", phenonemonTime, "delMe")

	// If the file size is larger than 50kb, rename the file by appending the current timestamp and create new one.
	// Otherwise, the file will get too large and thus it takes longer to read it and check for duplicates.
	if fileInfo.Size() > 50000 {
		file.Close()
		// Rename the file.
		newFilePath := fmt.Sprintf("%s/sink/%s/%s-%s.csv", env.StaticPath, thingName, fileName, time.Now().Format("2006-01-02T15:04:05Z"))
		if err := os.Rename(filePath, newFilePath); err != nil {
			panic("Could not rename file " + filePath + " - error: " + err.Error())
		}
		// Create a new file.
		file, openErr = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
		if openErr != nil {
			panic("Could not open file " + filePath + " - error: " + openErr.Error())
		}
		if _, writeErr := file.WriteString(csvHeader + "\n"); writeErr != nil {
			panic("Could not write header to file " + filePath + " - error: " + writeErr.Error())
		}
	} else if !mqttObservation {
		// Check for duplicates if it is a http observation.
		// If there is already an observation with the same phenomenon time, we don't need to add the observation (again).

		// Reading the data is copied from os.ReadFile() method:
		var size int
		size64 := fileInfo.Size()
		if int64(int(size64)) == size64 {
			size = int(size64)
		}
		size++ // one byte for final read at EOF

		// If a file claims a small size, read at least 512 bytes.
		// In particular, files in Linux's /proc claim size 0 but
		// then do not work right if read in small pieces,
		// so an initial read of 1 byte would not work correctly.
		if size < 512 {
			size = 512
		}

		data := make([]byte, 0, size)
		for {
			if len(data) >= cap(data) {
				d := append(data[:cap(data)], 0)
				data = d[:len(data)]
			}
			n, err := file.Read(data[len(data):cap(data)])
			data = data[:len(data)+n]
			if err != nil {
				if err == io.EOF {
					break
				} else {
					panic("Could not read file " + filePath + " - error: " + err.Error())
				}
			}
		}

		var regex, regError = regexp.Compile(phenonemonTimeUniqueString)
		if regError != nil {
			panic("Could not compile regex - error: " + regError.Error())
		}
		if regex.Match(data) {
			lock.(*sync.Mutex).Unlock()
			file.Close()
			return true
		}
	}

	// Write observation to file
	csvRow := fmt.Sprintf("%s,%s,%s,%d,%s", phenonemonTimeUniqueString, observation.ResultTime.Format(time.RFC3339Nano), observation.ReceivedTime.Format(time.RFC3339Nano), observation.Result, protocol)
	if _, writeErr := file.WriteString(csvRow + "\n"); writeErr != nil {
		panic("Could not write observation to file " + filePath + " - error: " + writeErr.Error())
	}

	file.Close()
	lock.(*sync.Mutex).Unlock()

	currentTime := time.Now()
	filePaths.Store(filePath, currentTime)

	return true
}
