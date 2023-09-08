package observations

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"regexp"
	"sink/env"
	"sink/log"
	"sink/things"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// The QoS for observations. We use QoS 1 (at least once).
// We also don't use QoS 2 since the messages could be delayed if the
// broker is overloaded or the upstream connection is slow. Note
// that this implies that we might receive the same observation twice.
const observationQoS = 1

// Received messages by their topic.
var ObservationsReceivedByTopic = make(map[string]uint64)

// The lock for the map.
var ObservationsReceivedByTopicLock = &sync.RWMutex{}

// The existing file paths and their last update time.
// The cache is used to speedup the creation of the index.
var filePaths = sync.Map{}

// The number of processed messages, for logging purposes.
var ObservationsReceived uint64 = 0
var ObservationsDiscarded uint64 = 0
var ObservationsProcessed uint64 = 0

// The locks for the sink files.
var sinkFileLocks = sync.Map{}

// Check out the number of received messages periodically.
func CheckReceivedMessagesPeriodically() {
	for {
		receivedNow := ObservationsReceived
		canceledNow := ObservationsDiscarded
		processedNow := ObservationsProcessed
		time.Sleep(120 * time.Second)
		receivedThen := ObservationsReceived
		canceledThen := ObservationsDiscarded
		processedThen := ObservationsProcessed
		dReceived := receivedThen - receivedNow
		dCanceled := canceledThen - canceledNow
		dProcessed := processedThen - processedNow
		// Panic if the number of received messages is too low.
		if dReceived == 0 {
			panic("No messages received in the last 120 seconds")
		}
		log.Info.Printf("Received %d observations in the last 120 seconds. (%d processed, %d canceled)", dReceived, dProcessed, dCanceled)
		ObservationsReceivedByTopicLock.RLock()
		for dsType, count := range ObservationsReceivedByTopic {
			log.Info.Printf("  - Received %d observations for `%s`.", count, dsType)
		}
		ObservationsReceivedByTopicLock.RUnlock()
	}
}

// Process a message.
func processMessage(msg mqtt.Message) {
	atomic.AddUint64(&ObservationsReceived, 1)

	// Add the observation to the correct map.
	topic := msg.Topic()

	// Check if the topic should be processed.
	dsType, ok := things.DatastreamMqttTopics.Load(topic)
	if !ok {
		atomic.AddUint64(&ObservationsDiscarded, 1)
		return
	}

	// Increment the number of received messages.
	ObservationsReceivedByTopicLock.Lock()
	ObservationsReceivedByTopic[dsType.(string)]++
	ObservationsReceivedByTopicLock.Unlock()

	var observation Observation
	if err := json.Unmarshal(msg.Payload(), &observation); err != nil {
		atomic.AddUint64(&ObservationsDiscarded, 1)
		return
	}

	thingName, ok := things.Datastreams.Load(topic)
	if !ok {
		atomic.AddUint64(&ObservationsDiscarded, 1)
		return
	}

	success := storeObservation(observation, dsType.(string), thingName.(string), true)
	if !success {
		atomic.AddUint64(&ObservationsDiscarded, 1)
		log.Warning.Println("Could not store observation")
		return
	}

	atomic.AddUint64(&ObservationsProcessed, 1)
}

// Store an observation in a file.
func storeObservation(observation Observation, layerName string, thingName string, mqttObservation bool) bool {
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
	filePath := fmt.Sprintf("%s/sink/%s/%s", env.StaticPath, thingName, fileName)
	lock, _ := sinkFileLocks.LoadOrStore(filePath, &sync.Mutex{})
	lock.(*sync.Mutex).Lock()
	file, openErr := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
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

// Listen for new observations via mqtt.
func ConnectObservationListener() {
	topics := []string{}
	things.DatastreamMqttTopics.Range(func(topic, _ interface{}) bool {
		topics = append(topics, topic.(string))
		return true
	})

	log.Info.Println("Topic count:", len(topics))

	// Create a new client for every 1000 subscriptions.
	// Otherwise messages will queue up after some time, since the client
	// is not parallelized enough. This is a workaround for the issue.
	// Bonus points: this also reduces CPU usage significantly.
	var client mqtt.Client
	var wg sync.WaitGroup
	for i, topic := range topics {
		if (i % 10000) == 0 {
			wg.Wait()
			opts := mqtt.NewClientOptions()
			opts.AddBroker(env.SensorThingsObservationMqttUrl)
			opts.SetConnectTimeout(10 * time.Second)
			opts.SetConnectRetry(true)
			opts.SetConnectRetryInterval(5 * time.Second)
			opts.SetAutoReconnect(true)
			opts.SetKeepAlive(60 * time.Second)
			opts.SetPingTimeout(10 * time.Second)
			opts.SetOnConnectHandler(func(client mqtt.Client) {
				log.Info.Printf(
					"Connected to observation mqtt broker: %s",
					env.SensorThingsObservationMqttUrl,
				)
			})
			opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
				log.Warning.Println("Connection to observation mqtt broker lost:", err)
			})
			randSource := rand.NewSource(time.Now().UnixNano())
			random := rand.New(randSource)
			clientID := fmt.Sprintf("priobike-sink-%d", random.Int())
			opts.SetClientID(clientID)
			opts.SetOrderMatters(false)
			opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
				log.Warning.Println("Received unexpected message on topic:", msg.Topic())
			})
			client = mqtt.NewClient(opts)
			if conn := client.Connect(); conn.Wait() && conn.Error() != nil {
				panic(conn.Error())
			}
		}

		wg.Add(1)
		// Wait 40ms between each subscription to avoid overloading the mqtt broker.
		time.Sleep(40 * time.Millisecond)
		go func(topic string) {
			defer wg.Done()

			// Subscribe to the datastream.
			if token := client.Subscribe(topic, observationQoS, func(client mqtt.Client, msg mqtt.Message) {
				// Process the message asynchronously to avoid blocking the mqtt client.
				go processMessage(msg)
			}); token.Wait() && token.Error() != nil {
				panic(token.Error())
			}
		}(topic)
	}

	log.Info.Println("Subscribed to all datastreams.")
}
