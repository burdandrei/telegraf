package resque

import (
	"strings"

	"github.com/go-redis/redis"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"fmt"
)

type Resque struct {
	Redis  string
	client *redis.Client
}

var sampleConfig = `
  ## specify resque redis via a url matching:
  ##  [protocol://][:password]@address[:port]/db
  ##  e.g.
  ##    redis://localhost:6379/1
  ##    redis://:password@192.168.99.100/2
  ##
  ## If no server is specified, then localhost is used as the host.
  ## If no port is specified, 6379 is used
  ## If db is not specified, 0 is used
`

// SampleConfig will populate the sample configuration portion of the plugin's configuration
func (r *Resque) SampleConfig() string {
	return sampleConfig
}

// Description will appear directly above the plugin definition in the config file
func (r *Resque) Description() string {
	return "Read metrics from resque redis servers"
}

// Gather defines what data the plugin will gather.
func (r *Resque) Gather(acc telegraf.Accumulator) error {
	if r.Redis == "" {
		r.Redis = "redis://"
	}

	redisClientOptions, err := redis.ParseURL(r.Redis)
	if err != nil {
		return 	fmt.Errorf("W! [inputs.resque]: %s", err)
	}

	r.client = redis.NewClient(redisClientOptions)

	queues, err := r.client.SMembers("resque:queues").Result()
	if err != nil {
		return err
	}

	queuesInfo := make(map[string]map[string]interface{})

	for _, queueName := range queues {
		queueKey := "resque:queue:" + queueName

		queueBackLog, err := r.client.LLen(queueKey).Result()
		if err != nil {
			return err
		}
		queuesInfo[queueName] = make(map[string]interface{})

		queuesInfo[queueName]["backlog"] = queueBackLog

	}

	workers, err := r.client.SMembers("resque:workers").Result()
	if err != nil {
		return err
	}

	workersPerQueue := make(map[string]int)

	for _, worker := range workers {
		workerQueues := strings.Split(worker, ":")[2]
		workerQueuesList := strings.Split(workerQueues, ",")
		for _, workerQueue := range workerQueuesList {
			// handle workers listening to *
			if strings.Contains(workerQueue, "*") {
				// handle *
				if workerQueue == "*" {
					for _, queueName := range queues {
						workersPerQueue[queueName] = workersPerQueue[queueName] + 1
					}
				} else {
					// handle something*
					for _, queueName := range queues {
						if strings.HasPrefix(queueName, workerQueue) {
							workersPerQueue[queueName] = workersPerQueue[queueName] + 1
						}
					}
				}
			} else {
				workersPerQueue[workerQueue] = workersPerQueue[workerQueue] + 1
			}
		}
	}

	for queueName, workersCount := range workersPerQueue {
		// handle worker without queue  in queues list
		if queuesInfo[queueName] == nil {
			queuesInfo[queueName] = make(map[string]interface{})
			queuesInfo[queueName]["backlog"] = 0
		}
		queuesInfo[queueName]["workers"] = workersCount
	}

	queueInfo := make(map[string]interface{})

	var queueName string

	for queueName, queueInfo = range queuesInfo {

		tags := map[string]string{
			"queue": queueName,
		}

		acc.AddFields("resque", queueInfo, tags)

	}

	return err
}

func init() {
	inputs.Add("resque", func() telegraf.Input {
		return &Resque{}
	})
}
