package resque

import (
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/go-redis/redis"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type Resque struct {
	ResqueRedis string

	client      *RedisClient
	initialized bool
}

type RedisClient struct {
	client *redis.Client
	tags   map[string]string
}


func (r *RedisClient) BaseTags() map[string]string {
	tags := make(map[string]string)
	for k, v := range r.tags {
		tags[k] = v
	}
	return tags
}

var sampleConfig = `
  ## specify resque redis via a url matching:
  ##  [protocol://][:password]@address[:port]/db
  ##  e.g.
  ##    tcp://localhost:6379/1
  ##    tcp://:password@192.168.99.100/2
  ##    unix:///var/run/redis.sock/3
  ##
  ## If no server is specified, then localhost is used as the host.
  ## If no port is specified, 6379 is used
  ## If db is not specified, 1 is used
  resque_redis = "tcp://localhost:6379/1"]
`

func (r *Resque) SampleConfig() string {
	return sampleConfig
}

func (r *Resque) Description() string {
	return "Read metrics from resque redis servers"
}

var Tracking = map[string]string{
	"backlog": "backlog",
}

func (r *Resque) init(acc telegraf.Accumulator) error {
	if r.initialized {
		return nil
	}

	if r.ResqueRedis == "" {
		r.ResqueRedis = "tcp://localhost:6379/1"
	}

	if !strings.HasPrefix(r.ResqueRedis, "tcp://") && !strings.HasPrefix(r.ResqueRedis, "unix://") {
		log.Printf("W! [inputs.resque]: server URL found without scheme; please update your configuration file")
		r.ResqueRedis = "tcp://" + r.ResqueRedis
	}

	u, err := url.Parse(r.ResqueRedis)
	if err != nil {
		return fmt.Errorf("Unable to parse to address %q: %v", r.Server, err)
	}

	password := ""
	if u.User != nil {
		pw, ok := u.User.Password()
		if ok {
			password = pw
		}
	}

	var address string
	if u.Scheme == "unix" {
		address = u.Path
	} else {
		address = u.Host
	}

	var db int
	if u.Path != "" {
		db, err = strconv.Atoi(u.Path)
		if err != nil {
			log.Printf("W! [inputs.resque]: redis db is not a number; please update your configuration file")
		}
	} else {
		db = 1
	}

	client := redis.NewClient(
		&redis.Options{
			Addr:     address,
			Password: password,
			Network:  u.Scheme,
			PoolSize: 1,
			DB:       db,
		},
	)

	tags := map[string]string{}
	if u.Scheme == "unix" {
		tags["socket"] = u.Path
	} else {
		tags["server"] = u.Hostname()
		tags["port"] = u.Port()
	}


	r.client= &RedisClient{
		client: client,
		tags:   tags,
	}


	r.initialized = true
	return nil
}

// Reads stats from configured resque redis accumulates stats.
// Returns one of the errors encountered while gather stats (if any).
func (r *Resque) Gather(acc telegraf.Accumulator) error {
	if !r.initialized {
		err := r.init(acc)
		if err != nil {
			return err
		}
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		acc.AddError(r.gatherBacklog(acc))
	}()

	wg.Wait()
	return nil
}

func (r *Resque) gatherBacklog(acc telegraf.Accumulator) error {
	queues, err := r.client.client.SMembers("resque:queues").Result()
	if err != nil {
		return err
	}

	for _, queuename := range queues {
		queueKey := "resque:queue:" + queuename
		fmt.Println(queueKey)
		queueBackLog, err := r.client.client.LLen(queueKey).Result()
		if err != nil {
			return err
		}

		fields := map[string]interface{}{
			"backlog": queueBackLog,
		}
		r.client.tags["queue"] = queuename

		acc.AddFields("resque_backlog",fields, r.client.tags)

		fmt.Printf("%s  - %d", queuename, queueBackLog)
	}

	return err
}

func init() {
	inputs.Add("resque", func() telegraf.Input {
		return &Resque{}
	})
}
