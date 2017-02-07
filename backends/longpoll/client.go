package longpoll

import (
	"crypto/tls"
	//"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"

	"github.com/kelseyhightower/confd/log"
)

type LongpollResponse struct {
	Error     string `json:"error"`
	Timeout   string `json:"timeout"`
	Timestamp int64  `json:"timestamp"`
	Events    []struct {
		Timestamp int64  `json:"timestamp"`
		Category  string `json:"category"`
		Data      string `json:"data"`
	} `json:"events"`
}

type Client struct {
	sync.Mutex

	transport *http.Transport

	endpoint string
	values   map[string]string
}

func NewLongpollClient(endpoint, caCert string, basicAuth bool, user, pass string) (*Client, error) {
	transport := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	return &Client{transport: transport, endpoint: endpoint, values: make(map[string]string)}, nil
}

func (this Client) update(key, value string) {
	this.Lock()
	defer this.Unlock()

	this.values[key] = value
}

func (this Client) GetValues(keys []string) (map[string]string, error) {
	this.Lock()
	defer this.Unlock()

	var vals = make(map[string]string)

	for _, k := range keys {
		val, ok := this.values[k]
		if !ok {
			return make(map[string]string), errors.New("incomplete set of keys")
		}

		vals[k] = val
	}

	return vals, nil
}

func (this Client) watch(prefix, key string, since uint64, update chan uint64, stop chan bool, ret chan error, wait *sync.WaitGroup) {
	defer wait.Done()

	cli := &http.Client{Transport: this.transport}

	for {
		url := fmt.Sprintf(
			"%s?timeout=10&category=%s&since=%d",
			this.endpoint+prefix, strings.TrimPrefix(key, prefix), since,
		)

		log.Debug("Watching %s", url)

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			ret <- err
			return
		}

		resp, err := cli.Do(req)

		/*
			I don't worry about monitoring the stop channel here since we have a
			small timeout window of 10s for the longpoll. Once a timeout occurs,
			we check the stop channel below to see if we need to reconnect or
			just exit. Any other type of response from the longpoll server
			causes us to return anyway.
		*/

		if err != nil {
			ret <- err
			return
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			ret <- err
			return
		}

		resp.Body.Close()

		var response = LongpollResponse{}
		json.Unmarshal(body, &response)

		// TODO: check response code for direction

		if response.Error != "" {
			ret <- errors.New(response.Error)
			return
		} else if response.Timeout != "" {
			select {
			case <-stop:
				log.Debug("Stop signal received. Exiting.")
				return
			default:
				log.Debug("Timeout reached in longpoll watcher. Reconnecting.")

				since = uint64(response.Timestamp)
				continue
			}
		} else if len(response.Events) > 0 {
			for _, e := range response.Events {
				this.update(prefix+e.Category, e.Data)
			}

			update <- uint64(response.Timestamp)
			ret <- nil
			return
		} else {
			ret <- errors.New("unexpected state returned")
			return
		}
	}
}

func (this Client) WatchPrefix(prefix string, keys []string, waitIndex uint64, stopChan chan bool) (uint64, error) {
	log.Debug("New longpoll watch for prefix %s and keys %#v", prefix, keys)

	var wait sync.WaitGroup

	var (
		stoppers []chan bool
		updates  = make(chan uint64, len(keys))
		returns  = make(chan error, len(keys))
	)

	for _, k := range keys {
		stop := make(chan bool, 1)
		stoppers = append(stoppers, stop)

		wait.Add(1)
		go this.watch(prefix, k, waitIndex, updates, stop, returns, &wait)
	}

	var update uint64

	go func() {
		for {
			u := <-updates

			this.Lock()

			if u > update {
				update = u
			}

			this.Unlock()
		}
	}()

	err := <-returns

	for _, s := range stoppers {
		s <- true
	}

	wait.Wait()

	if err != nil {
		return 0, err
	}

	return update, nil
}
