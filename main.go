package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	yaml "gopkg.in/yaml.v3"
)

var mqttMetric = prometheus.NewDesc(prometheus.BuildFQName("mqtt", "", "data"), "mqtt published metric", []string{"topic"}, nil)

type exporterConfig struct {
	Server   string
	ClientID string
	Listen   string
	Timeout  int
	QoS      int
	Topics   []string
}

type mqttData struct {
	topic     string
	value     float64
	timestamp time.Time
}

type mqttExporter struct {
	c       mqtt.Client
	timeout time.Duration
	data    map[string]mqttData
	m       sync.Mutex
}

func (m *mqttExporter) subscribe(topics []string, qos int) {
	for _, t := range topics {
		token := m.c.Subscribe(t, byte(qos), nil)
		token.Wait()
		log.Printf("Subscribed to topic %s", t)
	}
}

func (m *mqttExporter) messagePubHandler(client mqtt.Client, msg mqtt.Message) {
	raw := string(msg.Payload())
	topic := string(msg.Topic())
	log.Printf("%s: %s", topic, raw)
	pValue, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		log.Printf("can't convert %s to float64", raw)
		return
	}
	insert := mqttData{
		topic:     topic,
		value:     pValue,
		timestamp: time.Now(),
	}

	m.m.Lock()
	defer m.m.Unlock()
	m.data[topic] = insert
}

func (m *mqttExporter) timeoutData() {
	for {
		time.Sleep(m.timeout)
		log.Printf("run timeoutData()")
		m.m.Lock()

		newData := make(map[string]mqttData)
		for k, v := range m.data {
			if v.timestamp.Add(m.timeout).Before(time.Now()) {
				continue
			}
			newData[k] = v
		}
		m.data = newData
		m.m.Unlock()
	}
}

func (m *mqttExporter) connectHandler(client mqtt.Client) {
	log.Printf("connected")
}

func (m *mqttExporter) connectionLostHandler(client mqtt.Client, err error) {
	log.Fatalf("connection lost: %v", err)
}

func (m *mqttExporter) Describe(dch chan<- *prometheus.Desc) {
	dch <- mqttMetric
}

func (m *mqttExporter) Collect(mch chan<- prometheus.Metric) {
	m.m.Lock()
	defer m.m.Unlock()
	for k, v := range m.data {
		mch <- prometheus.MustNewConstMetric(mqttMetric, prometheus.GaugeValue, v.value, k)
	}
}

func main() {
	config := &exporterConfig{
		Listen:   ":2112",
		ClientID: fmt.Sprintf("lymex-%d", os.Getpid()),
		Timeout:  30,
		QoS:      1,
	}

	configData, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("can't read config file: %v", err)
	}

	if err = yaml.Unmarshal(configData, config); err != nil {
		log.Fatalf("can't parse config file: %v", err)
	}

	ex := &mqttExporter{
		data:    map[string]mqttData{},
		timeout: time.Second * time.Duration(config.Timeout),
	}

	go ex.timeoutData()

	opts := mqtt.NewClientOptions()
	opts.AddBroker(config.Server)
	opts.SetClientID(config.ClientID)
	opts.SetDefaultPublishHandler(ex.messagePubHandler)
	opts.OnConnect = ex.connectHandler
	opts.OnConnectionLost = ex.connectionLostHandler
	ex.c = mqtt.NewClient(opts)
	if token := ex.c.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("can't connect: %v", token.Error())
	}
	ex.subscribe(config.Topics, config.QoS)
	prometheus.DefaultRegisterer.MustRegister(ex)

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	log.Printf("listening on %s", config.Listen)
	http.ListenAndServe(config.Listen, nil)
}
