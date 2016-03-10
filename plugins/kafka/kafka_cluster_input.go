/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014-2015
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Mike Trinkala (trink@mozilla.com)
#   Rob Miller (rmiller@mozilla.com)
#   Matt Moyer (moyer@simple.com)
#
# ***** END LICENSE BLOCK *****/

package kafka

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/mozilla-services/heka/plugins/tcp"
	sarama "gopkg.in/Shopify/sarama.v1"
)

type KafkaClusterInputConfig struct {
	Splitter string

	// Client Config
	Id                         string
	Addrs                      []string
	MetadataRetries            int    `toml:"metadata_retries"`
	WaitForElection            uint32 `toml:"wait_for_election"`
	BackgroundRefreshFrequency uint32 `toml:"background_refresh_frequency"`

	// TLS Config
	UseTls bool `toml:"use_tls"`
	Tls    tcp.TlsConfig

	// Broker Config
	MaxOpenRequests int    `toml:"max_open_reqests"`
	DialTimeout     uint32 `toml:"dial_timeout"`
	ReadTimeout     uint32 `toml:"read_timeout"`
	WriteTimeout    uint32 `toml:"write_timeout"`

	// Consumer Config
	Topic             string
	Partition         int32
	PartitionStrategy string `toml:"partition_strategy"` // range, round_robin
	Group             string `toml:"group"`
	DefaultFetchSize  int32  `toml:"default_fetch_size"`
	MinFetchSize      int32  `toml:"min_fetch_size"`
	MaxMessageSize    int32  `toml:"max_message_size"`
	MaxWaitTime       uint32 `toml:"max_wait_time"`
	EventBufferSize   int    `toml:"event_buffer_size"`
}

type KafkaClusterInput struct {
	processMessageCount    int64
	processMessageFailures int64

	config        *KafkaClusterInputConfig
	clusterConfig *cluster.Config
	consumer      *cluster.Consumer
	//partitionConsumer  sarama.PartitionConsumer
	pConfig *pipeline.PipelineConfig
	ir      pipeline.InputRunner
	//checkpointFile     *os.File
	stopChan chan bool
	name     string
	//checkpointFilename string
}

func (k *KafkaClusterInput) ConfigStruct() interface{} {
	hn := k.pConfig.Hostname()
	return &KafkaClusterInputConfig{
		Splitter:                   "NullSplitter",
		Id:                         hn,
		MetadataRetries:            3,
		WaitForElection:            250,
		BackgroundRefreshFrequency: 10 * 60 * 1000,
		MaxOpenRequests:            4,
		DialTimeout:                60 * 1000,
		ReadTimeout:                60 * 1000,
		WriteTimeout:               60 * 1000,
		DefaultFetchSize:           1024 * 32,
		MinFetchSize:               1,
		MaxWaitTime:                250,
		EventBufferSize:            16,
		PartitionStrategy:          "range",
	}
}

func (k *KafkaClusterInput) SetPipelineConfig(pConfig *pipeline.PipelineConfig) {
	k.pConfig = pConfig
}

func (k *KafkaClusterInput) SetName(name string) {
	k.name = name
}

func (k *KafkaClusterInput) Init(config interface{}) (err error) {
	k.config = config.(*KafkaClusterInputConfig)
	if len(k.config.Addrs) == 0 {
		return errors.New("addrs must have at least one entry")
	}
	if len(k.config.Group) == 0 {
		k.config.Group = k.config.Id
	}

	k.clusterConfig = cluster.NewConfig()
	k.clusterConfig.ClientID = k.config.Id
	k.clusterConfig.Metadata.Retry.Max = k.config.MetadataRetries
	k.clusterConfig.Metadata.Retry.Backoff = time.Duration(k.config.WaitForElection) * time.Millisecond
	k.clusterConfig.Metadata.RefreshFrequency = time.Duration(k.config.BackgroundRefreshFrequency) * time.Millisecond

	k.clusterConfig.Net.TLS.Enable = k.config.UseTls
	if k.config.UseTls {
		if k.clusterConfig.Net.TLS.Config, err = tcp.CreateGoTlsConfig(&k.config.Tls); err != nil {
			return fmt.Errorf("TLS init error: %s", err)
		}
	}

	k.clusterConfig.Net.MaxOpenRequests = k.config.MaxOpenRequests
	k.clusterConfig.Net.DialTimeout = time.Duration(k.config.DialTimeout) * time.Millisecond
	k.clusterConfig.Net.ReadTimeout = time.Duration(k.config.ReadTimeout) * time.Millisecond
	k.clusterConfig.Net.WriteTimeout = time.Duration(k.config.WriteTimeout) * time.Millisecond

	k.clusterConfig.Consumer.Fetch.Default = k.config.DefaultFetchSize
	k.clusterConfig.Consumer.Fetch.Min = k.config.MinFetchSize
	k.clusterConfig.Consumer.Fetch.Max = k.config.MaxMessageSize
	k.clusterConfig.Consumer.MaxWaitTime = time.Duration(k.config.MaxWaitTime) * time.Millisecond

	k.clusterConfig.ChannelBufferSize = k.config.EventBufferSize

	switch k.config.PartitionStrategy {
	case "round_robin":
		k.clusterConfig.Group.PartitionStrategy = cluster.StrategyRoundRobin
	default:
		k.clusterConfig.Group.PartitionStrategy = cluster.StrategyRange
	}

	k.consumer, err = cluster.NewConsumer(k.config.Addrs, k.config.Group, []string{k.config.Topic}, k.clusterConfig)
	return err
}

func (k *KafkaClusterInput) addField(pack *pipeline.PipelinePack, name string,
	value interface{}, representation string) {

	if field, err := message.NewField(name, value, representation); err == nil {
		pack.Message.AddField(field)
	} else {
		k.ir.LogError(fmt.Errorf("can't add '%s' field: %s", name, err.Error()))
	}
}

func (k *KafkaClusterInput) Run(ir pipeline.InputRunner, h pipeline.PluginHelper) (err error) {
	sRunner := ir.NewSplitterRunner("")

	defer func() {
		k.consumer.Close()
		sRunner.Done()
	}()
	k.ir = ir
	k.stopChan = make(chan bool)

	var (
		hostname = k.pConfig.Hostname()
		event    *sarama.ConsumerMessage
		cError   error
		ok       bool
		n        int
	)

	packDec := func(pack *pipeline.PipelinePack) {
		pack.Message.SetType("heka.kafka")
		pack.Message.SetLogger(k.name)
		pack.Message.SetHostname(hostname)
		k.addField(pack, "Key", event.Key, "")
		k.addField(pack, "Topic", event.Topic, "")
		k.addField(pack, "Partition", event.Partition, "")
		k.addField(pack, "Offset", event.Offset, "")
	}
	if !sRunner.UseMsgBytes() {
		sRunner.SetPackDecorator(packDec)
	}

	eventChan := k.consumer.Messages()
	cErrChan := k.consumer.Errors()
	for {
		select {
		case event, ok = <-eventChan:
			if !ok {
				return nil
			}
			atomic.AddInt64(&k.processMessageCount, 1)
			if n, err = sRunner.SplitBytes(event.Value, nil); err != nil {
				ir.LogError(fmt.Errorf("processing message from topic %s: %s",
					event.Topic, err))
			}
			if n > 0 && n != len(event.Value) {
				ir.LogError(fmt.Errorf("extra data dropped in message from topic %s",
					event.Topic))
			}

		case cError, ok = <-cErrChan:
			if !ok {
				// Don't exit until the eventChan is closed.
				ok = true
				continue
			}
			atomic.AddInt64(&k.processMessageFailures, 1)
			ir.LogError(cError)

		case <-k.stopChan:
			return nil
		}
	}
}

func (k *KafkaClusterInput) Stop() {
	close(k.stopChan)
}

func (k *KafkaClusterInput) ReportMsg(msg *message.Message) error {
	message.NewInt64Field(msg, "ProcessMessageCount",
		atomic.LoadInt64(&k.processMessageCount), "count")
	message.NewInt64Field(msg, "ProcessMessageFailures",
		atomic.LoadInt64(&k.processMessageFailures), "count")
	return nil
}

func (k *KafkaClusterInput) CleanupForRestart() {
	return
}

func init() {
	pipeline.RegisterPlugin("KafkaClusterInput", func() interface{} {
		return new(KafkaClusterInput)
	})
}
