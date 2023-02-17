package main

import (
	"github.com/byuoitav/db-replicator/replication"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

func main() {
	var logLevel, port, configPath string
	pflag.StringVarP(&port, "port", "p", "7012", "Port on which to run the http server")
	pflag.StringVarP(&logLevel, "log", "l", "Info", "Initial log level")
	pflag.StringVarP(&configPath, "config", "c", "", "File path to replication config file")
	pflag.Parse()

	port = ":" + port
	log := buildLogger(logLevel)
	config, err := readConfig(configPath)
	if err != nil {
		log.Fatal("cannot read config", zap.Error(err))
	}

	log.Info("building db replicator from config")
	replicator := replication.BuildReplicatorFromConfig(log, config)

	httpStatus := make(chan error)
	go runHttpServer(httpStatus, port, replicator)

	replicatorStatus := make(chan error)
	go replicator.StartReplication(replicatorStatus)

	select {
	case s := <-httpStatus:
		log.Fatal("http server failure", zap.Error(s))
	case s := <-replicatorStatus:
		log.Fatal("replicator failure", zap.Error(s))
	}
}
