package main

import (
	"github.com/byuoitav/db-replicator/replication"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

func main() {
	var logLevel, port string
	pflag.StringVarP(&port, "port", "p", "7012", "port on which to run the http server")
	pflag.StringVarP(&logLevel, "log", "l", "Info", "Initial log level")
	pflag.Parse()

	port = ":" + port
	log := buildLogger(logLevel)

	replicator := &replication.DBReplicator{
		Log: log,
	}

	httpStatus := make(chan error)
	go runHttpServer(httpStatus, port, replicator)

	replicatorStatus := make(chan error)

	select {
	case s := <-httpStatus:
		log.Fatal("http server failure", zap.Error(s))
	case s := <-replicatorStatus:
		log.Fatal("replicator failure", zap.Error(s))
	}
}
