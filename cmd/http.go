package main

import (
	"fmt"
	"net/http"

	"github.com/byuoitav/db-replicator/replication"
	"github.com/gin-gonic/gin"
)

func runHttpServer(status chan error, port string, replicator *replication.DBReplicator) {
	router := gin.Default()

	apiRoute := router.Group("")
	apiRoute.GET("/replication/start", replicator.ManualReplicationHandler)

	server := &http.Server{
		Addr:           port,
		MaxHeaderBytes: 1021 * 10,
	}

	replicator.Log.Info("starting http server")
	err := router.Run(server.Addr)

	status <- fmt.Errorf(err.Error())
}
