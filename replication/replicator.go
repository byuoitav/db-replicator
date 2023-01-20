package replication

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type DBReplicator struct {
	Log *zap.Logger
}

func (dbr *DBReplicator) ManualReplicationHandler(c *gin.Context) {
	c.JSON(http.StatusOK, "ok")
}
