package replication

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type DBReplicator struct {
	Log          *zap.Logger
	Source       *replDB
	Target       *replDB
	timeInterval time.Duration
	jobs         []replJob
}

type replDB struct {
	Address  string
	Username string
	Password string
}

type replJob struct {
	Database   string
	Continuous bool
	IDSelector string
}

func (dbr *DBReplicator) ManualReplicationHandler(c *gin.Context) {
	dbr.Log.Debug("manual replication endpoint called")

	go dbr.runFullReplication()

	c.JSON(http.StatusOK, "replication started")
}

func (dbr *DBReplicator) StartReplication(status chan error) {
	dbr.Log.Info("starting replication cycle", zap.Float64("Time Interval (min)", dbr.timeInterval.Minutes()))
	sourceReachable, targetReachable, err := false, false, error(nil)

	dbr.Log.Info("checking database accessibility...")
	for !sourceReachable && !targetReachable {
		if !sourceReachable {
			sourceReachable, err = dbr.Source.CheckConnection()
			if err != nil {
				dbr.Log.Error("waiting for source database to start...", zap.Error(err))
			}
		}
		if !targetReachable {
			targetReachable, err = dbr.Target.CheckConnection()
			if err != nil {
				dbr.Log.Error("waiting for target database to start...", zap.Error(err))
			}
		}
	}

	status <- dbr.runReplicationIntervalLoop()
}

func (dbr *DBReplicator) runReplicationIntervalLoop() error {
	for {

		dbr.runFullReplication()

		dbr.Log.Debug("waiting", zap.Float64("Time Interval (min)", dbr.timeInterval.Minutes()))
		time.Sleep(dbr.timeInterval)
	}

	return fmt.Errorf("replication jobs failed")
}

func (dbr *DBReplicator) runFullReplication() error {
	replFailure := false
	for _, job := range dbr.jobs {
		err := dbr.doReplication(&job)
		if err != nil {
			dbr.Log.Error("database replication failed to start", zap.String("database", job.Database), zap.Error(err))
			replFailure = true
		}
	}
	if replFailure {
		return fmt.Errorf("failure to start all database replications")
	}
	return nil
}

func (dbr *DBReplicator) doReplication(job *replJob) error {
	dbr.Log.Debug("running replication", zap.String("database", job.Database))

	status, err := dbr.checkReplication(job)
	if err != nil {
		return err
	}

	if status == "running" || status == "started" || status == "added" {
		dbr.Log.Debug("replication already running", zap.String("database", job.Database), zap.String("status", status))
		return nil
	}

	conflict, err := dbr.postReplication(job)
	for conflict {
		//delete and repost
		err = dbr.deleteReplication(job)
		if err != nil {
			return err
		}

		conflict, err = dbr.postReplication(job)
	}

	return err
}

func (db *replDB) CheckConnection() (bool, error) {
	resp, err := http.Get(db.Address)
	if err != nil {
		return false, fmt.Errorf("cannot reach database; %s", err.Error())
	}
	resp.Body.Close()

	return true, nil
}
