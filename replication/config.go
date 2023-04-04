package replication

import (
	"time"

	"go.uber.org/zap"
)

type ReplicationConfig struct {
	Source       configCredentials `json:"source"`
	Target       configCredentials `json:"target"`
	Jobs         []configJob       `json:"jobs"`
	TimeInterval int               `json:"time_interval"` // in minutes
}

type configCredentials struct {
	Address  string `json:"address"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type configJob struct {
	Database   string `json:"database"`
	Continuous bool   `json:"continuous"`
	IDSelector string `json:"id_selector"`
}

func BuildReplicatorFromConfig(logger *zap.Logger, conf *ReplicationConfig) *DBReplicator {
	dbr := &DBReplicator{
		Log:          logger,
		timeInterval: time.Duration(conf.TimeInterval) * time.Minute, //time interval is configured as minutes, but used as seconds
		jobs:         make([]replJob, 0),
		Source: &replDB{
			Address:  conf.Source.Address,
			Username: conf.Source.Username,
			Password: conf.Source.Password,
		},
		Target: &replDB{
			Address:  conf.Target.Address,
			Username: conf.Target.Username,
			Password: conf.Target.Password,
		},
	}

	for _, j := range conf.Jobs {
		dbr.jobs = append(dbr.jobs, replJob(j))
	}

	return dbr
}
