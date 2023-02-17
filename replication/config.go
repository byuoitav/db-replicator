package replication

import "go.uber.org/zap"

type ReplicationConfig struct {
	Source       configCredentials `json:"source"`
	Target       configCredentials `json:"target"`
	Jobs         []configJob       `json:"jobs"`
	TimeInterval int               `json:"timeInterval"` // in minutes
}

type configCredentials struct {
	Address  string `json:"address"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type configJob struct {
	Database   string `json:"database"`
	Continuous bool   `json:"continuous"`
}

func BuildReplicatorFromConfig(logger *zap.Logger, conf *ReplicationConfig) *DBReplicator {
	dbr := &DBReplicator{
		Log:          logger,
		timeInterval: conf.TimeInterval * 60, //time interval is configured as minutes, but used as seconds
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
