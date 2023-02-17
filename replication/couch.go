package replication

import "encoding/json"

type replicationDocument struct {
	ID           string        `json:"_id"`
	Source       string        `json:"source"`
	Target       replDocTarget `json:"target"`
	CreateTarget bool          `json:"create_target"`
	Continuous   bool          `json:"continuous"`
}

type replDocTarget struct {
	URL  string      `json:"url"`
	Auth replDocAuth `json:"auth"`
}

type replDocAuth struct {
	Basic replDocCredentials `json:"basic"`
}

type replDocCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (dbr *DBReplicator) generateReplicationDocument(job *replJob) (string, error) {
	doc := replicationDocument{
		ID:     "auto_" + job.Database,
		Source: dbr.Source.Address + "/" + job.Database,
		Target: replDocTarget{
			URL: dbr.Target.Address + "/" + job.Database,
			Auth: replDocAuth{
				Basic: replDocCredentials{
					Username: dbr.Target.Username,
					Password: dbr.Target.Password,
				},
			},
		},
		CreateTarget: false,
		Continuous:   job.Continuous,
	}

	b, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// add credentials to db address func (db *replDB)
