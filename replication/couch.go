package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"go.uber.org/zap"
)

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
	Credentials replDocCredentials `json:"basic"`
}

type replDocCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type errorResponse struct {
	Error  string `json:"error"`
	Reason string `json:"reason"`
}

func (dbr *DBReplicator) checkReplication(job *replJob) (string, error) {
	dbr.Log.Debug("checking replication status", zap.String("database", job.Database))

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%v/_scheduler/docs/_replicator/%v", dbr.Source.Address, job.Database), nil)
	if err != nil {
		dbr.Log.Debug("failed to build request to scheduler", zap.String("database", job.Database))
		return "", fmt.Errorf("failed to create http GET request | %v", err.Error())
	}

	req.SetBasicAuth(dbr.Source.Username, dbr.Source.Password)
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("%v", err.Error())
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		dbr.Log.Debug("failed to read response body from status request", zap.String("database", job.Database), zap.Error(err))
		return "", fmt.Errorf("could not read body on http response | %v", err.Error())
	}

	if resp.StatusCode/100 == 2 {

		return "", nil
	}

	var rError errorResponse
	err = json.Unmarshal(body, &rError)
	if err != nil {
		dbr.Log.Debug("failed to unmarshal non-200 response body from status request", zap.String("database", job.Database), zap.Error(err))
		return "", fmt.Errorf("could not unmarshal body on non-200 http response | %v", err.Error())
	}

	dbr.Log.Error("failed to check replication status", zap.String("error", rError.Error), zap.String("description", rError.Reason))

	return "", nil
}

func (dbr *DBReplicator) postReplication(job *replJob) error {
	doc, err := dbr.generateReplicationDocument(job)
	if err != nil {
		return fmt.Errorf("%v", err.Error())
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%v/_replicator", dbr.Source.Address), bytes.NewReader(doc))
	if err != nil {
		return fmt.Errorf("%v", err.Error())
	}

	req.Header.Add("Content-Type", "application/json")
	req.SetBasicAuth(dbr.Source.Username, dbr.Source.Password)

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%v", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 == 2 {
		dbr.Log.Debug("replication started", zap.String("database", job.Database))
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		dbr.Log.Debug("failed to read non-200 response body from replication", zap.String("database", job.Database), zap.Error(err))
		return fmt.Errorf("could not read body on non-200 http response | %v", err.Error())
	}

	var rError errorResponse
	err = json.Unmarshal(body, &rError)
	if err != nil {
		dbr.Log.Debug("failed to unmarshal non-200 response body from replication", zap.String("database", job.Database), zap.Error(err))
		return fmt.Errorf("could not unmarshal body on non-200 http response | %v", err.Error())
	}

	dbr.Log.Error("failed to post replication", zap.String("error", rError.Error), zap.String("description", rError.Reason))

	return fmt.Errorf("replication post failed: %v", rError.Error)
}

func (dbr *DBReplicator) generateReplicationDocument(job *replJob) ([]byte, error) {
	doc := replicationDocument{
		ID:     "auto_" + job.Database,
		Source: dbr.Source.Address + "/" + job.Database,
		Target: replDocTarget{
			URL: dbr.Target.Address + "/" + job.Database,
			Auth: replDocAuth{
				Credentials: replDocCredentials{
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
		return nil, err
	}

	return b, nil
}
