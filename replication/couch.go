package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"
)

type replicationDocument struct {
	ID           string      `json:"_id"`
	Rev          string      `json:"_rev,omitempty"`
	Source       replDocDB   `json:"source"`
	Target       replDocDB   `json:"target"`
	Selector     interface{} `json:"selector"`
	CreateTarget bool        `json:"create_target"`
	Continuous   bool        `json:"continuous"`
}

type replDocDB struct {
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

type replicationState struct {
	DB         string                 `json:"database"`
	DocID      string                 `json:"doc_id"`
	ErrorCount int                    `json:"error_count"`
	ID         string                 `json:"id"`
	Info       map[string]interface{} `json:"info"`
	Updated    time.Time              `json:"last_updated"`
	Start      time.Time              `json:"start_time"`
	Source     string                 `json:"source"`
	Target     string                 `json:"target"`
	State      string                 `json:"state"`
}

func (dbr *DBReplicator) checkReplication(job *replJob) (string, error) {
	dbr.Log.Debug("checking replication status", zap.String("database", job.Database))

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%v/_scheduler/docs/_replicator/%v%v", dbr.Target.Address, "auto_", job.Database), nil)
	if err != nil {
		dbr.Log.Debug("failed to build request to scheduler", zap.String("database", job.Database))
		return "", fmt.Errorf("failed to create http GET request | %v", err.Error())
	}

	req.SetBasicAuth(dbr.Target.Username, dbr.Target.Password)
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
		var status replicationState
		err = json.Unmarshal(body, &status)
		if err != nil {
			dbr.Log.Debug("failed to unmarshal replication state", zap.String("database", job.Database), zap.Error(err))
			return "", fmt.Errorf("could not unmarshal body on http response | %v", err.Error())
		}

		dbr.Log.Debug("successfully retrieved replication state", zap.String("database", job.Database), zap.String("state", status.State))
		return status.State, nil
	}

	var rError errorResponse
	err = json.Unmarshal(body, &rError)
	if err != nil {
		dbr.Log.Debug("failed to unmarshal non-200 response body from status request", zap.String("database", job.Database), zap.Error(err))
		return "", fmt.Errorf("could not unmarshal body on non-200 http response | %v", err.Error())
	}

	if rError.Error != "not_found" {
		dbr.Log.Error("replication status error", zap.String("error", rError.Error), zap.String("description", rError.Reason))
		return "", fmt.Errorf("error getting replication status: %v - %v", rError.Error, rError.Reason)
	}

	return "", nil
}

func (dbr *DBReplicator) postReplication(job *replJob) (bool, error) {
	dbr.Log.Debug("posting replication doc", zap.String("database", job.Database))

	doc, err := dbr.generateReplicationDocument(job)
	if err != nil {
		dbr.Log.Debug("could not generate replication document", zap.String("database", job.Database), zap.Error(err))
		return false, fmt.Errorf("failed to generate replication document | %v", err.Error())
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%v/_replicator", dbr.Target.Address), bytes.NewReader(doc))
	if err != nil {
		dbr.Log.Debug("failed to build http request", zap.Error(err))
		return false, fmt.Errorf("failed to build http request | %v", err.Error())
	}

	req.Header.Add("Content-Type", "application/json")
	req.SetBasicAuth(dbr.Target.Username, dbr.Target.Password)

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		dbr.Log.Debug("failed to run http request", zap.Error(err))
		return false, fmt.Errorf("http request failed | %v", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 == 2 {
		dbr.Log.Debug("replication started", zap.String("database", job.Database))
		return false, nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		dbr.Log.Debug("failed to read non-200 response body from replication", zap.String("database", job.Database), zap.Error(err))
		return false, fmt.Errorf("could not read body on non-200 http response | %v", err.Error())
	}

	var rError errorResponse
	err = json.Unmarshal(body, &rError)
	if err != nil {
		dbr.Log.Debug("failed to unmarshal non-200 response body from replication", zap.String("database", job.Database), zap.Error(err))
		return false, fmt.Errorf("could not unmarshal body on non-200 http response | %v", err.Error())
	}

	if rError.Error == "conflict" {
		dbr.Log.Debug("could not post, document conflict", zap.String("database", job.Database))
		return true, nil
	}

	dbr.Log.Error("replication posting error", zap.String("error", rError.Error), zap.String("description", rError.Reason))

	return false, fmt.Errorf("replication post failed: %v", rError.Error)
}

func (dbr *DBReplicator) getReplication(job *replJob) (*replicationDocument, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%v/_replicator/%v%v", dbr.Target.Address, "auto_", job.Database), nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(dbr.Target.Username, dbr.Target.Password)

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		dbr.Log.Debug("couldn't read response body from get replication request", zap.String("database", job.Database), zap.Error(err))
		return nil, fmt.Errorf("couldn't read body from get replication request | %v", err.Error())
	}

	if resp.StatusCode/100 != 2 {
		var rError errorResponse
		err = json.Unmarshal(body, &rError)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal body in non-200 http response | %v", err.Error())
		}
		dbr.Log.Error("failed to get replication document", zap.String("error", rError.Error), zap.String("reason", rError.Reason))
		return nil, fmt.Errorf("failed to get replication document")
	}

	var doc replicationDocument
	err = json.Unmarshal(body, &doc)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal replication document | %v", err.Error())
	}

	return &doc, nil
}

func (dbr *DBReplicator) deleteReplication(job *replJob) error {
	dbr.Log.Debug("deleting replication document", zap.String("database", job.Database))

	doc, err := dbr.getReplication(job)
	if err != nil {
		return fmt.Errorf("couldn't get replication document | %v", err.Error())
	}

	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%v/_replicator/%v%v?rev=%v", dbr.Target.Address, "auto_", job.Database, doc.Rev), nil)
	if err != nil {
		return fmt.Errorf("couldn't create http request to delete document | %v", err.Error())
	}

	req.SetBasicAuth(dbr.Target.Username, dbr.Target.Password)

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed | %v", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 == 2 {
		dbr.Log.Debug("replication document successfully deleted deleted", zap.String("database", job.Database))
		return nil
	}

	return fmt.Errorf("failed to delete replication document")
}

func (dbr *DBReplicator) generateReplicationDocument(job *replJob) ([]byte, error) {
	doc := replicationDocument{
		ID:  "auto_" + job.Database,
		Rev: "",
		Source: replDocDB{
			URL: dbr.Source.Address + "/" + job.Database,
			Auth: replDocAuth{
				Credentials: replDocCredentials{
					Username: dbr.Source.Username,
					Password: dbr.Source.Password,
				},
			},
		},
		Target: replDocDB{
			URL: dbr.Target.Address + "/" + job.Database,
			Auth: replDocAuth{
				Credentials: replDocCredentials{
					Username: dbr.Target.Username,
					Password: dbr.Target.Password,
				},
			},
		},
		Selector:     job.genIDSelector(),
		CreateTarget: true,
		Continuous:   job.Continuous,
	}

	b, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (j *replJob) genIDSelector() interface{} {
	return map[string]interface{}{
		"_id": map[string]interface{}{
			"$regex": j.IDSelector,
		},
	}
}
