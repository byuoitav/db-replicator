package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.uber.org/zap"
)

type replicationDocument struct {
	ID           string `json:"_id"`
	Source       string `json:"source"`
	Target       string `json:"target"`
	CreateTarget bool   `json:"create_target"`
	Continuous   bool   `json:"continuous"`
}

type errorResponse struct {
	Error  string `json:"error"`
	Reason string `json:"reason"`
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
	source, err := dbr.Source.getAddressWithCredentials()
	if err != nil {
		dbr.Log.Debug("failed to aggregate source credentials", zap.Error(err))
		return nil, fmt.Errorf("cannot create replication doc: %s", err.Error())
	}
	target, err := dbr.Target.getAddressWithCredentials()
	if err != nil {
		dbr.Log.Debug("failed to aggregate target credentials", zap.Error(err))
		return nil, fmt.Errorf("cannot create replication doc: %s", err.Error())
	}

	doc := replicationDocument{
		ID:           "auto_" + job.Database,
		Source:       source + "/" + job.Database,
		Target:       target + "/" + job.Database,
		CreateTarget: false,
		Continuous:   job.Continuous,
	}

	b, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// add credentials to db address func (db *replDB)
func (db *replDB) getAddressWithCredentials() (string, error) {
	if db.Address == "" || db.Username == "" || db.Password == "" {
		return "", fmt.Errorf("missing database credentials")
	}

	addr := strings.Split(db.Address, "://")
	if len(addr) < 2 {
		return "", fmt.Errorf("database address missing protocol")
	}

	return fmt.Sprintf("%v://%v:%v@%v", addr[0], db.Username, db.Password, addr[1]), nil
}
