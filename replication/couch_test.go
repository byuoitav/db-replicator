package replication

import (
	"encoding/json"
	"testing"

	assert "github.com/go-playground/assert/v2"
)

func TestGenIDSelector(t *testing.T) {
	job := &replJob{
		Continuous: true,
		Database:   "database",
		IDSelector: "",
	}

	sel, _ := json.Marshal(job.genIDSelector())

	assert.Equal(t, string(sel), "{\"_id\":{\"$regex\":\"\"}}")

	job.IDSelector = "BLDG-ROOM"
	sel, _ = json.Marshal(job.genIDSelector())

	assert.Equal(t, string(sel), "{\"_id\":{\"$regex\":\"BLDG-ROOM\"}}")
}

func TestGenerateReplicationDocument(t *testing.T) {
	repl := &DBReplicator{
		Log: nil,
		Source: &replDB{
			Address:  "src-address",
			Username: "src-username",
			Password: "src-password",
		},
		Target: &replDB{
			Address:  "trgt-address",
			Username: "trgt-username",
			Password: "trgt-password",
		},
		timeInterval: 60,
		jobs:         make([]replJob, 0),
	}

	doc, err := repl.generateReplicationDocument(&replJob{
		Continuous: true,
		Database:   "database",
		IDSelector: "BLDG-ROOM",
	})
	if err != nil {
		t.Error(err.Error())
	}

	assert.Equal(t, string(doc), "{\"_id\":\"auto_database\",\"source\":{\"url\":\"src-address/database\",\"auth\":{\"basic\":{\"username\":\"src-username\",\"password\":\"src-password\"}}},\"target\":{\"url\":\"trgt-address/database\",\"auth\":{\"basic\":{\"username\":\"trgt-username\",\"password\":\"trgt-password\"}}},\"selector\":{\"_id\":{\"$regex\":\"BLDG-ROOM\"}},\"create_target\":true,\"continuous\":true}")
}
