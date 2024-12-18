# Backup Couch Database Replicator
Service for replicating a source database to a target database on a set time interval.

Service requires a json configuration document.
Example:
```
{
    "source": {
        "address": "http://localhost:42069",
        "username": "username",
        "password": "password"
    },
    "target": {
        "address": "http://localhost:42068",
        "username": "username",
        "password": "password"
    },
    "jobs": [
        {
            "database": "devices",
            "continuous": false,
            "id_selector": "ABC-123"
        },
        {
            "database": "rooms",
            "continuous": false,
            "id_selector": ""
        }
    ],
    "time_interval": 120 //in minutes
}
```
