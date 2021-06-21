[comment]: # ( Copyright Contributors to the Open Cluster Management project )

# Hub-of-Hubs Status Transport Bridge
Red Hat Advanced Cluster Management Hub-of-Hubs Status Transport Bridge  

## How it works

## Build to run locally

```
make
```

## Run Locally

Set the following environment variables:

* SYNC_SERVICE_PROTOCOL
* SYNC_SERVICE_HOST
* SYNC_SERVICE_PORT
* DATABASE_URL

Set the `DATABASE_URL` according to the PostgreSQL URL format: `postgres://YourUserName:YourURLEscapedPassword@YourHostname:5432/YourDatabaseName?sslmode=verify-full`.

:exclamation: Remember to URL-escape the password, you can do it in bash:

```
python -c "import sys, urllib as ul; print ul.quote_plus(sys.argv[1])" 'YourPassword'
```

```
./build/bin/hoh-status-transport-bridge
```
