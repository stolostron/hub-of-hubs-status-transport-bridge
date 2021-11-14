[comment]: # ( Copyright Contributors to the Open Cluster Management project )

# Hub-of-Hubs Status Transport Bridge

[![Go Report Card](https://goreportcard.com/badge/github.com/open-cluster-management/hub-of-hubs-status-transport-bridge)](https://goreportcard.com/report/github.com/open-cluster-management/hub-of-hubs-status-transport-bridge)
[![License](https://img.shields.io/github/license/open-cluster-management/hub-of-hubs-status-transport-bridge)](/LICENSE)

The status transport bridge component of [Hub-of-Hubs](https://github.com/open-cluster-management/hub-of-hubs).

Go to the [Contributing guide](CONTRIBUTING.md) to learn how to get involved.

## Getting Started

## Build and push the image to docker registry

1.  Set the `REGISTRY` environment variable to hold the name of your docker registry:
    ```
    $ export REGISTRY=...
    ```
    
1.  Set the `IMAGE_TAG` environment variable to hold the required version of the image.  
    default value is `latest`, so in that case no need to specify this variable:
    ```
    $ export IMAGE_TAG=latest
    ```
    
1.  Run make to build and push the image:
    ```
    $ make push-images
    ```

## Deploy on the hub of hubs

Set the `DATABASE_URL` according to the PostgreSQL URL format: `postgres://YourUserName:YourURLEscapedPassword@YourHostname:5432/YourDatabaseName?sslmode=verify-full&pool_max_conns=50`.

:exclamation: Remember to URL-escape the password, you can do it in bash:

```
python -c "import sys, urllib as ul; print ul.quote_plus(sys.argv[1])" 'YourPassword'
```

1.  Create a secret with your database url:

    ```
    kubectl create secret generic hub-of-hubs-database-transport-bridge-secret -n open-cluster-management --from-literal=url=$DATABASE_URL
    ```

1.  Set the `REGISTRY` environment variable to hold the name of your docker registry:
    ```
    $ export REGISTRY=...
    ```
    
1.  Set the `IMAGE` environment variable to hold the name of the image.

    ```
    $ export IMAGE=$REGISTRY/$(basename $(pwd)):latest
    ```

1.  Set the `SYNC_SERVICE_HOST` environment variable to hold the CSS host.
    ```
    $ export SYNC_SERVICE_HOST=...
    ```

1.  Set the `SYNC_SERVICE_PORT` environment variable to hold the CSS port.
    ```
    $ export SYNC_SERVICE_PORT=...
    ```
    
1.  Run the following command to deploy the `hub-of-hubs-status-transport-bridge` to your hub of hubs cluster:  
    ```
    envsubst < deploy/hub-of-hubs-status-transport-bridge.yaml.template | kubectl apply -f -
    ```
    
## Cleanup from the hub of hubs
    
1.  Run the following command to clean `hub-of-hubs-status-transport-bridge` from your hub of hubs cluster:  
    ```
    envsubst < deploy/hub-of-hubs-status-transport-bridge.yaml.template | kubectl delete -f -
    ```
