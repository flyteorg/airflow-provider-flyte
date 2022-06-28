# Flyte Provider for Apache Airflow

This package provides an operator, a sensor, and a hook that integrates [Flyte](flyte.org/) into Apache Airflow.
`FlyteOperator` is helpful to trigger a task/workflow in Flyte and `FlyteSensor` enables monitoring a Flyte execution status
for completion.

## Installation

Prerequisites: An environment running `apache-airflow`.

```
pip install airflow-provider-flyte
```

## Configuration

In the Airflow UI, configure a _Connection_ for Flyte.

- Host (required): The FlyteAdmin host.
- Port (optional): The FlyteAdmin port.
- Login (optional): `client_id`
- Password (optional): `client_credentials_secret`
- Extra (optional): Specify the `extra` parameter as JSON dictionary to provide additional parameters.
  - `project`: The default project to connect to.
  - `domain`: The default domain to connect to.
  - `insecure`: Whether to use SSL or not.
  - `command`: The command to execute to return a token using an external process.
  - `scopes`: List of scopes to request.
  - `auth_mode`: The OAuth mode to use. Defaults to pkce flow.
  - `env_prefix`: Prefix that will be used to lookup for injected secrets at runtime.
  - `default_dir`: Default directory that will be used to find secrets as individual files.
  - `file_prefix`: Prefix for the file in the `default_dir`.
  - `statsd_host`: The statsd host.
  - `statsd_port`: The statsd port.
  - `statsd_disabled`: Whether to send statsd or not.
  - `statsd_disabled_tags`: Turn on to reduce cardinality.
  - `local_sandbox_path`
  - S3 Config:
    - `s3_enable_debug`
    - `s3_endpoint`
    - `s3_retries`
    - `s3_backoff`
    - `s3_access_key_id`
    - `s3_secret_access_key`
  - GCS Config:
    - `gsutil_parallelism`

## Modules

### [Flyte Operator](https://github.com/flyteorg/airflow-provider-flyte/blob/master/flyte_provider/operators/flyte.py)

The `FlyteOperator` requires a `flyte_conn_id` to fetch all the connection-related
parameters that are useful to instantiate `FlyteRemote`. Also, you must give a
`launchplan_name` to trigger a workflow, or `task_name` to trigger a task; you can give a
handful of other values that are optional, such as `project`, `domain`, `max_parallelism`,
`raw_data_prefix`, `kubernetes_service_account`, `labels`, `annotations`,
`secrets`, `notifications`, `disable_notifications`, `oauth2_client`, `version`, and `inputs`.

Import into your DAG via:

```
from flyte_provider.operators.flyte import FlyteOperator
```

### [Flyte Sensor](https://github.com/flyteorg/airflow-provider-flyte/blob/master/flyte_provider/sensors/flyte.py)

If you need to wait for an execution to complete, use `FlyteSensor`.
Monitoring with `FlyteSensor` allows you to trigger downstream processes only when the Flyte executions are complete.

Import into your DAG via:

```
from flyte_provider.sensors.flyte import FlyteSensor
```

## Examples

See the [examples](https://github.com/flyteorg/airflow-provider-flyte/tree/master/flyte_provider/example_dags) directory for an example DAG.

## Issues

Please file issues and open pull requests [here](https://github.com/flyteorg/airflow-provider-flyte).
If you hit any roadblock, hit us up on [Slack](https://slack.flyte.org/).
