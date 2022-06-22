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

- Host(optional): The FlyteAdmin host. Defaults to localhost.
- Port (optional): The FlyteAdmin port. Defaults to 30081.
- Login (optional): `client_id`
- Password (optional): `client_credentials_secret`
- Extra (optional): Specify the `extra` parameter as JSON dictionary to provide additional parameters.
  - `project`: The default project to connect to.
  - `domain`: The default domain to connect to.
  - `insecure`: Whether to use SSL or not.
  - `command`: The command to execute to return a token using an external process.
  - `scopes`: List of scopes to request.
  - `auth_mode`: The OAuth mode to use. Defaults to pkce flow.

## Modules

### [Flyte Operator](https://github.com/flyteorg/airflow-provider-flyte/blob/main/flyte_provider/operators/flyte.py)

The `FlyteOperator` requires a `flyte_conn_id` to fetch all the connection-related
parameters that are useful to instantiate `FlyteRemote`. Also, you must give a
`launchplan_name` to trigger a workflow, or `task_name` to trigger a task; you can give a
handful of other values that are optional, such as `project`, `domain`, `max_parallelism`,
`raw_data_prefix`, `assumable_iam_role`, `kubernetes_service_account`, `labels`, `annotations`,
`secrets`, `notifications`, `disable_notifications`, `oauth2_client`, `version`, and `inputs`.

Import into your DAG via:

```
from flyte_provider.operators.flyte import FlyteOperator
```

### [Flyte Sensor](https://github.com/flyteorg/airflow-provider-flyte/blob/main/flyte_provider/sensors/flyte.py)

If you need to wait for an execution to complete, use `FlyteSensor`.
Monitoring with `FlyteSensor` allows you to trigger downstream processes only when the Flyte executions are complete.

Import into your DAG via:

```
from flyte_provider.sensors.flyte import FlyteSensor
```

## Examples

See the [examples](https://github.com/flyte/airflow-provider-flyte/tree/main/flyte_provider/example_dags) directory for an example DAG.

## Issues

Please file issues and open pull requests [here](https://github.com/flyteorg/airflow-provider-flyte).
If you hit any roadblock, hit us up on [Slack](https://slack.flyte.org/).

### Pre-commit Hooks

Please use [pre-commit](https://pre-commit.com/) to automate linting and code formatting on every commit.
Run `pre-commit install` after installing running `pip install pre-commit`.
