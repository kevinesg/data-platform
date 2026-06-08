# metabase

The `metabase` component owns analytics service runtime and configuration.

Metabase connects to published warehouse tables as an analytics client. It
stays independent from Airflow, dbt, and scripts internals.

Dashboard automation can come later when manually validated dashboards are
stable enough to preserve as code.

## Setup Status

Metabase setup commands are intentionally not present yet. The platform is being
rebuilt in order: scripts first, then dbt, then runtime images and orchestration,
then analytics service integration.

Use the root `README.md` for the current setup flow. This README becomes the
Metabase end-to-end setup entrypoint when the Metabase component is implemented.
