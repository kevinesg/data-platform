# metabase

The `metabase` component owns analytics service runtime and configuration.

Metabase connects to published warehouse tables as an analytics client. It
should stay independent from Airflow, dbt, and scripts internals.

Dashboard automation can come later when manually validated dashboards are
stable enough to preserve as code.
