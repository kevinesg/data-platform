# scripts

The `scripts` component owns extract/load commands and source-specific pipeline
code.

This component should be terminal-testable without Airflow. Airflow can
orchestrate script commands later, but extraction and loading logic belongs here.

Source-specific contracts and commands should live near the relevant pipeline
implementation instead of accumulating in this component README.
