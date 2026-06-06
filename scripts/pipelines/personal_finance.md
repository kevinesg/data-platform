# personal_finance

The `personal_finance` source models a Google Sheet as an external source
system. The current source contract includes four sheet tabs:

| Sheet tab | Raw table | Purpose |
| --- | --- | --- |
| `transactions` | `personal_finance__transactions` | Personal income/expense rows that affect cashflow. |
| `paid_for_others` | `personal_finance__paid_for_others` | Payments made for other people. |
| `transfers` | `personal_finance__transfers` | Internal account movements plus direct inflows and outflows. |
| `accounts` | `personal_finance__accounts` | Account dimension data used for balance reporting. |

The repository contains no real finance data. Local exports, credentials, and
warehouse service account files stay outside version control.

## Local Extract

Live extraction requires dev source access first:

- a dev GCP project with a scripts service account.
- a service-account JSON key stored outside the repository.
- the source Google Sheet shared with that service account.
- a GCS landing bucket with write access for the scripts service account.
- `scripts/.env` populated from `scripts/.env.example`.

Until those prerequisites exist, validate this code path with unit tests and
`--help` only. Do not invent placeholder credentials or commit local exports.

Run from the `scripts/` directory after the prerequisites are ready:

```bash
RUN_ID=dev-test-001
uv run python src/personal_finance.py --run-id "$RUN_ID"
```

The command reads the configured Google Sheet in chunks, filters and coerces
fields through `schemas/personal_finance__*.json`, and writes run-scoped JSONL
chunks to GCS under
`gs://$PERSONAL_FINANCE_GCS_BUCKET/$PERSONAL_FINANCE_GCS_PREFIX/<entity>/$RUN_ID/extract/`.

To extract one entity while debugging:

```bash
uv run python src/personal_finance.py --entity transactions --run-id "$RUN_ID"
```

The raw load step will read these durable staged files instead of re-reading the
source.
