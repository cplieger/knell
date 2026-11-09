# Contributing to knell

The [shared rules](https://github.com/cplieger/.github/blob/main/CONTRIBUTING.md) for commits, releases, synced files and checks apply here.

## Dashboard

- `grafana-dashboard.json` reads series names and labels as text. A test in `internal/obs` fails when a panel reads a series or a label knell does not serve, or when a rule in `alerts/promql.yaml` has no Overview tile. Update the dashboard with any rename.
- Save a dashboard change made in the Grafana UI with Export, then Export as code, choosing the V2 Resource model.
- A change to `grafana-dashboard.json` takes `fix:` or `feat:`. Under `chore:` or `docs:` it replaces the dashboard's OCI artifact at the current version tags, and the GitHub Release keeps the old file.
