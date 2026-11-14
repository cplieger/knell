# The Grafana dashboard

This page describes knell's Grafana dashboard, what each tab shows and how to keep it matched to the image you run. It is for operators who scrape knell with Prometheus and use Grafana.

[`grafana-dashboard.json`](../grafana-dashboard.json) imports as the knell dashboard. It reads the [metrics knell publishes](monitoring.md#metrics) from a Prometheus data source. It needs Grafana 13.2 or newer. When you run more than one knell, the knell variable picks the instance to show. It has three tabs:

- Overview counts the beats past their deadline, which mirrors `KnellBeatOverdue`, and the beats watched, with the pings and outages in the selected range. Below them, the Needs attention row has one hidden tile for each other rule in `alerts/promql.yaml`, and appears only while one of those alerts fires. The Beats table shows each beat as Fresh or Missing, when knell last accepted a ping for it, its deadline, the share of the deadline left and its outages in the range.
- History charts the check-ins and outages per beat, when each beat was fresh or missing, and the notices sent, failed or lost.
- knell health shows the scrape, the listener's accept errors, the requests refused before routing and why, when knell last started, and the request latency and status codes.

The tiles and the count of beats past their deadline read each rule's `ALERTS` series, which Prometheus and the Mimir ruler store beside your metrics. The Needs attention row stays hidden while those rules are not loaded. Without them, the count shows the beats that have been past their deadline for 7 minutes. The two LogQL rules read the log, so the dashboard has no tile for them.

The dashboard is versioned with the image, and the `grafana-dashboard.json` of release `<tag>` matches the metrics that image serves. Each release attaches it with its checksum at `https://github.com/cplieger/knell/releases/download/<tag>/grafana-dashboard.json`, and each image publish pushes it as the OCI artifact `ghcr.io/cplieger/knell/dashboard:<tag>`. The file sets `metadata.name` to `knell`, which Grafana uses as the dashboard UID, so importing a newer copy over the old one updates it in place.
