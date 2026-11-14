# Watching your alert pipeline

This page shows how to make knell post to Discord when your Prometheus or Loki alert pipeline stops delivering. It is for operators who run Alertmanager and want to hear when it goes quiet.

knell can tell you when your alert pipeline itself stops, whether the rule evaluator, the metric store or Alertmanager failed. Keep one rule always firing and have Alertmanager deliver it to a knell beat. Then knell posts to Discord when those deliveries stop. knell never sends through Alertmanager, so it still reaches you when Alertmanager is the part that died.

1. Add a beat for the pipeline to `BEATS`, such as `pipeline-watchdog:20m`.
2. Add a rule that always fires to your ruler:

   ```yaml
   groups:
     - name: pipeline-watch
       rules:
         - alert: Watchdog
           expr: vector(1)
           labels:
             severity: none
           annotations:
             summary: "Always firing, so knell hears from the alert pipeline while it works"
   ```

3. Route it to a webhook receiver for the beat, first in your Alertmanager `route`, and set `repeat_interval` below the beat's deadline. Alertmanager's default `repeat_interval` is 4h, so a 20-minute beat would page between repeats.

   ```yaml
   route:
     routes:
       - matchers: ['alertname="Watchdog"']
         receiver: knell
         group_wait: 0s
         group_interval: 1m
         repeat_interval: 5m
   receivers:
     - name: knell
       webhook_configs:
         - url: http://knell:9190/beat/pipeline-watchdog
           send_resolved: false
           http_config:
             authorization:
               type: Bearer
               credentials_file: /etc/alertmanager/knell_beat_token
   ```

   The file holds your `BEAT_TOKEN`. knell ignores the request body, so Alertmanager's webhook payload is fine. With `ALLOWED_HOSTS` set, add the name in the URL, here `knell`.

With more than one ruler, give each its own always-firing rule, route and beat, so the Discord notice names the ruler that went quiet.

The alert rules that watch knell itself are in [Monitoring and alerts](monitoring.md#alerting).
