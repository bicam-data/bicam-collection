# Lane status systemd files

Nothing in this directory is installed automatically. Copying these files
changes what the production timers do on their next fire, so a human runs
the steps below by hand.

What gets installed:

- `bicam-.service.d/10-lane-status.conf` — prefix drop-in that systemd
  applies to every `bicam-*.service`. Adds `ExecStopPost=` (records the
  run in `lane_runs` via `scripts/record_lane_run.py`) and
  `OnFailure=alert-bicam-lane@%n.service`.
- `alert-bicam-lane@.service` — template started on failure; runs
  `scripts/alert_lane_failure.py --unit <failed unit>`, which appends a
  JSON line to `~/bicam/reports/lane-alerts.log` and stamps
  `details->'alerted_at'` on the newest `lane_runs` row for that unit.

## 1. Apply the migration

`lane_runs` must exist before the drop-in is installed, otherwise every
lane records a stderr line instead of a row (the lane itself is unaffected).

```bash
cd ~/bicam/bicam-collection
uv run python scripts/run_migrations.py
```

Expected output includes `016_lane_runs.sql`.

## 2. Install the units

```bash
cd ~/bicam/bicam-collection
mkdir -p ~/.config/systemd/user/bicam-.service.d
cp deploy/systemd/bicam-.service.d/10-lane-status.conf ~/.config/systemd/user/bicam-.service.d/10-lane-status.conf
cp deploy/systemd/alert-bicam-lane@.service ~/.config/systemd/user/alert-bicam-lane@.service
systemctl --user daemon-reload
```

## 3. Verify

```bash
systemctl --user cat bicam-html-text.service
```

The output should end with the `10-lane-status.conf` drop-in showing the
`OnFailure=` and `ExecStopPost=` lines. Then confirm the template resolves:

```bash
systemctl --user show alert-bicam-lane@bicam-html-text.service.service -p ExecStart
```

After the next timer fire, a row appears:

```bash
uv run python scripts/report_lane_status.py
```

which writes `~/bicam/reports/lane-status.md` and `lane-status.json`.

## Removal

```bash
rm ~/.config/systemd/user/bicam-.service.d/10-lane-status.conf
rm ~/.config/systemd/user/alert-bicam-lane@.service
systemctl --user daemon-reload
```
