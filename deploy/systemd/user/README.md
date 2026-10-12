# Production systemd user units

These are the units installed in `~/.config/systemd/user/` on the production
host. Paths use `%h` and assume this repo is checked out at
`~/bicam/bicam-collection`, with `rotisserie` and `reports` as siblings.

The lanes call host `psql` (`sudo apt-get install postgresql-client-16`), so
install that first. Installing the units changes what the timers run on their
next fire, so do it when no lane is active:

```bash
# Wait until nothing holds the shared lock
flock -n /run/user/$(id -u)/bicam-ingestion.lock true && echo idle

systemctl --user stop 'bicam-*.timer'
cp -a ~/bicam/bicam-collection/deploy/systemd/user/bicam-* ~/.config/systemd/user/
# Units for the historical backfill lanes removed on 2026-10-12
(cd ~/.config/systemd/user && rm -rf bicam-amendment-* bicam-congress-cooldown-resume.* \
  bicam-congress-list-page-repair.service bicam-congress-partition-queue* \
  bicam-congress-per-key-refresh-persistent.service* bicam-congress-supplemental-history.service \
  bicam-ingestion-supervisor.* bicam-reconciliation-probe.service)
systemctl --user daemon-reload
systemctl --user start $(systemctl --user list-unit-files 'bicam-*.timer' --state=enabled --no-legend | cut -d' ' -f1)
systemctl --user list-timers 'bicam-*'
```

Timers are enabled individually; see the table in `HANDOFF.md` for which are
on. Edit units here first, then copy, so this directory stays the source of
truth.
