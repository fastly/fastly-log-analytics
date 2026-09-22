# Deployment Traffic Seeding Logging

## Goal

Make the live traffic-seeding phase of `scripts/dev/deploy_test_all.sh`
diagnosable without changing its load profile or target environments.

## Design

Replace the four inline background `scale_harness.py` launches with a shared
shell wrapper. The wrapper will log the environment name, exact Fastly URL,
service ID, backend checkpoint URL, stage/concurrency/RUM settings, PID,
start time, exit status, and elapsed time. Each harness process will write its
stdout and stderr to a named file under the active deployment report's
`logs/` directory, while a concise lifecycle record remains in `deploy.log`.

The wrapper will preserve the existing `IGNORE_LOCAL_STD` behavior and the
existing waits. A non-zero harness exit will remain visible in the deployment
output and will cause the traffic-seeding phase to fail as it does today.

## Verification

Run the shell syntax check and inspect a deployment report to confirm that the
traffic section lists all target URLs and that each per-environment harness
log is created with an exit status.
