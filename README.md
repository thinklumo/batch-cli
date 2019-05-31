batch-cli
=========
`batch-cli` is a simple command line tool for watching the stream of AWS Batch jobs.

### Features
- watches for job status changes
- displays links to Cloudwatch logs when they're available
- shows the last few lines of the Cloudwatch log when a job fails

## Getting started
```bash
pip install -r requirements.txt
```
At some point I'll set up proper PyPI distribution of this artifact, but for now you have to run it from the project. 

## Handy commands

Watch for all new job status changes for a given job queue
```bash
./batch-cli.py --job-queue=prod-HighPriorityCpuJobQueue --watch
```

Include the last 10 minutes of job status changes for a particular job name and then watch for new status changes
```bash
./batch-cli.py --job-queue=prod-HighPriorityCpuJobQueue --job-name="*gdp*" --watch --since="10 minutes ago"
```

Show all passing and failing jobs in the past hour and exit
```
 ./batch-cli.py --job-queue=prod-HighPriorityCpuJobQueue --job-status=FAILED --job-status=SUCCEEDED --since="1 hour ago"
```

## TODO
- Retrieve log files for array jobs
- set up PyPI distribution
