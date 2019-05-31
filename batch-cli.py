#!/usr/bin/env python3
import argparse
import datetime
import fnmatch
import logging
import time
import typing
from enum import Enum, unique

import attr
import boto3
import botocore.exceptions
import humanfriendly
import parsedatetime
from colorama import Fore, Style
from colorama import init

init()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__file__)

batch = boto3.client('batch')
client = boto3.client('logs')


@unique
class JobStatus(Enum):
    SUBMITTED = 'SUBMITTED'
    PENDING = 'PENDING'
    RUNNABLE = 'RUNNABLE'
    STARTING = 'STARTING'
    RUNNING = 'RUNNING'
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'

    # magic methods for argparse compatibility
    def __str__(self):
        return self.name.upper()

    def __repr__(self):
        return str(self)

    @staticmethod
    def argparse(s):
        try:
            return JobStatus[s.upper()]
        except KeyError:
            return s


@attr.s
class CommandLineArgs(object):
    job_queue = attr.ib(type=str)
    job_name = attr.ib(type=str)
    job_status = attr.ib(type=typing.List[JobStatus])
    since = attr.ib(type=str)
    watch = attr.ib(type=bool)


# def millisecond_timestamp_to_datetime(ts):
#     try:
#         return datetime.datetime.fromtimestamp(ts / 1000).astimezone()
#     except TypeError:
#         return None


@attr.s(frozen=True)
class BatchJobSummary:
    jobId = attr.ib(type=str)
    jobName = attr.ib(type=str)
    status = attr.ib(type=JobStatus, converter=JobStatus)
    createdAt = attr.ib(type=float)
    container = attr.ib(type=dict, default=None, cmp=False, repr=False)
    arrayProperties = attr.ib(type=dict, default=None, cmp=False, repr=False)
    statusReason = attr.ib(type=str, default=None)
    stoppedAt = attr.ib(type=float, default=None)
    startedAt = attr.ib(type=float, default=None)


def list_queue_jobs(job_queue: str, job_statuses: typing.List[JobStatus], **kwargs):
    for job_status in job_statuses:
        params = {}
        while True:
            jobs = batch.list_jobs(jobQueue=job_queue, jobStatus=job_status.value, **params)
            for job in jobs['jobSummaryList']:
                yield BatchJobSummary(**job)
            if 'nextToken' in jobs:
                params = dict(nextToken=jobs['nextToken'])
            else:
                break


def job_list_diff(existing_jobs: typing.List[BatchJobSummary], latest_fetch: typing.List[BatchJobSummary]):
    return list(set(existing_jobs) - set(latest_fetch))


def job_status_color_map(status: JobStatus):
    return {
        JobStatus.SUBMITTED: Fore.LIGHTBLACK_EX,
        JobStatus.PENDING: Fore.LIGHTBLACK_EX,
        JobStatus.RUNNABLE: Fore.LIGHTBLUE_EX,
        JobStatus.STARTING: Fore.YELLOW,
        JobStatus.RUNNING: Fore.YELLOW,
        JobStatus.SUCCEEDED: Fore.GREEN,
        JobStatus.FAILED: Fore.RED
    }[status]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Utilities for AWS Batch')
    parser.add_argument('--job-queue', required=True)
    parser.add_argument('--job-name', default='*', help='name of job to filter. Shell-style wildcards are supported')
    parser.add_argument('--job-status', action='append', type=JobStatus.argparse, choices=list(JobStatus), default=[])
    parser.add_argument('--since', type=str, default='now')
    parser.add_argument('--watch', action='store_true')
    args = CommandLineArgs(**parser.parse_args().__dict__)

    session = boto3.session.Session()
    region = session.region_name

    if len(args.job_status) < 1:
        args.job_status = list(JobStatus)

    time_s, parse_s = parsedatetime.Calendar().parse(args.since)
    since = int(datetime.datetime(*time_s[:6]).timestamp() * 1000)

    print(args)
    job_details_map = {}
    tracked_jobs = []
    while True:
        job_generator = list_queue_jobs(job_queue=args.job_queue, job_statuses=args.job_status)
        job_snapshot = [job for job in job_generator if job.createdAt > since and fnmatch.fnmatchcase(job.jobName, args.job_name)]
        new_jobs = sorted(job_list_diff(job_snapshot, tracked_jobs), key=lambda job: job.createdAt)

        for job in new_jobs:
            log_events = []
            reason = {f' "{job.statusReason}"'} if job.statusReason and job.status != JobStatus.SUCCEEDED else ''
            duration = f' "completed in {humanfriendly.format_timespan((job.stoppedAt - job.startedAt) / 1000)}"' if job.startedAt and job.stoppedAt else ""
            if job.container and 'reason' in job.container:
                print(job.container['reason'])

            log_link = ''
            log_stream_name = ''
            if job.status in (JobStatus.RUNNING, JobStatus.FAILED):
                if job.jobId not in job_details_map:
                    job_details_map[job.jobId] = batch.describe_jobs(jobs=[job.jobId])['jobs'][0]

                try:
                    log_stream_name = job_details_map[job.jobId]['container']['logStreamName']
                    log_link = f" https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}#logEventViewer:group=/aws/batch/job;stream={log_stream_name}"
                except:
                    # This is probably an array or multi-node job
                    # print(job_details_map[job.jobId])
                    # raise
                    pass

            print(f'{job_status_color_map(job.status)}{job.status:<9}{Style.RESET_ALL} {job.jobId} {job.jobName}{duration}{reason}{log_link}')

            if job.status in (JobStatus.FAILED, ):
                try:
                    log_events = client.get_log_events(
                        logGroupName='/aws/batch/job',
                        logStreamName=log_stream_name,
                        limit=20
                    )
                    for event in log_events['events']:
                        print(f"{Fore.LIGHTBLACK_EX} {event['message']}")
                    print(Style.RESET_ALL)
                except botocore.exceptions.ParamValidationError:
                    pass

        if not args.watch:
            break

        tracked_jobs.extend(new_jobs)
        time.sleep(1)
