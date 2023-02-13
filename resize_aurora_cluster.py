"""
This script safely resizes an Aurora cluster to a new instance class/size.
It determines the writer and the reader(s), resizes readers first, then
finishes with the writer which will trigger AWS to automatically promote
one of the already resized readers to be the new writer.

The script is idempotent. If anything unexpected happens, e.g. a step
times out, it's safe to just run it again: it will skip any instance
that is already resized.

However, if it encounters an instance that is not in the expected
'available' status, it will bail out and ask you to investigate.

Size changes take about 10m per instance, class changes can take longer
"""
import argparse
import boto3
import time
from datetime import datetime


def parse_args():
    parser = argparse.ArgumentParser(description='resizes db instances in a staged manner')

    parser.add_argument('--instance-class','-c', help='instance class e.g. db.r5.large')
    parser.add_argument('--region','-r', default='us-east-1', help='aws region')
    parser.add_argument('--dryrun','-d', action='store_true', help='don\'t do anything')
    parser.add_argument('--skip-prompt','-s', action='store_true', help='don\'t prompt before starting')
    known_args, args = parser.parse_known_args()
    return known_args, args


def logit(msg):
    print(f'{datetime.now()} {msg}')


def get_instance_info(client, db_instance_id):
    response = client.describe_db_instances(
        Filters=[
            {
                'Name': 'db-instance-id',
                'Values': [
                    db_instance_id,
                ]
            },
        ]
    )
    instances = response.get('DBInstances')
    if len(instances) != 1:
        raise ValueError(f'expecting one instance found {response}')
    instance = instances[0]
    current_class = instance.get('DBInstanceClass')
    # gather utilization stats
    status = instance.get('DBInstanceStatus')
    return current_class, status


def wait_for_instance_ready(client, db_instance_id, desired_class):
    # first wait for state 'modifying'
    start_time_secs = time.time()
    limit_secs = 300
    sleep_time = 15

    logit('Waiting to start modifying')
    while True:
        time.sleep(sleep_time)
        size, status = get_instance_info(client, db_instance_id)
        logit(f'status is {status}')
        if status == 'modifying':
            break
        if time.time() - start_time_secs > limit_secs:
            raise Exception('Never saw "modifying", ABORT')

    # now wait for state 'available'
    start_time_secs = time.time()
    # size change can take over 10m
    limit_secs = 1200
    sleep_time = 30
    logit('Waiting to be "available" again')
    while True:
        time.sleep(sleep_time)
        size, status = get_instance_info(client, db_instance_id)
        logit(f'status is {status}')
        if status == 'available' and size == desired_class:
            break
        if time.time() - start_time_secs > limit_secs:
            raise Exception(f'Never saw {desired_class} and "available", ABORT')
    return


def prompt_user(instances_to_resize, desired_class):
    print('+' * 40)
    print(f'Ready to to proceed? Resizing to {desired_class} for:')
    print(f'{instances_to_resize}\n')
    input('Press Enter to proceed')


def resize_db_cluster(
        client,
        db_cluster_id,
        desired_class,
        dryrun,
        skip_prompt):

    instance_map = {}
    instance_map['readers'] = []
    response = client.describe_db_clusters(
        Filters=[
            {
                'Name': 'db-cluster-id',
                'Values': [
                    db_cluster_id,
                ]
            },
        ]
    )
    clusters = response['DBClusters']
    if len(clusters) != 1:
        raise ValueError(f'expecting only one cluster found more {response}')
    cluster = clusters[0]
    multiaz = cluster.get('MultiAZ')
    if not multiaz:
        raise ValueError(f'This cluster is not multiaz, update it with downtime instead') 
    members = cluster.get('DBClusterMembers')
    # get the size from the writer instance

    for member in members:
        dbinstance = member.get('DBInstanceIdentifier')
        size, status = get_instance_info(client, dbinstance)
        if status != 'available':
            raise ValueError(f'{dbinstance} status is not "available": {status}')
        db_info = {
            'name': dbinstance,
            'class': size
        }
        if member.get('IsClusterWriter'):
            instance_map['writer'] = db_info
        else:
            instance_map['readers'].append(db_info)

    # resize readers then writer
    instances_to_resize = instance_map['readers']
    instances_to_resize.append(instance_map['writer'])

    if not skip_prompt and not dryrun:
        prompt_user(instances_to_resize, desired_class)

    for db in instances_to_resize:
        current_class = db['class']
        db_name = db['name']
        if current_class == desired_class:
            logit(f'{db_name} is already {desired_class}, skipping')
            continue
        logit(f'Modifying {db_name} from {current_class} to {desired_class}')
        if dryrun:
            logit(f'dryrun enabled, skipping this operation')
            continue
        else:
            response = client.modify_db_instance(
                DBInstanceIdentifier=db_name,
                DBInstanceClass=desired_class,
                ApplyImmediately=True
            )
            # this, inexplicably returned without doing anything one time
            # so uncomment if you need to debug
            # print(response)
        wait_for_instance_ready(client, db_name, desired_class)


def main():
    args, other = parse_args()
    if len(other) != 1:
        raise ValueError('You must provide a db cluster id as the lone argument')
    db_cluster_id = other[0]
    client = boto3.client('rds', args.region)

    resize_db_cluster(
        client,
        db_cluster_id,
        args.instance_class,
        args.dryrun,
        args.skip_prompt)


if __name__ == '__main__':
    main()
