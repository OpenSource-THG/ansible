#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
   Copyright 2020 THG / The Hut Group
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'status': ['preview'],
    'supported_by': 'community'
}

DOCUMENTATION = '''
---
module: cockroach_cluster_health
short_description: Checks the health of the cockroach node in question to
version_added: "2.9.2"
author:
    - Mohammed Isap
description:
    - "Looks at the replicas/leaseholders per store, polls every 10s and makes sure they're the same"
requirements:
    - "python >= 2.7 < 3" (instructions for python3+ support included)
options:
    host:
        description:
            - The host/address of the cockroach node
            - Support to check a specific node to come in a future version
        required: false
        default: localhost
    node_id:
        description:
            - This is the id of the node
            - Support to check a specific node to come in a future version
        required: false
    certs_dir:
        description:
            - The directory where the certificates are stored to connect to a secure cluster
        required: true
    timeout:
        description:
            - Amount of time IN SECONDS to keep trying before timing out
        required: false
        default: 600
'''

EXAMPLES = '''
# Check health
- name: Check cluster health
  cockroach_cluster_health:
      certs_dir: /opt/cockroachdb/certs
'''
RETURN = ''

from ansible.module_utils.basic import AnsibleModule
import csv as csv_reader
import json
import time
from StringIO import StringIO
#----- comment line above and uncomment following line for python3+ -------
#from io import StringIO

def execute_command(module, command):
    (rc, stdout, stderr) = module.run_command(command, check_rc=True)
    if rc != 0:
        module.fail_json(msg="An error occurred: {}. Tried to execute '{}'".format(stderr, command))
    return stdout

def get_nodes_status(module, certs_dir, format):
    command = "cockroach node status --ranges --certs-dir={} --format={}".format(certs_dir, format)
    return execute_command(module, command)

def convert_csv_to_json(single_stat):
    reader = csv_reader.DictReader(StringIO(single_stat))

    stat_array=[]
    stat_json = {}

    for row in reader:
        stat_array.append(row)
    stat_json = json.dumps(stat_array)

    return stat_json

#all_stats = array of 6 json objects where each object contains an array of status info, for n nodes, at a given point in time
def populate_all_stats(module, certs_dir, all_stats) :
    if (len(all_stats) == 6) :
        all_stats.pop(0)
        nodes_status = get_nodes_status(module, certs_dir, "csv");
        all_stats.append(convert_csv_to_json(nodes_status))
    else :
        all_stats = []
        start_time = time.time()
        while (len(all_stats) < 6) :
            nodes_status = get_nodes_status(module, certs_dir, "csv")
            all_stats.append(convert_csv_to_json(nodes_status))
            time.sleep(10.0 - ((time.time() - start_time) % 10.0))
    return all_stats

# the stats from all_stats or then sorted by node rather than by time i.e. node n : 6 status entries for n
def sort_stats(all_stats):
    sorted_stats = {}

    for status in all_stats: #for each json object
        status = json.loads(status)

        for node in status: #for each node in the group
            id = node['id']
            if (id in sorted_stats):
                sorted_stats[id].append(node)
            else:
                sorted_stats[id] = []
                sorted_stats[id].append(node)

    return sorted_stats

#check if all "replicas_leaseholders" are equal to the first one for a given node
def check_stats(sorted_stats):
    checked_stats = sorted_stats
    for key, value in checked_stats.items() : #node id to 6 status entries for that node
        if all(v["replicas_leaseholders"] == value[0]["replicas_leaseholders"] for v in value):
            del checked_stats[key]
    return checked_stats

def get_times(start_time, end_time):
    times = {}
    times["Start Time"] = time.ctime(start_time)
    times["End Time"] = time.ctime(end_time)
    times["Duration"] = round(end_time-start_time)
    return times

def main():
    module = AnsibleModule(
        argument_spec = dict(
            host = dict(required = False, default = 'localhost', type = 'str'),
            node_id = dict(required = False, type = 'int'),
            certs_dir = dict(required = True, default = None, type = 'str'),
            timeout = dict(required = False, default = 600, type = 'int')
        ),
        supports_check_mode = True
    )

    result = dict(
        changed=False,
        msg=''
    )

    if module.check_mode:
        module.exit_json(**result)

    host = module.params['host']
    node_id = module.params['node_id']
    certs_dir = module.params['certs_dir']
    timeout = module.params['timeout']

# start of the checking script
    all_stats = []
    start_time = time.time()
    check_until = start_time + timeout

    while True:
        loop_start_time = time.time()
        all_stats = populate_all_stats(module, certs_dir, all_stats)
        sorted_stats = sort_stats(all_stats)
        checked_stats = check_stats(sorted_stats)

        if len(checked_stats) == 0 :
            result['changed'] = True
            end_time = time.time()
            result['msg'] = "Cluster Healthy!! {}".format(get_times(start_time, time.time()))
            break
        elif time.time() > check_until :
            result['msg'] = "Cluster Health Check TIMEDOUT at {}".format(time.ctime(time.time()))
            break
        else :
            time.sleep(10.0 - ((time.time() - loop_start_time) % 10.0))
# end of checking script

    module.exit_json(**result)

if __name__ == '__main__':
    main()
