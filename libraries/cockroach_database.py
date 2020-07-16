#!/usr/bin/python
# -*- coding: utf-8 -*-

ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'status': ['preview'],
    'supported_by': 'community'
}

DOCUMENTATION = '''
---
module: cockroach_database
short_description: Manages cockroach databases
version_added: "2.9.2"
author:
    - Josh Blease
description:
    - "Allows the creation or deletion of Cockroach DB users"
requirements:
    - "python >= 2.7"
options:
    name:
        description:
            - This is the name of the user
        required: true
    password:
        description:
            - The password for the user to be created
        required: false
    admin:
        description:
            - Whether the user is an admin or not
        required: false
        default: false
    state:
        description:
            - Define whether the user with the specified name should exist or not
        required: false
        default: present
        choices: [ present, absent ]
    host:
        description:
            - The host/address to connect to the cockroach cluster with
        required: false
        default: localhost
    port:
        description:
            - The port used to connect to the cockroach cluster
        required: false
        default: 26257
    certs_dir:
        description:
            - The directory where the certificates are stored to connect to a secure cluster
        required: false
'''

EXAMPLES = '''
# Create a database
- name: Create a database
  cockroach_database:
    name: users

- name: Delete a database
  cockroach_database:
    name: users
    state: absent

# Create a database on a secure cluster
- name: Create a database
  cockroach_database:
    name: users
    certs_dir: /opt/cockroachdb/certs

# Create a database from a node not running CockroachDB
- name: Create a database
  cockroach_database:
    name: users
    host: cockroach-service.foo.bar
'''
RETURN = ''

from ansible.module_utils.basic import AnsibleModule

def prepareCommandFormat(host, port, certs_dir):
    secure_flag = "--insecure" if not certs_dir else "--certs-dir={}".format(certs_dir)
    return "cockroach sql --execute='{}' --host=%s --port=%s %s --format=csv --set=show_times=false" % (host, port, secure_flag)

def executeCommand(module, command):
    (rc, stdout, stderr) = module.run_command(command)
    if rc != 0:
        module.fail_json(msg="An error occurred: {}. Tried to execute '{}'".format(stderr, command))
    return stdout

def createDatabase(module, name, host, port, certs_dir):
    creation_query = 'CREATE DATABASE IF NOT EXISTS "{}";'.format(name)
    command = prepareCommandFormat(host, port, certs_dir).format(creation_query)
    executeCommand(module, command)

def deleteDatabase(module, name, host, port, certs_dir):
    deletion_query = 'DROP DATABASE IF EXISTS "{}" CASCADE;'.format(name)
    command = prepareCommandFormat(host, port, certs_dir).format(deletion_query)
    executeCommand(module, command)

def main():
    module = AnsibleModule(
        argument_spec = dict(
            name = dict(required = True, type = 'str'),
            state = dict(choices = ['present', 'absent'], required = False, default = 'present', type = 'str'),
            host = dict(required = False, default = 'localhost', type = 'str'),
            port = dict(required = False, default = '26257', type = 'str'),
            certs_dir = dict(required = False, default = None, type = 'str')
        ),
        supports_check_mode = True
    )

    result = dict(
        changed=False,
        msg=''
    )

    if module.check_mode:
        module.exit_json(**result)

    name = module.params['name']
    state = module.params['state']
    host = module.params['host']
    port = module.params['port']
    certs_dir = module.params['certs_dir']

    result['changed'] = True

    if state == 'absent':
        deleteDatabase(module, name, host, port, certs_dir)
        result['msg'] = "Database '{}' was successfully deleted from cockroach".format(name)
    elif state == 'present':
        createDatabase(module, name, host, port, certs_dir)
        result['msg'] = "Database '{}' was successfully created in cockroach".format(name)

    module.exit_json(**result)


if __name__ == '__main__':
    main()