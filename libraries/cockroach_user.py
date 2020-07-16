#!/usr/bin/python
# -*- coding: utf-8 -*-

ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'status': ['preview'],
    'supported_by': 'community'
}

DOCUMENTATION = '''
---
module: cockroach_user
short_description: Manages cockroach users
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
# Create a user
- name: Create a user
  cockroach_user:
    name: billyjoel
    password: super_secret_password

# Create an admin user
- name: Create an admin
  cockroach_user:
    name: billyjoel
    password: super_secret_password
    admin: True

- name: Create a user with grants
  cockroach_user:
    name: billyjoel
    password: super_secret_password
    grants:
      users: ALL
      items: SELECT

# Delete a user
- name: Delete a user
  cockroach_user:
    name: billyjoel
    state: absent

# Create a user on a secure cluster
- name: Create a user
  cockroach_user:
    name: billyjoel
    password: super_secret_password
    certs_dir: /opt/cockroachdb/certs

# Create a user from a node not running CockroachDB
- name: Create a user
  cockroach_user:
    name: billyjoel
    password: super_secret_password
    host: cockroach-service.foo.bar
'''
RETURN = ''
IGNORED_DATABASES = ['system']

from ansible.module_utils.basic import AnsibleModule

def prepareCommandFormat(host, port, certs_dir):
    secure_flag = "--insecure" if not certs_dir else "--certs-dir={}".format(certs_dir)
    return """cockroach sql --execute="{}" --host=%s --port=%s %s --format=csv --set=show_times=false""" % (host, port, secure_flag)

def executeCommand(module, command, can_fail=False):
    (rc, stdout, stderr) = module.run_command(command)
    if rc != 0 and not can_fail:
        module.fail_json(msg="An error occurred: {}. Tried to execute '{}'".format(stderr, command))
    return stdout

def listDatabases(module, host, port, certs_dir):
    list_databases_query = """SHOW DATABASES;"""
    command = prepareCommandFormat(host, port, certs_dir).format(list_databases_query)
    stdout = executeCommand(module, command)
    blacklist_values = IGNORED_DATABASES + ['database_name']
    return [d for d in stdout.strip().split("\n") if d not in blacklist_values]

def listTables(module, database, host, port, certs_dir):
    grant_db_query = """SHOW TABLES FROM {};""".format(database)
    command = prepareCommandFormat(host, port, certs_dir).format(grant_db_query)
    stdout = executeCommand(module, command)
    return filter(lambda i: i != "table_name", stdout.strip().split("\n"))

def grantUserAccessToDatabase(module, name, database, grant, host, port, certs_dir):
    grant_db_query = """GRANT {} ON DATABASE {} TO {};""".format(grant, database, name)
    command = prepareCommandFormat(host, port, certs_dir).format(grant_db_query)
    executeCommand(module, command)

def grantUserAccessToDatabaseTables(module, name, database, grant, host, port, certs_dir):
    grant_tables_query = """GRANT {} ON TABLE {}.* TO {};""".format(grant, database, name)
    command = prepareCommandFormat(host, port, certs_dir).format(grant_tables_query)
    executeCommand(module, command)

def setUserGrants(module, name, grants, host, port, certs_dir):
    for database, grant in {k: v for d in grants for k, v in d.items()}.items():
        grantUserAccessToDatabase(module, name, database, grant, host, port, certs_dir)
        if len(listTables(module, database, host, port, certs_dir)) > 0:
            grantUserAccessToDatabaseTables(module, name, database, grant, host, port, certs_dir)

def revokeUserAccessFromAllDatabases(module, name, host, port, certs_dir):
    databases = listDatabases(module, host, port, certs_dir)
    for database in databases:
        revoke_query = """REVOKE ALL ON DATABASE {} FROM {};""".format(database, name)
        command = prepareCommandFormat(host, port, certs_dir).format(revoke_query)
        executeCommand(module, command, can_fail=True)

def revokeUserAccessFromAllTables(module, name, host, port, certs_dir):
    databases = listDatabases(module, host, port, certs_dir)
    for database in databases:
        revoke_query = """REVOKE ALL ON TABLE {}.* FROM {};""".format(database, name)
        command = prepareCommandFormat(host, port, certs_dir).format(revoke_query)
        executeCommand(module, command, can_fail=True)

def deleteUser(module, name, host, port, certs_dir):
    revokeUserAccessFromAllTables(module, name, host, port, certs_dir)
    revokeUserAccessFromAllDatabases(module, name, host, port, certs_dir)
    deletion_query = """DROP USER IF EXISTS {};""".format(name)
    command = prepareCommandFormat(host, port, certs_dir).format(deletion_query)
    executeCommand(module, command)

def createUser(module, name, host, port, certs_dir):
    creation_query = """CREATE USER IF NOT EXISTS {};""".format(name)
    command = prepareCommandFormat(host, port, certs_dir).format(creation_query)
    executeCommand(module, command)

def setUserPassword(module, name, password, host, port, certs_dir):
    password_query = """ALTER USER {} WITH PASSWORD \\"{}\\";""".format(name, password)
    command = prepareCommandFormat(host, port, certs_dir).format(password_query)
    executeCommand(module, command)

def setUserAdminRights(module, name, has_admin_rights, host, port, certs_dir):
    if has_admin_rights:
        rights_query = """UPSERT INTO system.role_members (role, member, \\"isAdmin\\") VALUES ('admin', '{}', true);""".format(name)
        command = prepareCommandFormat(host, port, certs_dir).format(rights_query)
        executeCommand(module, command)
    else:
        rights_query = """DELETE FROM system.role_members WHERE member='{}';""".format(name)
        command = prepareCommandFormat(host, port, certs_dir).format(rights_query)
    executeCommand(module, command)


def main():
    module = AnsibleModule(
        argument_spec=dict(
            name=dict(required=True, type='str'),
            password=dict(required=False, type='str', no_log=True, default=None),
            admin=dict(required=False, type='bool', default=False),
            grants=dict(required=False, type='list', default=[]),
            state=dict(choices=['present', 'absent'], required=False, default='present', type='str'),
            host=dict(required=False, default='localhost', type='str'),
            port=dict(required=False, default='26257', type='str'),
            certs_dir=dict(required=False, default=None, type='str')
        ),
        supports_check_mode=True
    )

    result = dict(
        changed=False,
        msg=''
    )

    if module.check_mode:
        module.exit_json(**result)

    name = module.params['name']
    password = module.params['password']
    admin = module.params['admin']
    grants = module.params['grants']
    state = module.params['state']
    host = module.params['host']
    port = module.params['port']
    certs_dir = module.params['certs_dir']

    if state == 'absent':
        deleteUser(module, name, host, port, certs_dir)
        result = dict(
            changed = True,
            msg = "User '{}' was successfully deleted from cockroach".format(name)
        )
    elif state == 'present':
        if password == None:
            module.fail_json(msg="You must provide a password when creating a user")
        if " " in name:
            module.fail_json(msg="Username cannot contain spaces")

        createUser(module, name, host, port, certs_dir)
        setUserPassword(module, name, password, host, port, certs_dir)
        setUserAdminRights(module, name, admin, host, port, certs_dir)
        setUserGrants(module, name, grants, host, port, certs_dir)

        result['changed'] = True
        if admin:
            result['msg'] = "User '{}' was successfully created with admin rights".format(name)
        else:
            result['msg'] = "User '{}' was successfully created".format(name)

    module.exit_json(**result)

if __name__ == '__main__':
    main()