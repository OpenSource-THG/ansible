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
module: openstack_secret
short_description: Manages openstack secrets
version_added: "2.9.2"
author:
    - Josh Blease
description:
    - "Allows the creation, modification and deletion or secrets stored within openstack. "
requirements:
    - "python >= 2.7"
    - "openstacksdk"
options:
    name:
        description:
            - This is the name of the secret to be modified
        required: true
    state:
        description:
            - Define whether the secret with the specified name should exist or not
        required: false
        default: present
        choices: [ present, absent ]
    cloud:
        description:
            - The name of the OpenStack cloud to interact with
        required: false
        default: openstack
    value:
        description:
            - The value of the secret
        required: false
    overwrite_mode:
        description:
            - If a secret already exists with the same key it can be overwritten, fail and throw an error or ignore the new data and progress on
        required: false
        default: ignore
        choices: [ force, fail, ignore ]
extends_documentation_fragment: openstack
'''

EXAMPLES = '''
# Create a secret
- name: Create a secret
  openstack_secret:
    name: super_secret_name
    value: super_secret_password_data

# Read a secret
- name: Read a secret
  openstack_secret:
    name: super_secret_name

# Overwrite a secret
- name: Overwrite a secret
  openstack_secret:
    name: existing_secret_name
    value: new_secret_data
    overwrite_mode: force

# Create a secret if it doesn't exist, otherwise fail
- name: Overwrite a secret
  openstack_secret:
    name: existing_secret_name
    value: new_secret_data
    overwrite_mode: fail

# Delete a secret
- name: Delete a secret
  openstack_secret:
    name: super_secret_name
    state: absent
'''

RETURN = '''
secret_value:
    description: The secret value
    type: str
    returned: always
'''

from ansible.module_utils.basic import AnsibleModule
import openstack

def createSecret(conn, secret_key, secret_value, secret_keys = None):
    secrets = secrets or getSecrets(conn)
    secret_keys = getSecretKeys(conn, secrets)
    if secret_key in secret_keys:
        raise Exception(results={'msg': 'A secret with key "{}" already exists.'.format(secret_key)})
    conn.key_manager.create_secret(
        name=secret_key,
        payload=secret_value,
        mode="cbc",
        payload_content_type="text/plain")

def modifySecret(conn, secret_key, secret_value, secrets = None):
    secrets = secrets or getSecrets(conn)
    found_secrets = filter(lambda s: s['id'] == secret_key, secrets)

    if len(found_secrets) == 0:
        raise Exception('A secret with key "{}" does not exist.'.format(secret_key))

    destroySecret(conn, secret_key, secrets)
    createSecret(conn, secret_key, secret_value)

def destroySecret(conn, secret_key, secrets = None):
    secrets = secrets or getSecrets(conn)
    found_secrets = filter(lambda s: s['id'] == secret_key, secrets)

    if len(found_secrets) == 0:
        return False

    for s in found_secrets:
        conn.key_manager.delete_secret(
            secret=s['link'].rsplit('/', 1)[-1])
    return True

def getSecretPayload(conn, secret_key, secrets = None):
    secrets = secrets or getSecrets(conn)
    found_secrets = filter(lambda s: s['id'] == secret_key, secrets)

    if len(found_secrets) == 0:
        raise Exception('A secret with key "{}" does not exist.'.format(secret_key))

    return conn.key_manager.get_secret(secret=found_secrets[0]['link'].rsplit('/', 1)[-1]).payload

def getSecretKeys(conn, secrets = None):
    secrets = secrets or getSecrets(conn)
    return filter(lambda s: s != None, [s['id'] for s in secrets])

def getSecrets(conn):
    return [{'id':s['name'], 'link':s['secret_ref']} for s in conn.key_manager.secrets()]

def main():
    module = AnsibleModule(
        argument_spec=dict(
            name=dict(required=True, type='str'),
            state=dict(choices=['present', 'absent'], required=False, default='present', type='str'),
            cloud=dict(required=False, default='openstack', type='str'),
            value=dict(required=False, default=None, no_log=True, type='str'),
            overwrite_mode=dict(choices=['force', 'fail', 'ignore'], required=False, default='ignore', type='str')
        ),
        supports_check_mode=True
    )

    result = dict(
        changed=False,
        secret_value=''
    )

    if module.check_mode:
        module.exit_json(**result)

    name = module.params['name']
    state = module.params['state']
    cloud = openstack.connect(cloud=module.params['cloud'])
    value = module.params['value']
    overwrite_mode = module.params['overwrite_mode']

    secrets = getSecrets(cloud)

    if state == 'absent':
        result['changed'] = destroySecret(cloud, name, secrets)
    elif state == 'present':
        if overwrite_mode == 'force':
            if value == None:
                raise Exception('Value for "{}" has not been provided.'.format(name))
            else:
                modifySecret(cloud, name, value, secrets)
                result['changed'] = True
                result['secret_value'] = value
        elif overwrite_mode == 'fail':
            if value == None:
                raise Exception('Value for "{}" has not been provided.'.format(name))
            else:
                createSecret(conn, name, value, secrets)
                result['changed'] = True
                result['secret_value'] = value
        elif overwrite_mode == 'ignore':
            if name in getSecretKeys(cloud, secrets):
                result['changed'] = False
                result['secret_value'] = getSecretPayload(cloud, name, secrets)
            else:
                if value == None:
                    raise Exception('Value for "{}" has not been provided and secret does not exist.'.format(name))
                else:
                    createSecret(conn, name, value, secrets)
                    result['changed'] = True
                    result['secret_value'] = value

    module.exit_json(**result)


if __name__ == '__main__':
    main()