# Useful Ansible Modules

A collection of ansible library code that can be used when interacting with hosts.

## CockroachDB modules

Ansible modules for interacting with [CockroachDB](https://www.cockroachlabs.com/), including creating databases and users, adding and revoking grants, checking cluster health.

### Usage

Within an ansible folder structure:

```.
├── callback_plugins
├── environment_vars
├── includes
├── inventories
├── library
├── playbooks
└── roles
```

Add the module to the library folder and adjust your `ansible.cfg` to include something similar to this:

`library=../../library`

Examples of usage are included in the doc comments in each module