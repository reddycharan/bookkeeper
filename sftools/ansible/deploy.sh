#!/bin/sh
#
ansible-playbook installBits.yml -v -k --extra-vars="ansible_become_pass=sfdc123 package_name=$1" || exit 1
ansible-playbook startBK.yml -v -f 1 -k --extra-vars="ansible_become_pass=sfdc123" || exit 1
ansible-playbook startSFMS.yml -v -k --extra-vars="ansible_become_pass=sfdc123" || exit 1
