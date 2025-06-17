To enable TLS run playbook:

cd ansible
ansible-playbook -Ki inv.yaml -e "tls=on" minio_tls.yaml


To disable TLS run playbook:

cd ansible
ansible-playbook -Ki inv.yaml -e "tls=off" minio_tls.yaml
