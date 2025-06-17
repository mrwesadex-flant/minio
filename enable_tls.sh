#!/bin/bash

# Minio part

MINIO_USER=minio-user

mkdir -p ~$MINIO_USER/.minio/certs
cd ~$MINIO_USER/.minio/certs
wget https://github.com/minio/certgen/releases/latest/download/certgen-linux-amd64 -o certgen
chmod +x certgen
./certgen -host "localhost,storage-san-test-*,10.200.0.*"

sed -i 's/http:/https:/g' /etc/default/minio
# systemctl restart minio

