#!/bin/bash

# MinIO TLS management script
# Usage: ./minio_tls.sh --enable | --disable

set -e

MINIO_USER=minio-user
MINIO_CONF="/etc/default/minio"
CERTS_DIR="/home/$MINIO_USER/.minio/certs"

case "$1" in
  --enable)
    echo "Enabling TLS for MinIO..."

    mkdir -p "$CERTS_DIR"
    cd "$CERTS_DIR"

    wget -O certgen https://github.com/minio/certgen/releases/latest/download/certgen-linux-amd64
    chmod +x certgen

    ./certgen -host "localhost,storage-san-test-*,10.200.0.*,10.210.0.*"

    sed -i 's/http:/https:/g' "$MINIO_CONF"

    echo "TLS has been enabled. You may want to restart MinIO."
    ;;

  --disable)
    echo "Disabling TLS for MinIO..."

    sed -i 's/https:/http:/g' "$MINIO_CONF"
    rm -rf "$CERTS_DIR"

    echo "TLS has been disabled. You may want to restart MinIO."
    ;;

  *)
    echo "Usage: $0 --enable | --disable"
    exit 1
    ;;
esac

systemctl restart minio
systemctl status minio


