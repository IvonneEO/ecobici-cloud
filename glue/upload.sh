#!/usr/bin/env bash
# Sube el script del Job de Glue y el paquete ecobici (zip importable) al bucket de datos.
# Correr después de cada `sam deploy` que cambie código usado por el job.
set -euo pipefail
cd "$(dirname "$0")/.."

STACK="${STACK:-ecobici-cloud}"
BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='BucketName'].OutputValue" --output text)

rm -rf dist && mkdir -p dist
(cd src && zip -qr ../dist/ecobici.zip ecobici -x "*/__pycache__/*")
aws s3 cp glue/curate_job.py "s3://$BUCKET/glue/curate_job.py"
aws s3 cp dist/ecobici.zip "s3://$BUCKET/glue/libs/ecobici.zip"
echo "Listo: script y paquete en s3://$BUCKET/glue/"
