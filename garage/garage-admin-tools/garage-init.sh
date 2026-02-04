#!/bin/sh

set -e

# fail-fast if env vars hasn't been set
: "${GARAGE_ADMIN_TOKEN:?GARAGE_ADMIN_TOKEN is not set}"

# Get current layout version
LAYOUT_VERSION=$(curl -sf "$GARAGE_ADMIN_API/v2/GetClusterLayout" \
    -H "Authorization: Bearer $GARAGE_ADMIN_TOKEN" |
    jq -r '.version')

# Only run if layout version is 0
if [ "$LAYOUT_VERSION" -ne "0" ]; then
    echo "Garage layout already initialized. Skipping."
    exit 0
fi

echo "Garage layout not initialized. Proceeding..."

# Get node id
NODE_ID=""
RETRIES=12
SLEEP=5
i=0

while [ -z "$NODE_ID" ] || [ "$NODE_ID" = "null" ]; do
    NODE_ID=$(curl -sf "$GARAGE_ADMIN_API/v2/GetClusterStatus" \
        -H "Authorization: Bearer $GARAGE_ADMIN_TOKEN" |
        jq -r '.nodes[0].id') || true

    if [ -n "$NODE_ID" ] && [ "$NODE_ID" != "null" ]; then
        break
    fi

    i=$((i + 1))
    if [ "$i" -ge "$RETRIES" ]; then
        echo "NODE_ID not found after $((RETRIES * SLEEP)) seconds. Exiting."
        exit 1
    fi

    echo "Waiting for Garage node to be ready..."
    sleep $SLEEP
done
if [ -z "$NODE_ID" ] || [ "$NODE_ID" = "null" ]; then
    echo "Failed to get NODE_ID from cluster. Exiting."
    exit 1
fi

# Create single node layout
echo "initializing single node layout..."
RESPONSE=$(
    curl -s -w "\n%{http_code}" -X POST "$GARAGE_ADMIN_API/v2/UpdateClusterLayout" \
        -H "Authorization: Bearer $GARAGE_ADMIN_TOKEN" \
        -H "Content-Type: application/json" \
        -d @- <<EOF
{
    "roles": [
        {
            "id": "$NODE_ID",
            "zone": "dc1",
            "capacity": $GARAGE_NODE_CAPACITY,
            "tags": []
        }
    ]
}
EOF
)

HTTP_STATUS=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_STATUS" -ne 200 ]; then
    echo "Failed to initialize single node layout. HTTP Status: $HTTP_STATUS"
    echo "Response: $BODY"
    exit 1
fi

echo "Succesfully staged single node layout. Applying layout..."

# Apply layout
RESPONSE=$(
    curl -s -w "\n%{http_code}" -X POST "$GARAGE_ADMIN_API/v2/ApplyClusterLayout" \
        -H "Authorization: Bearer $GARAGE_ADMIN_TOKEN" \
        -H "Content-Type: application/json" \
        -d @- <<EOF
{
    "version": 1
}
EOF
)

HTTP_STATUS=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_STATUS" -ne 200 ]; then
    echo "Failed to apply layout changes. HTTP Status: $HTTP_STATUS"
    echo "Response: $BODY"
    exit 1
fi

echo "Succefully initialized single node layout."

# Create access key
echo "Creating access key..."

RESPONSE=$(
    curl -s -w "\n%{http_code}" -X POST "$GARAGE_ADMIN_API/v2/CreateKey" \
        -H "Authorization: Bearer $GARAGE_ADMIN_TOKEN" \
        -H "Content-Type: application/json" \
        -d @- <<EOF
{
    "name": "airflow_key",
    "neverExpires": true
}
EOF
)

HTTP_STATUS=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_STATUS" -ne 200 ]; then
    echo "Failed to create access key. HTTP Status: $HTTP_STATUS"
    echo "Response: $BODY"
    exit 1
fi

GARAGE_ENV_PATH=/mnt/garage/.env.garage

touch $GARAGE_ENV_PATH

ACCESS_KEY_ID=$(echo "$BODY" | jq -r ".accessKeyId")
SECRET_ACCESS_KEY=$(echo "$BODY" | jq -r ".secretAccessKey")

# Write to .env.garage
echo "AWS_ACCESS_KEY_ID=$ACCESS_KEY_ID" >"$GARAGE_ENV_PATH"
echo "AWS_SECRET_ACCESS_KEY=$SECRET_ACCESS_KEY" >>"$GARAGE_ENV_PATH"

echo "Succefully created access key."
echo "Access key id and secret access key written to ./garage/.env.garage"

# Create bucket
echo "Creating garage bucket..."

RESPONSE=$(
    curl -s -w "\n%{http_code}" -X POST "$GARAGE_ADMIN_API/v2/CreateBucket" \
        -H "Authorization: Bearer $GARAGE_ADMIN_TOKEN" \
        -H "Content-Type: application/json" \
        -d @- <<EOF
{
    "globalAlias": "$BUCKET_NAME"
}
EOF
)

HTTP_STATUS=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_STATUS" -ne 200 ]; then
    echo "Failed to create bucket. HTTP Status: $HTTP_STATUS"
    echo "Response: $BODY"
    exit 1
fi

BUCKET_ID=$(echo "$BODY" | jq -r ".id")

echo "Succesfully created bucket with local alias: $BUCKET_NAME."

# Give bucket permissions to access key
echo "Giving bucket permissions to access key..."

RESPONSE=$(
    curl -s -w "\n%{http_code}" -X POST "$GARAGE_ADMIN_API/v2/AllowBucketKey" \
        -H "Authorization: Bearer $GARAGE_ADMIN_TOKEN" \
        -H "Content-Type: application/json" \
        -d @- <<EOF
{
    "accessKeyId": "$ACCESS_KEY_ID",
    "bucketId": "$BUCKET_ID",
    "permissions": {
        "owner": true,
        "read": true,
        "write": true
    } 
}
EOF
)

HTTP_STATUS=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_STATUS" -ne 200 ]; then
    echo "Failed to give bucket permissions to access key. HTTP Status: $HTTP_STATUS"
    echo "Response: $BODY"
    exit 1
fi

echo "Succesfully given bucket permissions to access key."
