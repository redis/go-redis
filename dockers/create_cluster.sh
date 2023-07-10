#! /bin/bash

mkdir -p /nodes
touch /nodes/nodemap
if [ -z ${START_PORT} ]; then
    START_PORT=16379
fi
if [ -z ${END_PORT} ]; then
    END_PORT=16384
fi
if [ ! -z "$3" ]; then
    START_PORT=$2
    START_PORT=$3
fi
echo "STARTING: ${START_PORT}"
echo "ENDING: ${END_PORT}"

for PORT in `seq ${START_PORT} ${END_PORT}`; do
  mkdir -p /nodes/$PORT
  if [[ -e /redis.conf ]]; then
    cp /redis.conf /nodes/$PORT/redis.conf
  else
    touch /nodes/$PORT/redis.conf
  fi
  cat << EOF >> /nodes/$PORT/redis.conf
port ${PORT}
cluster-enabled yes
daemonize yes
logfile /redis.log
dir /nodes/$PORT
EOF

  set -x
  /opt/redis-stack/bin/redis-server /nodes/$PORT/redis.conf
  sleep 1
  if [ $? -ne 0 ]; then
    echo "Redis failed to start, exiting."
    continue
  fi
  echo 127.0.0.1:$PORT >> /nodes/nodemap
done
if [ -z "${REDIS_PASSWORD}" ]; then
    echo yes | /opt/redis-stack/bin/redis-cli --cluster create `seq -f 127.0.0.1:%g ${START_PORT} ${END_PORT}` --cluster-replicas 1
else
    echo yes | opt/redis-stack/bin/redis-cli -a ${REDIS_PASSWORD} --cluster create `seq -f 127.0.0.1:%g ${START_PORT} ${END_PORT}` --cluster-replicas 1
fi
tail -f /redis.log
