#!/usr/bin/env bash

TAG="__pg_retire_test__"
export PGDATABASE=postgres
export PGUSER=vagrant
export PGHOST=192.168.33.14
declare -a clients=()
CLIENT_NUM=10

#
# Show backend
#
function show_backend()
{
  echo "hosf"
  psql <<EOF
SET pg_retire.enable = off;
SELECT pid, usename, query, backend_start
FROM   pg_stat_activity
WHERE  pid <> pg_backend_pid() and query like '%$TAG%'
EOF
}


echo "==> Run $CLIENT_NUM queries"

for i in $(seq 1 $CLIENT_NUM); do
  PROC="$(
    psql <<EOF > /dev/null 2>&1 &
SET pg_retire.enable = true;
SET pg_retire.interval = 5;
WITH RECURSIVE t(n) AS (
  VALUES (1)
UNION ALL
  SELECT n+1 FROM t WHERE n < 100000000
)
SELECT sum(n), '$TAG' FROM t;
EOF
    echo $!
  )"
  clients[$i]="$PROC"
done

echo "==> waiting process startup"

sleep 3

{
  echo "==> kill processes"
  # Kill processes
  for i in $(seq 1 $CLIENT_NUM); do
    kill "${clients[$i]}"
  done

  echo "==> waiting for being canceled"
  # Check whether all boot queries have been canceled.
  while true; do
    result="$(show_backend)"
    if [[ "$result" =~ $TAG ]]; then
      printf "."
      sleep 1
      continue
    fi
    printf "done\n"
    break
  done
} 2> /dev/null
