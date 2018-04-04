pg_retire
=========

**NOTE: This is a personal experimental program.**

pg_retire is simplified query canceler for PostgreSQL.

Overview
--------

pg_retire module runs in normal backend process. pg_retire watches whether
the client is alive by sending ParameterStatus message periodically.
If the client process is down, pg_retire cancels running transaction
by sending SIGINT signal to the backend.

Parameters
----------

- pg_retire.enable
Specifies a bool value to enable pg_retire. Default value is false.


- pg_retire.interval (sec)
Specifies how long interval pg_retire watches the client. Default value is 10.

How to install pg_retire
------------------------

```
$ cd pg_regire
$ make USE_PGXS=1 
$ sudo make USE_PGXS=1 install
```

How to set up pg_retire
-----------------------

```
$ vim postgresql.conf
shared_preload_libraries = 'pg_retire'
pg_retire.enable   = true
pg_retire.interval = 10
```

Simple Test
-----------

```
$ psql <<EOF &
SET pg_retire.enable = on;
WITH RECURSIVE t(n) AS (
VALUES (1)
UNION ALL
SELECT n+1 FROM t WHERE n < 10000000
)
SELECT sum(n) FROM t;
EOF
$ PROC=$!
$ kill $PROC
```
