# dbatcher

[![Build Status](https://app.travis-ci.com/edwvee/dbatcher.svg?branch=main)](https://app.travis-ci.com/edwvee/dbatcher)
[![Go Report Card](https://goreportcard.com/badge/github.com/edwvee/dbatcher)](https://goreportcard.com/report/github.com/edwvee/dbatcher)
[![codecov](https://codecov.io/gh/edwvee/dbatcher/branch/main/graph/badge.svg)](https://codecov.io/gh/edwvee/dbatcher/)

### NOT YET READY

A server for batching single inserts to databases. Gather many single or just small inserts to your DMBS and send them in batches with **dbatcher**. That could lower DBMS' load, make less latency and is viable for ClickHouse. The goal of this project is low latency and simplicity to use: you need only HTTP client and JSON libraries.

### Supports
- ClickHouse
- MySQL

## Instalation and setup
1. Install go
2. `go get github.com/edwvee/dbatcher/cmd/dbatcher`
3. `go build github.com/edwvee/dbatcher/cmd/dbatcher`
4. Write config and place it in `config.toml`
5. `./dbatcher config.toml`

### Config example
```toml
#address for pprof http (https://pkg.go.dev/runtime/pprof)
#remove if you won't profile
pprof_http_bind = "localhost:6034"

#log for insert errors (not for sync=1 requests)
#format: {"timestamp":..., "timestamp_string":..., "error": ..., "table":..., "fields":..., "rows": ...}\n
#remove or leave empty path if not needed
[insert_error_logger]
    path = "error.log"
    pretty_print = true

[receivers]

    [receivers.first-http]
        type = "http"
        #bind address
        bind = ":8124"

[inserters]

    #first-clickhouse is a name
    [inserters.first-clickhouse]
        #use this type for clickhouse
        type = "clickhouse"
        #connection string (look here https://github.com/ClickHouse/clickhouse-go#dsn)
        #use native tcp interface, not http or mysql
        dsn = "tcp://localhost:9000?user=default"
        #maximum simultaneous connections (treat like maximum simultaneous queries)
        max_connections = 2
        insert_timeout_ms = 30000

    [inserters.second-mysql]
        #use this type for mysql
        type = "mysql"
        #connection string (look here https://github.com/go-sql-driver/mysql#dsn-data-source-name)
        dsn = "user:password@tcp(hostname)/db_name?charset=utf8mb4,utf8"
        #maximum simultaneous connections (treat like maximum simultaneous queries)
        max_connections = 2
        insert_timeout_ms = 30000

    [inserters.third-dummy]
        #dummy inserter only reports about inserts
        type = "dummy"
```

## HTTP interface
**Type**: `POST`

**URL**: `/`

**Query parameters**:
- `table` (string) - table name. Could be with database. Use backticks (\`) here if database or table name should be encoded. Database could be infered from DSN (db connection string). Examples : `my_table`, `database.my_table`, `` `database`.`my_table` ``
- `fields` (string) -  comma separated column names that match columns in rows to pass. Spaces are ignored. Use backticks if column name should be escaped. Example: `` field1,field2,`table`, field4 ``
- `sync` (0 or 1) - insert rows right away. Mostly debug feature. Parameters bellow are ignored if `sync` is set to 1
- `timeout_ms` (uint > 0) - timeout before data insertion in milliseconds. Updates for table inside **dbatcher** after insertion
- `max_rows` (uint > 0) - maximum rows number before insert

**Body**: rows in JSON format. Should be array of arrays. Column order should match `fields`. For correct type representation see the tables below.

**Response**: success - code 200, empty body; fail - non 200 code, body with an error message as a plain text.

Insertion to database happens when `sync` is 1 (only for requests data), after timeout is came or after row count for table reached `max_rows` (not in request time, async).

**Example**:

`table`: db.\`table\`

`fields`: string_field,int_field

`timeout_ms`: 10000

`max_rows`: 10000

Full URL: `http://127.0.0.1:8124/?table=db.%60table%60&fields=string_field%2Cint_field&timeout_ms=10000&max_rows=10000`

Body:
`[[\"foo\",123],[\"bar\",321]]`

## ClickHouse - JSON types compatibility

|                    | string               | number              | int/uint as string  |
|--------------------|----------------------|---------------------|---------------------|
| UInt8/16/32/64     |                      | +                   | +                   |
| Int8/16/32/64      |                      | +                   | +                   |
| Float32/64         |                      | +                   |                     |
| String/FixedString | +                    |                     |                     |
| Date               | yyyy-mm-dd           | unix time (seconds) | unix time (seconds) |
| DateTime           | yyyy-mm-dd H:i:s     | unix time (seconds) | unix time (seconds) |
| DateTime64         | yyyy-mm-dd H:i:s.XXX |                     |                     |
| Enum8/16           | +                    | +                   |                     |

## MySQL - JSON types compatibility

|                                      | string           | number | int/uint/float as string |
|--------------------------------------|------------------|--------|--------------------------|
| TINYINT/SMALLINT/INT/BIGINT UNSIGNED | -                | +      | +                        |
| TINYINT/SMALLINT/INT/BIGINT          |                  | +      | +                        |
| FLOAT/DOUBLE                         |                  | +      | +                        |
| CHAR/BINARY/VARCHAR/TEXT             | +                |        |                          |
| DATE                                 | yyyy-mm-dd       |        |                          |
| DATETIME/TIMESTAMP                   | yyyy-mm-dd H:i:s |        |                          |
| ENUM                                 | +                | +      |                          |

1. For MySQL **dbatcher** uses `INSERT IGNORE`.
2. Those types and formats are tested. Others could work too (see MySQL documentation). Open an issue to add official support.
