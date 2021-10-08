# dbatcher

[![Build Status](https://app.travis-ci.com/edwvee/dbatcher.svg?branch=main)](https://app.travis-ci.com/edwvee/dbatcher)
[![Go Report Card](https://goreportcard.com/badge/github.com/edwvee/dbatcher)](https://goreportcard.com/report/github.com/edwvee/dbatcher)
[![codecov](https://codecov.io/gh/edwvee/dbatcher/branch/main/graph/badge.svg)](https://codecov.io/gh/edwvee/dbatcher/)

a server for batching single requests to databases

# NOT YET READY

## HTTP interface
**Type**: `POST`

**URL**: `/`

**Query parameters**:
- `table` (string) - table name. Could be with database. Use backticks (\`) here if database or table name should be encoded. Database could be infered from DSN (db connection string). Examples : `my_table`, `database.my_table`, `` `database`.`my_table` ``
- `fields` (string) -  comma separated column names that match columns in rows to pass. Spaces are ignored. Use backticks if column name should be escaped. Example: `` field1,field2,`table`, field4 ``
- `sync` (0 or 1) - insert rows right away. Mostly debug feature. Parameters bellow are ignored if `sync` is set to 1
- `timeout_ms` (uint > 0) - timeout before data insertion in milliseconds. Updates for table inside `dbatcher` after insertion
- `max_rows` (uint > 0) - maximum rows number before insert

**Body**: rows in JSON format. Should be array of arrays. Column order should match `fields`. For correct type representation see the tables below.

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
| DateTime           | yyyy-mm-dd h:i:s     | unix time (seconds) | unix time (seconds) |
| DateTime64         | yyyy-mm-dd h:i:s.XXX |                     |                     |
| Enum8/16           | +                    | +                   |                     |
