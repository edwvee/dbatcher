# dbatcher

[![Build Status](https://app.travis-ci.com/edwvee/dbatcher.svg?branch=main)](https://app.travis-ci.com/edwvee/dbatcher)
[![Go Report Card](https://goreportcard.com/badge/github.com/edwvee/dbatcher)](https://goreportcard.com/report/github.com/edwvee/dbatcher)
[![codecov](https://codecov.io/gh/edwvee/dbatcher/branch/main/graph/badge.svg)](https://codecov.io/gh/edwvee/dbatcher/)

a server for batching single requests to databases

# NOT YET READY

### ClickHouse - JSON types compatibility

|                    | string               | number              | int/uint as string  |
|--------------------|----------------------|---------------------|---------------------|
| UInt8/16/32/64     | -                    | +                   | +                   |
| Int8/16/32/64      |                      | +                   | +                   |
| Float32/64         |                      | +                   |                     |
| String/FixedString | +                    |                     |                     |
| Date               | yyyy-mm-dd           | unix time (seconds) | unix time (seconds) |
| DateTime           | yyyy-mm-dd h:i:s     | unix time (seconds) | unix time (seconds) |
| DateTime64         | yyyy-mm-dd h:i:s.XXX |                     |                     |
| Enum8/16           | +                    | +                   |                     |
