DROP TABLE IF EXISTS default.dbatcher_test_table;
CREATE TABLE default.dbatcher_test_table
(
    `uint8Number` UInt8,
    `uint16Number` UInt16,
    `uint32Number` UInt32,
    `uint64Number` UInt64,
    `int8Number` Int8,
    `int16Number` Int16,
    `int32Number` Int32,
    `int64Number` Int64,
    `uint8String` UInt8,
    `uint16String` UInt16,
    `uint32String` UInt32,
    `uint64String` UInt64,
    `int8String` Int8,
    `int16String` Int16,
    `int32String` Int32,
    `int64String` Int64,
    `float32Number` Float32,
    `float64Number` Float64,
    `stringString` String,
    `stringFStrinF` FixedString(16),
    `dateNumber` Date,
    `dateTimeNumber` DateTime,
    `dateString` Date,
    `dateTimeString` DateTime,
    `dateTime64String` DateTime64(3),
    `enum8Number` Enum8('a' = 1, 'b' = 2),
    `enum16Number` Enum16('a' = 1, 'b' = 2),
    `enum8String` Enum8('a' = 1, 'b' = 2),
    `enum16String` Enum16('a' = 1, 'b' = 2)
)
ENGINE = MergeTree
ORDER BY dateNumber
SETTINGS index_granularity = 8192;