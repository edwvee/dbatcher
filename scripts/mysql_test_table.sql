CREATE DATABASE IF NOT EXISTS db_name CHARACTER SET utf8mb4;
DROP TABLE IF EXISTS db_name.dbatcher_test_table;
CREATE TABLE db_name.dbatcher_test_table(
	uTinyIntNumber TINYINT UNSIGNED,
	uSmallIntNumber SMALLINT UNSIGNED,
	uIntNumber INT UNSIGNED,
	uBigIntNumber BIGINT UNSIGNED,
	
	tinyIntNumber TINYINT,
	smallIntNumber SMALLINT,
	intNumber INT,
	bigIntNumber BIGINT,
	
	floatNumber FLOAT,
	doubleNumber DOUBLE,
	
	uTinyIntString TINYINT UNSIGNED,
	uSmallIntString SMALLINT UNSIGNED,
	uIntString INT UNSIGNED,
	uBigIntString BIGINT UNSIGNED,
	
	tinyIntString TINYINT,
	smallIntString SMALLINT,
	intString INT,
	bigIntString BIGINT,
	
	floatString FLOAT,
	doubleString DOUBLE,
	
	dateString DATE,
	dateTimeString DATETIME,
	timestampString TIMESTAMP,
	
	char32String CHAR(32),
	binary8String BINARY(8),
	varchar255String VARCHAR(255),
	textString TEXT,
	enumString ENUM('ASD', 'ZXC'),
	enumNumber ENUM('ASD', 'ZXC')
) Engine=InnoDB