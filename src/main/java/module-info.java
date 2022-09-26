module InfluxDbSqlDriver {
	requires java.sql;
	requires lombok;
	requires influxdb.java;
	provides java.sql.Driver with net.suteren.jdbc.influxdb.InfluxDbDriver;
}