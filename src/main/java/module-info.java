module InfluxDbSqlDriver {
	requires java.sql;
	requires lombok;
	requires influxdb.java;
	requires org.apache.commons.lang3;
	provides java.sql.Driver with net.suteren.jdbc.influxdb.InfluxDbDriver;
}