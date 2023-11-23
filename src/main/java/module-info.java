module InfluxDbSqlDriver {
	requires java.sql;
	requires lombok;
	requires influxdb.java;
	requires org.apache.commons.lang3;
	requires semver4j;
	requires org.slf4j;
	provides java.sql.Driver with net.suteren.jdbc.influxdb.InfluxDbDriver;
}