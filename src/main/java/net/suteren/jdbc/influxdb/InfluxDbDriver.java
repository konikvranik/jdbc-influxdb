package net.suteren.jdbc.influxdb;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.extern.java.Log;

@Log
public class InfluxDbDriver implements java.sql.Driver {
	@Override public InfluxDbConnection connect(String url, Properties info) throws SQLException {
		Pattern p = Pattern.compile("jdbc:influxdb:(.*)");
		Matcher m = p.matcher(url);
		if (m.matches()) {
			url = m.group(1);
			String username = null;
			String password = null;
			if (info != null) {
				username = info.getProperty("username");
				if (username == null) {
					username = info.getProperty("user");
				}
				password = info.getProperty("password");
			}
			return new InfluxDbConnection(url.matches("^https?://.*$") ? url : "http://" + url,
				username, password, this);
		} else {
			throw new java.sql.SQLException(String.format("Invalid URL %s", url));
		}
	}

	@Override public boolean acceptsURL(String url) throws SQLException {
		return url != null && url.startsWith("jdbc:inbluxdb:");
	}

	@Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return new DriverPropertyInfo[0];
	}

	@Override public int getMajorVersion() {
		return 1;
	}

	@Override public int getMinorVersion() {
		return 0;
	}

	@Override public boolean jdbcCompliant() {
		return false;
	}

	@Override public Logger getParentLogger() {
		return log;
	}
}
