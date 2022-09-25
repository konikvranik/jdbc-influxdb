package net.suteren.jdbc.influxdb;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
			AtomicReference<String> database = new AtomicReference<>();
			if (info != null) {
				username = info.getProperty("username");
				if (username == null) {
					username = info.getProperty("user");
				}
				password = info.getProperty("password");
				database.set(info.getProperty("database"));
			}
			try {
				Map<String, Set<String>> params = Arrays.stream(new URI(url).getQuery().split("&"))
					.map(x -> x.split("=", 2))
					.collect(Collectors.groupingBy(x -> x[0], Collectors.mapping(x -> x[1], Collectors.toSet())));
				params.get("db").forEach(database::set);
			} catch (URISyntaxException e) {
				throw new SQLException(String.format("Invalid URL %s", url), e);
			}
			return new InfluxDbConnection(url.matches("^https?://.*$") ? url : "http://" + url,
				username, password, database.get(), this);
		} else {
			throw new java.sql.SQLException(String.format("Invalid URL %s", url));
		}
	}

	@Override public boolean acceptsURL(String url) {
		return url != null && url.startsWith("jdbc:inbluxdb:");
	}

	@Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
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
