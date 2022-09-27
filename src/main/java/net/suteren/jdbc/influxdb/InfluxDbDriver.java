package net.suteren.jdbc.influxdb;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import lombok.SneakyThrows;
import lombok.extern.java.Log;

@Log
public class InfluxDbDriver implements java.sql.Driver {

	public static final String USERNAME_PROPERTY = "username";
	public static final String PASSWORD_PROPERTY = "password";
	public static final String DATABASE_PROPERTY = "database";
	public static final String USER_PROPERTY = "user";

	@Override public InfluxDbConnection connect(String url, Properties info) throws SQLException {
		Pattern p = Pattern.compile("jdbc:influxdb:(.*)");
		Matcher m = p.matcher(url);
		if (m.matches()) {
			url = m.group(1);
			url = url.matches("^https?://.*$") ? url : "http://" + url;
			if (info == null) {
				info = parseUrlParams(url);
			} else {
				info.putAll(parseUrlParams(url));
			}
			return new InfluxDbConnection(url, info.getProperty(USERNAME_PROPERTY, info.getProperty(USER_PROPERTY)),
				info.getProperty(PASSWORD_PROPERTY), info.getProperty(DATABASE_PROPERTY), this);
		} else {
			throw new java.sql.SQLException(String.format("Invalid URL %s", url));
		}
	}

	@SneakyThrows private static Properties parseUrlParams(String url) {
		try {
			URL url1 = new URL(url);
			String[] ui = Optional.ofNullable(url1.getUserInfo()).map(u -> u.split(":", 2)).orElse(null);

			Map<String, String> properties = Arrays.stream(url1.getQuery().split("&"))
				.map(x -> x.split("=", 2))
				.collect(Collectors.groupingBy(x -> x[0], Collectors.mapping(x -> x[1], Collectors.joining(","))));
			if (ui != null && ui.length > 0 && StringUtils.isNotBlank(ui[0])) {
				properties.put(USERNAME_PROPERTY, ui[0]);
			}
			if (ui != null && ui.length > 1 && StringUtils.isNotBlank(ui[1])) {
				properties.put(PASSWORD_PROPERTY, ui[1]);
			}
			Properties properties1 = new Properties();
			properties1.putAll(properties);
			return properties1;
		} catch (MalformedURLException e) {
			throw new SQLException(String.format("Invalid URL %s", url), e);
		}
	}

	@Override public boolean acceptsURL(String url) {
		return url != null && url.startsWith("jdbc:influxdb:");
	}

	@Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
		Set<DriverPropertyInfo> propertyInfos = new HashSet<>();
		info.putAll(parseUrlParams(url));
		if (StringUtils.isBlank(info.getProperty("db"))) {
			propertyInfos.add(makePropertyInfo(DATABASE_PROPERTY, null,true,"Database name"));
		}
		if (StringUtils.isBlank(info.getProperty(USERNAME_PROPERTY)) && StringUtils.isBlank(info.getProperty(
			USER_PROPERTY))) {
			propertyInfos.add(makePropertyInfo(USERNAME_PROPERTY, null,false,"User name"));
		}
		if (StringUtils.isBlank(info.getProperty(PASSWORD_PROPERTY))) {
			propertyInfos.add(makePropertyInfo(PASSWORD_PROPERTY, null,false,"Password"));
		}
		return propertyInfos.toArray(DriverPropertyInfo[]::new);
	}

	private static DriverPropertyInfo makePropertyInfo(String databaseProperty, String value, boolean required,
		String description) {
		DriverPropertyInfo driverPropertyInfo = new DriverPropertyInfo(databaseProperty, value );
		driverPropertyInfo.required = required;
		driverPropertyInfo.description = description;
		return driverPropertyInfo;
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
