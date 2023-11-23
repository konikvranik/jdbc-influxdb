package net.suteren.jdbc;

import java.io.IOException;
import java.util.Properties;

import com.vdurmont.semver4j.Semver;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Version {

	private static final Version VERSION = new Version();
	private final Semver version;

	public static Version getInstance() {
		return VERSION;
	}

	public static Semver getVersion() {
		return getInstance().version;
	}

	private Version() {
		version = new Semver(loadVersionFromProperty());
	}

	@NonNull private String loadVersionFromProperty() {
		Properties properties = new Properties();
		try {
			properties.load(getClass().getResourceAsStream("/version.properties"));
			return (String) properties.get("version");
		} catch (IOException e) {
			log.error("Can not load version properties: ".concat(e.getMessage()));
		}
		return "";
	}
}
