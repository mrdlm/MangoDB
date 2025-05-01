package config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigManager {
    private Properties properties;

    public ConfigManager(final String propertyFileName) {
        properties = new Properties();

        try (final InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propertyFileName)) {

            if (inputStream == null) {
                System.err.println("Unable to find " + propertyFileName);
                throw new RuntimeException("Unable to find config");
            }

            properties.load(inputStream);

        } catch (IOException e) {
            System.err.println("Error loading properties file " + propertyFileName + ": " + e.getMessage());
            throw new RuntimeException("Unable to find config");
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public boolean getBooleanProperty(String key, boolean defaultValue) {
        final String value = getProperty(key);
        if (value != null) {
            try {
                return Boolean.parseBoolean(value);
            } catch (final Exception e) {
                System.err.println("Error parsing property '" + key + "': " + e.getMessage());
            }
        }

        return defaultValue;
    }

    public int getIntProperty(String key, int defaultValue) {
        final String value = getProperty(key);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (final NumberFormatException e) {
                System.err.println("Invalid integer format for key '" + key + "': " + value);
            }
        }

        return defaultValue;
    }
}
