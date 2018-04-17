package org.apache.kafka.connect.streams;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.HeaderConverter;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Utils {

    public static Map<String, Object> fromProperties(Properties properties) {
        Map<String, Object> map = new HashMap<>(properties.size());
        for (Enumeration<?> e = properties.propertyNames(); e.hasMoreElements(); ) {
            String key = (String) e.nextElement();
            Object value = properties.getProperty(key);
            if (value != null) {
                map.put(key, value);
            }
        }
        return map;
    }

    public static <T> T newInstance(Class<T> c) {
        if (c == null)
            throw new KafkaException("class cannot be null");
        try {
            return c.getDeclaredConstructor().newInstance();
        } catch (NoSuchMethodException e) {
            throw new KafkaException("Could not find a public no-argument constructor for " + c.getName(), e);
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw new KafkaException("Could not instantiate class " + c.getName(), e);
        }
    }

    public static Converter newConverter(AbstractConfig config, String classPropertyName) {
        if (!config.originals().containsKey(classPropertyName)) {
            // This configuration does not define the converter via the specified property name
            return null;
        }
        Class<? extends Converter> converterClass = config.getClass(classPropertyName).asSubclass(Converter.class);
        Converter converter = Utils.newInstance(converterClass);

        // Determine whether this is a key or value converter based upon the supplied property name ...
        final boolean isKeyConverter = ConnectStreamsConfig.KEY_CONVERTER_CLASS_CONFIG.equals(classPropertyName)
                || ConnectStreamsConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG.equals(classPropertyName);

        // Configure the Converter using only the old configuration mechanism ...
        String configPrefix = classPropertyName + ".";
        Map<String, Object> converterConfig = config.originalsWithPrefix(configPrefix);
        converter.configure(converterConfig, isKeyConverter);
        return converter;
    }

    public static HeaderConverter newHeaderConverter(AbstractConfig config, String classPropertyName) {
        if (!config.originals().containsKey(classPropertyName)) {
            // This configuration does not define the header converter via the specified property name
            return null;
        }
        Class<? extends HeaderConverter> converterClass = config.getClass(classPropertyName).asSubclass(HeaderConverter.class);
        HeaderConverter converter = Utils.newInstance(converterClass);

        String configPrefix = classPropertyName + ".";
        Map<String, Object> converterConfig = config.originalsWithPrefix(configPrefix);
        converterConfig.put(ConverterConfig.TYPE_CONFIG, ConverterType.HEADER.getName());
        converter.configure(converterConfig);
        return converter;
    }
}
