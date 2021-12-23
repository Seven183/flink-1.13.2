package utils;

import jline.internal.Log;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

@Slf4j
public class PropertyUtils {

	public final static String CONF_NAME = "application-";

	public static final Properties contextProperties = new Properties();

	public static void init(String env){
		try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(CONF_NAME + env + ".properties")){
			assert in != null;
			InputStreamReader inputStreamReader = new InputStreamReader(in, StandardCharsets.UTF_8);
			contextProperties.load(inputStreamReader);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
		Log.info("配置文件加载完毕，环境：" + env);
	}

	public static String getStrValue(String key) {
		return contextProperties.getProperty(key);
	}

	public static int getIntValue(String key) {
		String strValue = getStrValue(key);
		return Integer.parseInt(strValue);
	}

}