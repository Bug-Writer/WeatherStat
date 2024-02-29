package nooboo.WeatherStat;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Properties;

public class Configuration {

    public String getClassName() {
        Properties props = new Properties();
        String sinkClassName = null;
        try {
            props.load(new FileInputStream("config.properties"));
            sinkClassName = props.getProperty("databaseSinkClass");
            return sinkClassName;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T createInstance(Class<T> type, Object... args) {
        String className = this.getClassName();
        try {
            Class<?>[] argClasses = new Class<?>[args.length];
            for (int i = 0; i < args.length; i++) {
                argClasses[i] = args[i].getClass();
            }

            Class<?> clazz = Class.forName(className);
            Constructor<?> constructor = clazz.getConstructor(argClasses);
            return type.cast(constructor.newInstance(args));
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate class: " + className, e);
        }
    }
}
