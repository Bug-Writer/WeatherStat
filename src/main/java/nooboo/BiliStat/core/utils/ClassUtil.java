package nooboo.BiliStat.core.utils;

import java.lang.reflect.Constructor;

public final class ClassUtil {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T> T instantiate(String className, Class<T> t, Object... args) {
        try {
            Constructor constructor = (Constructor) Class.forName(className).getConstructor(ClassUtil.toClassType(args));
            return (T) constructor.newInstance(args);
        }
        catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static Class<?>[] toClassType(Object[] args) {
        Class<?>[] clazz = new Class<?>[args.length];

        for (int i = 0, length = args.length; i < length; i++) {
            clazz[i] = args[i].getClass();
        }

        return clazz;
    }

}
