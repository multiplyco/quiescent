package co.multiply.quiescent.impl;

import clojure.lang.RT;

/**
 * Context classloader for executor threads. Resolves Clojure's own classes and the
 * application's classes through the loader it wraps, and always returns the newest
 * version of a class Clojure compiled at runtime.
 *
 * <p>It is deliberately not a {@code DynamicClassLoader}. {@code RT.classForName}
 * consults Clojure's newest-version class cache only for loaders that aren't, and
 * otherwise defers to {@code Class.forName}, which the JVM answers from its own
 * per-loader cache: the first version that loader ever returned for the name.
 *
 * <p>Remaining gap: a direct {@code Class.forName(name, init, thisLoader)} is
 * still answered from the JVM's cache, so it keeps returning the first version.
 */
public final class TaskClassLoader extends ClassLoader {
    static {
        registerAsParallelCapable();
    }

    public TaskClassLoader(ClassLoader base) {
        super("quiescent", new Base(base));
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        // Skips findLoadedClass, which would return the version the JVM recorded for this loader.
        Class<?> c = RT.classForName(name, false, getParent());
        if (resolve) {
            resolveClass(c);
        }
        return c;
    }

    /**
     * Plain pass-through to the wrapped loader. It exists so that RT.classForName sees a
     * loader that isn't a DynamicClassLoader: the wrapped loader is one whenever Clojure
     * is loaded from source.
     */
    private static final class Base extends ClassLoader {
        static {
            registerAsParallelCapable();
        }

        Base(ClassLoader parent) {
            super("quiescent-base", parent);
        }
    }
}
