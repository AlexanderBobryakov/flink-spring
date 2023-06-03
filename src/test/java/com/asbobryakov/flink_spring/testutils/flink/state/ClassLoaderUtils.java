package com.asbobryakov.flink_spring.testutils.flink.state;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.File;
import java.io.FileWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import static java.lang.String.format;

@UtilityClass
public class ClassLoaderUtils {

    @SneakyThrows
    public static URLClassLoader writeAndCompile(File root, String className, String source) {
        final var file = writeSourceToFile(root.toPath().resolve("source").toFile(), createJavaFileName(className), source);
        compileClass(file);
        return createClassLoader(file.getParentFile(), Thread.currentThread().getContextClassLoader());
    }

    private static String createJavaFileName(String className) {
        return className + ".java";
    }

    @SneakyThrows
    private static File writeSourceToFile(File root, String filename, String source) {
        final var file = new File(root, filename);
        if (!file.getParentFile().mkdirs()) {
            throw new IllegalStateException(format("Failed to create directory: [%s]", file));
        }
        try (var fileWriter = new FileWriter(file)) {
            fileWriter.write(source);
        }
        return file;
    }

    @SneakyThrows
    private static void compileClass(File sourceFile) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        compiler.run(
                null,
                null,
                null,
                "-proc:none",
                "-classpath",
                sourceFile.getParent() + ":" + System.getProperty("java.class.path"),
                sourceFile.getPath());
    }

    private static URLClassLoader createClassLoader(File root, ClassLoader parent)
            throws MalformedURLException {
        return new URLClassLoader(new URL[]{root.toURI().toURL()}, parent);
    }
}
