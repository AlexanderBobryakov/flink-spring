package com.asbobryakov.flink_spring.testutils.flink.state;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.apache.commons.io.FileUtils;
import org.jeasy.random.EasyRandom;
import org.jeasy.random.EasyRandomParameters;
import org.jeasy.random.randomizers.range.IntegerRangeRandomizer;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static java.lang.String.format;

@UtilityClass
@SuppressWarnings({"PMD.UseVarargs", "PMD.UseProperClassLoader"})
public class StateClassLoadingUtil {

    @NotNull
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static <E> ObjectTypeClassLoaderDto<E> compileStateClass(String className, int version) {
        final var tmpPath = Path.of(System.getProperty("java.io.tmpdir"));
        final var baseGeneratedClassesPath = tmpPath.resolve("generatedClasses").resolve(UUID.randomUUID().toString()).toFile();
        if (!baseGeneratedClassesPath.mkdirs()) {
            throw new IllegalStateException(format("Failed to create temporary directory: [%s]", List.of(baseGeneratedClassesPath)));
        }
        final var generatedClassPath = baseGeneratedClassesPath.toPath().resolve(className).resolve(UUID.randomUUID().toString());
        @Cleanup final var classLoader = ClassLoaderUtils.writeAndCompile(
                generatedClassPath.toFile(),
                className,
                loadStateClassContentFromResources(className, version));

        final var aClass = (Class<E>) classLoader.loadClass(className);
        final var obj = (E) createEasyRandom().nextObject(aClass);
        return new ObjectTypeClassLoaderDto<>(obj, aClass, classLoader);
    }

    @SneakyThrows
    public static String loadStateClassContentFromResources(String className, int version) {
        final var stateClassFile = getResourceByName("state/" + className + "/v" + version + ".txt");
        return FileUtils.readFileToString(stateClassFile, StandardCharsets.UTF_8);
    }

    @SneakyThrows
    private static File getResourceByName(String name) {
        final var fileUrl = StateClassLoadingUtil.class.getClassLoader().getResource(name);
        if (fileUrl == null) {
            throw new FileNotFoundException(format("Resource file '%s' not found", name));
        }
        return new File(fileUrl.getFile());
    }

    private static EasyRandom createEasyRandom() {
        // Important: create new EasyRandomParameters, because it caches classes
        final var easyRandomParameters = new EasyRandomParameters();
        easyRandomParameters.setRandomizationDepth(1);
        easyRandomParameters.setStringLengthRange(new EasyRandomParameters.Range<>(1, 5));
        easyRandomParameters.setCollectionSizeRange(new EasyRandomParameters.Range<>(1, 2));
        easyRandomParameters.randomize(Integer.class, new IntegerRangeRandomizer(0, 1000));
        return new EasyRandom(easyRandomParameters);
    }
}
