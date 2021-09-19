package ru.violence.sqlitechunkformattool;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Locale;

public class Main {

    public static void main(String[] args) throws Throwable {
        if (args.length < 4) {
            System.out.println("Provide args: <sqlite/region> <compression_level> <path_server_jar> <path_to_world_dir>");
            return;
        }

        String task = args[0].toLowerCase(Locale.ROOT);
        int compressionLevel = Integer.parseInt(args[1]);
        String pathToServerJar = args[2];
        String pathToWorldDir = args[3];

        File serverJar = new File(pathToServerJar);
        File worldDir = new File(pathToWorldDir);

        if (!serverJar.exists()) {
            System.out.println("Server jar does not exist");
            return;
        }

        if (!worldDir.exists()) {
            System.out.println("World directory does not exist");
            return;
        }

        if (!worldDir.isDirectory()) {
            System.out.println("Provided world path is not directory");
            return;
        }

        addJarToClasspath(serverJar);

        File dimensionDir = resolveDimensionDirectory(worldDir);

        if (!dimensionDir.isDirectory()) {
            System.out.println("Unknown dimension folder: " + dimensionDir.getCanonicalPath());
            return;
        }

        File regionDir = new File(dimensionDir, "region");

        if (task.equals("sqlite")) {
            Tasker.processSQLiteTask(compressionLevel, dimensionDir, regionDir);
        } else if (task.equals("region")) {
            Tasker.processRegionTask(compressionLevel, dimensionDir, regionDir);
        } else {
            System.out.println("Available tasks: sqlite, region");
        }
    }

    @SuppressWarnings("ConstantConditions")
    private static File resolveDimensionDirectory(File worldDir) {
        return Arrays.stream(worldDir.listFiles())
                .filter(file -> file.getName().startsWith("DIM"))
                .findFirst()
                .orElse(worldDir);
    }

    private static void addJarToClasspath(File file) throws Throwable {
        Method addUrlMethod = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        addUrlMethod.setAccessible(true);
        addUrlMethod.invoke(Main.class.getClassLoader(), file.toURI().toURL());
    }
}
