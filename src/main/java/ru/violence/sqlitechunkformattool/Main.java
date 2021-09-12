package ru.violence.sqlitechunkformattool;

import net.minecraft.server.v1_12_R1.ChunkCoordIntPair;
import net.minecraft.server.v1_12_R1.NBTTagCompound;
import net.minecraft.server.v1_12_R1.RegionFileCache;
import net.minecraft.server.v1_12_R1.SQLiteChunkLoader;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
public class Main {
    private static final Pattern regionFileCoordPattern = Pattern.compile("^r\\.(-?\\d+)\\.(-?\\d+)\\.mca$"); // r.-10.-10.mca

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
            processSQLiteTask(compressionLevel, dimensionDir, regionDir);
        } else if (task.equals("region")) {
            processRegionTask(compressionLevel, dimensionDir, regionDir);
        } else {
            System.out.println("Available tasks: sqlite, region");
        }
    }

    private static void processSQLiteTask(int compressionLevel, File dimensionDir, File regionDir) throws Throwable {
        long startTime = System.currentTimeMillis();

        if (new File(dimensionDir, "region.db").exists()
                || new File(dimensionDir, "region.db-shm").exists()
                || new File(dimensionDir, "region.db-wal").exists()) {
            System.out.println("Database file is already exists");
            return;
        }

        SQLiteChunkLoader sqLiteChunkLoader = new SQLiteChunkLoader(dimensionDir, compressionLevel);

        AtomicInteger done = new AtomicInteger(0);
        int total = (int) Files.walk(regionDir.toPath(), 1)
                .skip(1) // Skip region folder
                .filter(Files::isRegularFile)
                .filter(Main::isRegionFile)
                .count();

        Files.walk(regionDir.toPath(), 1)
                .skip(1) // Skip region folder
                .filter(Files::isRegularFile)
                .filter(path -> {
                    if (isRegionFile(path)) return true;
                    System.out.println("Skipping file: " + path);
                    return false;
                })
                .forEach(path -> {
                    try {
                        String fileName = path.getFileName().toString();
                        System.out.println("Processing (" + calcPercentage(done.get(), total) + "% / 100%): " + fileName + " (" + done.getAndIncrement() + "/" + total + ")");

                        Matcher matcher = regionFileCoordPattern.matcher(fileName);

                        if (!matcher.find()) {
                            System.out.println("Unable to parse region file name: " + fileName);
                            return;
                        }

                        int xOffset = Integer.parseInt(matcher.group(1)) * 32;
                        int zOffset = Integer.parseInt(matcher.group(2)) * 32;

                        for (int x = 0; x < 32; x++) {
                            for (int z = 0; z < 32; z++) {
                                int chunkX = x + xOffset;
                                int chunkZ = z + zOffset;

                                NBTTagCompound chunkTag = RegionFileCache.d(dimensionDir, chunkX, chunkZ);
                                if (chunkTag != null) {
                                    sqLiteChunkLoader.writeToDB(new ChunkCoordIntPair(chunkX, chunkZ), chunkTag);
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

        sqLiteChunkLoader.closeConnection();

        System.out.println("Successful (took " + ((System.currentTimeMillis() - startTime) / 1000) + " s)");
    }

    private static void processRegionTask(int compressionLevel, File dimensionDir, File regionDir) throws Throwable {
        long startTime = System.currentTimeMillis();

        if (regionDir.exists()) {
            System.out.println("region folder is already exists");
            return;
        }
        if (!new File(dimensionDir, "region.db").exists()) {
            System.out.println("region.db does not exist");
            return;
        }

        SQLiteChunkLoader sqLiteChunkLoader = new SQLiteChunkLoader(dimensionDir, compressionLevel);
        Connection connection = sqLiteChunkLoader.getConnection();

        try (Statement st = connection.createStatement()) {
            AtomicInteger done = new AtomicInteger(0);
            int total = st.executeQuery("SELECT COUNT() FROM chunk").getInt(1);

            ResultSet rs = st.executeQuery("SELECT x, z FROM `chunk`");
            List<ChunkCoordIntPair> coords = new ArrayList<>();
            while (rs.next()) {
                coords.add(new ChunkCoordIntPair(rs.getInt(1), rs.getInt(2)));
            }

            for (ChunkCoordIntPair pos : coords) {
                try {
                    int chunkX = pos.x;
                    int chunkZ = pos.z;

                    if (done.get() % 1000 == 0) {
                        System.out.println("Processing (" + calcPercentage(done.get(), total) + "% / 100%): " + chunkX + ";" + chunkZ + " (" + done + "/" + total + ")");
                    }

                    NBTTagCompound chunkTag = sqLiteChunkLoader.loadFromDB(pos);

                    { // Add back overhead data
                        chunkTag.setInt("DataVersion", 1343);
                        NBTTagCompound levelCompound = chunkTag.getCompound("Level");
                        levelCompound.setInt("xPos", chunkX);
                        levelCompound.setInt("zPos", chunkZ);
                        if (!levelCompound.hasKeyOfType("LastUpdate", 4)) {
                            levelCompound.setLong("LastUpdate", 0);
                        }
                    }

                    RegionFileCache.e(dimensionDir, chunkX, chunkZ, chunkTag);
                    done.getAndIncrement();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        sqLiteChunkLoader.closeConnection();
        // Flush
        RegionFileCache.a.values().forEach(regionFile -> {
            try {
                regionFile.c();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        System.out.println("Successful (took " + ((System.currentTimeMillis() - startTime) / 1000) + " s)");
    }

    @SuppressWarnings("ConstantConditions")
    private static File resolveDimensionDirectory(File worldDir) {
        return Arrays.stream(worldDir.listFiles())
                .filter(file -> file.getName().startsWith("DIM"))
                .findFirst()
                .orElse(worldDir);
    }

    private static boolean isRegionFile(Path path) {
        String fileName = path.getFileName().toString();
        return fileName.startsWith("r.") && fileName.endsWith(".mca");
    }

    private static int calcPercentage(int done, int total) {
        return (int) ((done * 100.0f) / total);
    }

    private static void addJarToClasspath(File file) throws Throwable {
        Method addUrlMethod = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        addUrlMethod.setAccessible(true);
        addUrlMethod.invoke(Main.class.getClassLoader(), file.toURI().toURL());
    }
}
