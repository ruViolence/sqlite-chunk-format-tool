package ru.violence.sqlitechunkformattool;

import com.github.luben.zstd.Zstd;
import net.minecraft.server.v1_12_R1.ChunkCoordIntPair;
import net.minecraft.server.v1_12_R1.DataConverterManager;
import net.minecraft.server.v1_12_R1.DataConverterTypes;
import net.minecraft.server.v1_12_R1.DataStreamNBTHelper;
import net.minecraft.server.v1_12_R1.NBTTagCompound;
import net.minecraft.server.v1_12_R1.NBTTagList;
import net.minecraft.server.v1_12_R1.RegionFile;
import net.minecraft.server.v1_12_R1.RegionFileCache;
import net.minecraft.server.v1_12_R1.SQLiteChunkLoader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection", "SqlResolve"})
public class Tasker {
    private static final Pattern regionFileCoordPattern = Pattern.compile("^r\\.(-?\\d+)\\.(-?\\d+)\\.mca$"); // r.-10.-10.mca
    private static final byte[] emptySkyLight = new byte[2048];

    static void processSQLiteTask(int compressionLevel, File dimensionDir, File regionDir) throws Throwable {
        if (!regionDir.exists()) {
            System.out.println("region directory does not exists");
            return;
        }
        if (new File(dimensionDir, "region.db").exists()
                || new File(dimensionDir, "region.db-shm").exists()
                || new File(dimensionDir, "region.db-wal").exists()) {
            System.out.println("Database file is already exists");
            return;
        }

        boolean hasSkyLight = !dimensionDir.getName().startsWith("DIM");
        SQLiteChunkLoader sqLiteChunkLoader = new SQLiteChunkLoader(dimensionDir, compressionLevel);

        // Speedup SQLite writes
        try (Statement st = sqLiteChunkLoader.getConnection().createStatement()) {
            st.execute("PRAGMA journal_mode = 'OFF'");
            st.execute("PRAGMA synchronous = '0'");
        }
        sqLiteChunkLoader.getConnection().setAutoCommit(false);

        AtomicInteger done = new AtomicInteger(0);
        final int total = (int) Files.walk(regionDir.toPath(), 1)
                .skip(1) // Skip region folder
                .filter(Files::isRegularFile)
                .filter(Utils::isRegionFile)
                .count();

        Thread logThread = startLogThread(done, total);

        DataConverterManager dataFixer = new DataConverterManager(1343);

        Iterator<File> iter = Files.walk(regionDir.toPath(), 1)
                .skip(1) // Skip region folder
                .filter(Files::isRegularFile)
                .filter(path -> {
                    if (Utils.isRegionFile(path)) return true;
                    System.out.println("Skipping file: " + path);
                    return false;
                })
                .map(Path::toFile)
                .iterator();

        Object lock = new Object();
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        while (iter.hasNext()) {
            File file = iter.next();
            executor.submit(() -> {
                try {
                    String fileName = file.getName();

                    Matcher matcher = regionFileCoordPattern.matcher(fileName);

                    if (!matcher.find()) {
                        System.out.println("Unable to parse region file name: " + fileName);
                        return;
                    }

                    RegionFile regionFile = new RegionFile(file);

                    int xOffset = Integer.parseInt(matcher.group(1)) * 32;
                    int zOffset = Integer.parseInt(matcher.group(2)) * 32;

                    List<Object[]> chunks = new ArrayList<>();

                    for (int x = 0; x < 32; x++) {
                        for (int z = 0; z < 32; z++) {
                            NBTTagCompound chunkTag = Utils.readChunk(regionFile, x, z);
                            if (chunkTag == null) continue;

                            // Upgrade old chunks
                            chunkTag = dataFixer.a(DataConverterTypes.CHUNK, chunkTag);
                            if (!hasSkyLight) {
                                NBTTagList sectionsTagList = chunkTag.getCompound("Level").getList("Sections", 10);
                                for (int i = 0; i < sectionsTagList.size(); i++) {
                                    sectionsTagList.get(i).remove("SkyLight");
                                }
                            }

                            int chunkX = x + xOffset;
                            int chunkZ = z + zOffset;

                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            DataStreamNBTHelper.writeChunkNBTAsDataStream(chunkTag, new DataOutputStream(baos));
                            byte[] compressed = Zstd.compress(baos.toByteArray(), compressionLevel);

                            chunks.add(new Object[]{chunkX, chunkZ, compressed});
                        }
                    }

                    // Write chunks to the SQLite
                    synchronized (lock) {
                        try (PreparedStatement ps = sqLiteChunkLoader.getConnection().prepareStatement("INSERT OR REPLACE INTO chunk VALUES (?, ?, ?)")) {
                            for (Object[] chunk : chunks) {
                                ps.setInt(1, (int) chunk[0]);
                                ps.setInt(2, (int) chunk[1]);
                                ps.setBytes(3, (byte[]) chunk[2]);
                                ps.execute();
                            }
                        }
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                } finally {
                    done.getAndIncrement();
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
        logThread.interrupt();

        sqLiteChunkLoader.getConnection().commit();
        sqLiteChunkLoader.closeConnection();
    }

    static void processRegionTask(int compressionLevel, File dimensionDir, File regionDir) throws Throwable {
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

        List<ChunkCoordIntPair> coords;
        AtomicInteger done = new AtomicInteger(0);
        final int total;

        try (Statement st = connection.createStatement()) {
            total = st.executeQuery("SELECT COUNT() FROM chunk").getInt(1);
            coords = new ArrayList<>(total);

            ResultSet rs = st.executeQuery("SELECT x, z FROM chunk");
            while (rs.next()) {
                coords.add(new ChunkCoordIntPair(rs.getInt(1), rs.getInt(2)));
            }
        }

        Thread logThread = startLogThread(done, total);

        Object lock = new Object();
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        for (ChunkCoordIntPair pos : coords) {
            executor.submit(() -> {
                try {
                    int chunkX = pos.x;
                    int chunkZ = pos.z;

                    NBTTagCompound chunkTag;

                    try (PreparedStatement ps = sqLiteChunkLoader.getConnection().prepareStatement("SELECT data FROM chunk WHERE x = ? AND z = ?")) {
                        ps.setInt(1, chunkX);
                        ps.setInt(2, chunkZ);
                        ResultSet rs = ps.executeQuery();
                        byte[] data = rs.getBytes(1);

                        chunkTag = DataStreamNBTHelper.readChunkNBTFromDataStream(
                                new DataInputStream(
                                        new ByteArrayInputStream(Zstd.decompress(data, (int) Zstd.decompressedSize(data)))
                                )
                        );
                    }

                    { // Add back overhead data
                        chunkTag.setInt("DataVersion", 1343);
                        NBTTagCompound levelCompound = chunkTag.getCompound("Level");
                        levelCompound.setInt("xPos", chunkX);
                        levelCompound.setInt("zPos", chunkZ);
                        if (!levelCompound.hasKeyOfType("LastUpdate", 4)) {
                            levelCompound.setLong("LastUpdate", 0);
                        }
                        if (!levelCompound.hasKeyOfType("SkyLight", 7)) {
                            levelCompound.setByteArray("SkyLight", emptySkyLight);
                        }
                    }

                    synchronized (lock) {
                        RegionFileCache.e(dimensionDir, chunkX, chunkZ, chunkTag);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    done.getAndIncrement();
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
        logThread.interrupt();

        sqLiteChunkLoader.closeConnection();
        // Flush and close region file writers
        for (RegionFile regionFile : RegionFileCache.a.values()) {
            try {
                regionFile.c();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static Thread startLogThread(AtomicInteger done, int total) {
        Thread logThread = new Thread(() -> {
            while (true) {
                try {Thread.sleep(1000);} catch (InterruptedException e) {return;}
                int doneL = done.get();
                if (doneL >= total) return;
                System.out.println("Processing (" + Utils.calcPercentage(doneL, total) + "%) (" + doneL + " / " + total + ")");
            }
        });
        logThread.setDaemon(true);
        logThread.start();
        return logThread;
    }
}
