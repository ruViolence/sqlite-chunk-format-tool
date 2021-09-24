package ru.violence.sqlitechunkformattool;

import net.minecraft.server.v1_12_R1.NBTCompressedStreamTools;
import net.minecraft.server.v1_12_R1.NBTTagCompound;
import net.minecraft.server.v1_12_R1.RegionFile;
import net.minecraft.server.v1_12_R1.RegionFileCache;

import java.io.DataInputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.nio.file.Path;

public class Utils {
    private static final MethodHandle METHOD_RegionFile_isOversized;
    private static final MethodHandle METHOD_RegionFileCache_readOversizedChunk;

    static {
        try {
            {
                Method method = RegionFile.class.getDeclaredMethod("isOversized", int.class, int.class);
                method.setAccessible(true);
                METHOD_RegionFile_isOversized = MethodHandles.lookup().unreflect(method);
            }
            {
                Method method = RegionFileCache.class.getDeclaredMethod("readOversizedChunk", RegionFile.class, int.class, int.class);
                method.setAccessible(true);
                METHOD_RegionFileCache_readOversizedChunk = MethodHandles.lookup().unreflect(method);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isChunkOversized(RegionFile regionFile, int x, int z) throws Throwable {
        return (boolean) METHOD_RegionFile_isOversized.invoke(regionFile, x, z);
    }

    public static NBTTagCompound readChunk(RegionFile regionFile, int x, int z) throws Throwable {
        if (isChunkOversized(regionFile, x, z)) {
            return (NBTTagCompound) METHOD_RegionFileCache_readOversizedChunk.invoke(regionFile, x, z);
        } else {
            DataInputStream datainputstream = regionFile.a(x, z);
            return datainputstream == null ? null : NBTCompressedStreamTools.a(datainputstream);
        }
    }

    public static int calcPercentage(int done, int total) {
        return (int) ((done * 100.0f) / total);
    }

    public static boolean isRegionFile(Path path) {
        String fileName = path.getFileName().toString();
        return fileName.startsWith("r.") && fileName.endsWith(".mca");
    }
}
