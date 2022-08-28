package com.huangjunyi1993.zeromq.util;

import com.huangjunyi1993.zeromq.config.GlobalConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.List;
import java.util.stream.Collectors;

import static com.huangjunyi1993.zeromq.base.constants.CommonConstant.SUFFIX_INDEX;
import static com.huangjunyi1993.zeromq.base.constants.CommonConstant.SUFFIX_LOG;

/**
 * 文件工具类
 * Created by huangjunyi on 2022/8/21.
 */
public class FileUtil {

    /**
     * 寻找指定目录下偏移量最新的文件
     * @param dir
     * @return
     * @throws IOException
     */
    public static String findLastLOGFile(String dir) throws IOException {
        return findLastFile(dir, SUFFIX_LOG);
    }

    public static String findLastIndexFile(String dir) throws IOException {
        return findLastFile(dir, SUFFIX_INDEX);
    }

    public static String findLastFile(String dir, String suffix) throws IOException {
        Path path = Paths.get(dir);
        boolean pathExists = Files.exists(path, LinkOption.NOFOLLOW_LINKS);

        if (pathExists) {
            List<Long> fileNames = Files.list(path)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .map(fileName -> fileName.substring(0, fileName.lastIndexOf(".")))
                    .map(Long::parseLong)
                    .collect(Collectors.toList());

            long max = fileNames.get(0);
            for (long fileName : fileNames) {
                max = Math.max(max, fileName);
            }

            return dir + File.separator + max + "." + suffix;
        }
        return null;
    }


    /**
     * 创建指定topic的日志目录，同时创建一个新的日志文件
     * @param dir
     * @param topic
     * @return
     */
    public static String createTopicDirAndNewLogFIle(String dir, String topic) throws IOException {
        return createTopicDirAndNewFIle(dir, topic, "log", GlobalConfiguration.get().getMaxLogFileSize());
    }

    /**
     * 创建指定topic的索引目录，同时创建一个新的索引文件
     * @param dir
     * @param topic
     * @return
     * @throws IOException
     */
    public static String createTopicDirAndNewIndexFIle(String dir, String topic) throws IOException {
        return createTopicDirAndNewFIle(dir, topic, "index", 8 * 1000L);
    }

    /**
     * 创建指定目录，同时创建一个新文件
     * @param dir
     * @param topic
     * @param suffix
     * @param length
     * @return
     * @throws IOException
     */
    public static String createTopicDirAndNewFIle(String dir, String topic, String suffix, long length) throws IOException {
        String topicDirPath = dir + File.separator + topic;
        Path path = Paths.get(topicDirPath);
        boolean pathExists = Files.exists(path, LinkOption.NOFOLLOW_LINKS);
        if (!pathExists) {
            Files.createDirectories(path);
        }
        String filePath = topicDirPath + File.separator + 0 + "." + suffix;
        RandomAccessFile r = null;
        try {
            r = new RandomAccessFile(filePath, "rw");
            r.setLength(length);
        } finally{
            if (r != null) {
                r.close();
            }
        }
        return filePath;
    }

    /**
     * 创建新的消息日志文件
     * @param dir
     * @return
     * @throws IOException
     */
    public static String createNewLogFile(String dir) throws IOException {
        long totalSize = Files.list(Paths.get(dir)).map(path -> {
            try {
                return Files.size(path);
            } catch (IOException e) {
                return 0L;
            }
        }).collect(Collectors.summarizingLong(size -> size)).getSum();

        String newLogFileName = String.valueOf(totalSize) + ".log";
        String path = dir + File.separator + newLogFileName;
        RandomAccessFile r = null;
        try {
            r = new RandomAccessFile(path, "rw");
            r.setLength(GlobalConfiguration.get().getMaxLogFileSize());
        } finally{
            if (r != null) {
                r.close();
            }
        }
        return path;
    }

    /**
     * 创建新的索引文件
     * @param lastIndexFile
     * @return
     * @throws IOException
     */
    public static String createNewIndexFile(String lastIndexFile) throws IOException {
        int newNum = Integer.valueOf(lastIndexFile.substring(lastIndexFile.lastIndexOf(File.separator) + 1, lastIndexFile.lastIndexOf("."))) + 1;
        String newIndexFileName = lastIndexFile.substring(0, lastIndexFile.lastIndexOf(File.separator) + 1) + newNum + ".index";
        RandomAccessFile r = null;
        try {
            r = new RandomAccessFile(newIndexFileName, "rw");
            r.setLength(8 * 1000L);
        } finally{
            if (r != null) {
                r.close();
            }
        }
        return newIndexFileName;
    }

    /**
     * 获取文件大小
     * @param fileName
     * @return
     * @throws IOException
     */
    public static long getFileSize (String fileName) throws IOException {
        return Files.size(Paths.get(fileName));
    }

    /**
     * 获取文件名记录的偏移量
     * @param fileName
     * @return
     */
    public static long getOffsetOfFileName(String fileName) {
        return Long.valueOf(fileName.substring(fileName.lastIndexOf(File.separator) + 1, fileName.lastIndexOf(".")));
    }

    /**
     * 根据消息日志偏移量确定目标文件
     * @param dir
     * @param messageLogOffset
     * @return
     * @throws IOException
     */
    public static String determineTargetLogFile(String dir, long messageLogOffset) throws IOException {
        Path path = Paths.get(dir);
        boolean pathExists = Files.exists(path, LinkOption.NOFOLLOW_LINKS);

        if (pathExists) {
            List<Long> fileNames = Files.list(path)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .map(fileName -> fileName.substring(0, fileName.lastIndexOf(".")))
                    .map(Long::parseLong)
                    .collect(Collectors.toList());

            fileNames.sort((o1, o2) -> (int)(o2 - o1));
            for (Long fileName : fileNames) {
                if (fileName <= messageLogOffset) {
                    return dir + File.separator + fileName + ".log";
                }
            }
        }
        return null;
    }

    /**
     * 检查文件是否存在，不存在则创建，并且返回false，存在返回true
     * @param file
     * @return
     * @throws IOException
     */
    public static boolean createIfNotExists(String file) throws IOException {
        Path path = Paths.get(file);
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            if (!Files.exists(path.getParent(), LinkOption.NOFOLLOW_LINKS)) {
                Files.createDirectories(path.getParent());
            }
            Files.createFile(path);
            return false;
        }
        return true;
    }

    /**
     * 检查文件是否存在
     * @param file
     * @return
     * @throws IOException
     */
    public static boolean exists (String file) throws IOException {
        Path path = Paths.get(file);
        return Files.exists(path, LinkOption.NOFOLLOW_LINKS);
    }
}
