package com.github.kylin.utils;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-09-15 15:26
 * @description utils.
 */
public final class Utils {
    private static final SimpleDateFormat DEFAULT_SIMPLE_DATE_FORMAE = new SimpleDateFormat("yyyyMMddHHmmss");
    public static final String LINE_SEPARATOR = System.getProperty("line.separator");

    // Prints up to 2 decimal digits. Used for human readable printing
    private static final DecimalFormat TWO_DIGIT_FORMAT = new DecimalFormat("0.##");
    private static final String[] BYTE_SCALE_SUFFIXES = new String[] {"B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};

    private Utils(){}

    /**
     * Formats a byte number as a human readable String ("3.2 MB")
     * @param bytes some size in bytes
     */
    public static String formatBytes(long bytes) {
        if (bytes < 0) {
            return String.valueOf(bytes);
        }
        double asDouble = (double) bytes;
        int ordinal = (int) Math.floor(Math.log(asDouble) / Math.log(1024.0));
        double scale = Math.pow(1024.0, ordinal);
        double scaled = asDouble / scale;
        String formatted = TWO_DIGIT_FORMAT.format(scaled);
        try {
            return formatted + " " + BYTE_SCALE_SUFFIXES[ordinal];
        } catch (IndexOutOfBoundsException e) {
            //huge number?
            return String.valueOf(asDouble);
        }
    }

    public static String formatDateTime(long dateTimeMills,String pattern){
        SimpleDateFormat sdf;
        if(pattern == null){
            sdf = DEFAULT_SIMPLE_DATE_FORMAE;
        }else{
            sdf = new SimpleDateFormat(pattern);
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(dateTimeMills);
        return sdf.format(calendar.getTime());
    }

    public static long startTimeStampOfDate(String dateStr,String pattern){
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        long startTs = 0L;
        try{
            Date date = sdf.parse(dateStr);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            startTs = calendar.getTimeInMillis();
        }catch (ParseException pe){
            pe.printStackTrace();
        }
        return startTs;
    }

    /**
     *  日期加减
     * */
    public static String dateShift(String dateStr, String format, int amount){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        String shiftDate = "";
        try{
            Date date = sdf.parse(dateStr);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.add(Calendar.DATE,amount);
            shiftDate = sdf.format(calendar.getTime());
        }catch (ParseException pe){
            pe.printStackTrace();
        }

        return shiftDate;
    }

    public static boolean isSegmentOverlapped(String dateStr,String pattern,long segmentStartTs,long segmentStopTs){
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        boolean overlappedFlag = false;
        try{
            Date date = sdf.parse(dateStr);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            long now = calendar.getTimeInMillis() + 10 * 3600000;
            // cube段重叠
            if(now >= segmentStartTs && now <= segmentStopTs){
                overlappedFlag = true;
            }
        }catch (ParseException pe){
            pe.printStackTrace();
        }
        return overlappedFlag;
    }

    public static List<String> readResourceAsLines(String resourceName) {
        Resource resource = new ClassPathResource(resourceName);
        List<String> lines = new ArrayList<>();
        try{
            // File file = ResourceUtils.getFile("classpath:" + resourceName);
            File file = resource.getFile();
            lines = readAllLines(file,Charset.defaultCharset());
        }catch (IOException ioe){
            ioe.printStackTrace();
        }
        return lines;
    }

    public static void sleepQuietly(long mills){
        try{
            Thread.sleep(mills);
        }catch (InterruptedException ie){
            // ignore.
        }
    }

    private static List<String> readAllLines(File file, Charset charset) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
        try {
            List<String> result = new ArrayList<>();
            for (;;) {
                String line = reader.readLine();
                if (line == null){
                    break;
                }
                result.add(line);
            }
            return result;
        }
        finally {
            reader.close();
        }
    }

    public static List<String> readAllLines(InputStream input) throws IOException {
        final List<String> lines = new ArrayList<>();
        readAllLines(input, lines::add);
        return lines;
    }

    private static void readAllLines(InputStream input, Consumer<String> consumer) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                consumer.accept(line);
            }
        }
    }

    public static void writeCsvFile(String filePath,List<String> lines){
        FileWriter fileWriter = null;

        try{
            fileWriter = new FileWriter(filePath);
            for (String line : lines) {
                fileWriter.write(line);
                fileWriter.write(LINE_SEPARATOR);
            }


        }catch (IOException ioe){
            ioe.printStackTrace();
        }finally {
            if(fileWriter != null){
                try{
                    fileWriter.close();
                }catch (IOException ex){
                    ex.printStackTrace();
                }
            }
        }
    }

}
