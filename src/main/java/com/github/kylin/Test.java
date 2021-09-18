package com.github.kylin;

import com.github.kylin.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-09-16 18:24
 * @description
 */
public class Test {
    public static void main(String[] args) throws IOException {
        File file = new File("default-cubes.txt");
        List<String> lines = Utils.readResourceAsLines("default-cubes.txt");
        for (String line : lines) {
            System.out.println(line);
        }
    }
}
