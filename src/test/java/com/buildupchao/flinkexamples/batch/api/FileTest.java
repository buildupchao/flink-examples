package com.buildupchao.flinkexamples.batch.api;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author buildupchao
 * @date 2020/01/01 23:41
 * @since JDK 1.8
 */
public class FileTest {

    public static void main(String[] args) throws IOException {
        File file = new File("data/user.txt");
        System.out.println(Files.readAllLines(Paths.get(file.getAbsolutePath())));
    }
}
