package com.linear.solution;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class FrequencyApplication {
    /**
     * 词频统计
     * 1、文本分块
     * 2、局部统计词频
     * 3、合并统计
     * 4、TOP100
     */

    public static String FILE_PATH = "/Attention_Is_All_You_Need_In_Search_of_an_Understandable_Consensus_Algorithm.txt";

    public static ConcurrentHashMap<String, AtomicInteger> map = new ConcurrentHashMap<>();

    public static ExecutorService executorService = Executors.newFixedThreadPool(3);

    public static void main(String[] args) throws URISyntaxException, InterruptedException {
        URL resource = FrequencyApplication.class.getResource(FILE_PATH);
        Path path = Paths.get(resource.toURI());
        // 目标是 target
        Path parent = path.getParent();

        // 拆分文本
        List<File> files = splitChunk(
                resource.getPath(),
                parent.toString(),
                10240);

        // 词频统计
        makeStatistic(files);

        // 汇总计算
        totalStatistic();

        executorService.shutdown();
        boolean b = executorService.awaitTermination(600, TimeUnit.SECONDS);
        System.out.println(b);
    }

    private static List<File> splitChunk(String filePath, String chunkPath, long chunkSize) {
        List<File> chunks = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            char[] buffer = new char[(int) chunkSize];
            int charsRead = 0;
            int fileCount = 0;

            while ((charsRead = reader.read(buffer)) != -1) {
                File chunkFile = new File(chunkPath, "chunk_" + fileCount + ".txt");
                fileCount++;
                try(BufferedWriter writer = new BufferedWriter(
                        new FileWriter(chunkFile))
                ){
                    writer.write(buffer, 0, charsRead);
                }
                chunks.add(chunkFile);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return chunks;
    }

    private static void makeStatistic(List<File> chunks){
        for (File chunk : chunks) {
            executorService.submit(()->{
                HashMap<String, Integer> local = new HashMap<>();

                try(Scanner scanner = new Scanner(chunk)){
                    scanner.useDelimiter("[^a-zA-Z0-9]+");
                }catch(Exception e){
                    e.printStackTrace();
                }
            });
        }
    }

    private static void totalStatistic(){

    }
}