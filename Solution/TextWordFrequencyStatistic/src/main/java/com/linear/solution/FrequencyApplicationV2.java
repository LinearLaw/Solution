package com.linear.solution;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class FrequencyApplicationV2 {
    /**
     * 词频统计
     * 1、文本分块
     * 2、局部统计词频
     * 3、合并统计
     * 4、TOP100
     */

    public static String FILE_PATH = "/Attention_Is_All_You_Need_In_Search_of_an_Understandable_Consensus_Algorithm.txt";

    public static ConcurrentHashMap<String, AtomicInteger> wordMap = new ConcurrentHashMap<>();

    public static ExecutorService executorService = Executors.newFixedThreadPool(3);

    public static void main(String[] args) {
        try {
            calc();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void calc() throws URISyntaxException, InterruptedException {
        URL resource = FrequencyApplicationV2.class.getResource(FILE_PATH);
        Path path = Paths.get(resource.toURI());
        // 目标是 target
        Path parent = path.getParent();

        // 1、拆分文本，分片
        List<File> files = splitChunk(
                resource.getPath(),
                parent.toString(),
                10240);

        // 2、每一个分片进行词频统计
        try {
            makeStatistic(files);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // 3、汇总计算
        List<Map.Entry<String, AtomicInteger>> res = totalStatistic();
        for (Map.Entry<String, AtomicInteger> re : res) {
            System.out.println(re);
        }

        executorService.shutdown();
        executorService.awaitTermination(600, TimeUnit.SECONDS);
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
                try (BufferedWriter writer = new BufferedWriter(
                        new FileWriter(chunkFile))
                ) {
                    writer.write(buffer, 0, charsRead);
                }
                chunks.add(chunkFile);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return chunks;
    }

    private static void makeStatistic(List<File> chunks) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(chunks.size());

        for (File chunk : chunks) {
            // use completableFuture instead
            CompletableFuture.supplyAsync(()->{
                doStatistic4Chunk(chunk);
                countDownLatch.countDown();
                return true;
            }, executorService).exceptionally(e ->{
                // exception throw, but countDown
                countDownLatch.countDown();
                return false;
            });
        }

        countDownLatch.await();
    }

    private static void doStatistic4Chunk(File chunk){
        HashMap<String, Integer> local = new HashMap<>();

        try (Scanner scanner = new Scanner(chunk)) {
            scanner.useDelimiter("[^a-zA-Z0-9]+");

            while (scanner.hasNext()) {
                String word = scanner.next().toLowerCase();
                local.put(word, local.getOrDefault(word, 0) + 1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 直接合并到全局map
        local.forEach((word, count) -> {
            wordMap.computeIfAbsent(
                    word,
                    k -> new AtomicInteger(0)
            ).addAndGet(count);
        });
    }

    private static List<Map.Entry<String, AtomicInteger>> totalStatistic() {
        PriorityQueue<Map.Entry<String, AtomicInteger>> queue = new PriorityQueue<>(
                Comparator.comparingInt(a -> a.getValue().get())
        );

        wordMap.entrySet().forEach(entry -> {
            queue.add(entry);
            // 数量超出100，弹出堆顶
            if (queue.size() > 100) {
                queue.poll();
            }
        });

        List<Map.Entry<String, AtomicInteger>> list = new ArrayList<>(queue);
        return list;
    }
}