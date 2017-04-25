package com.jbox;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.asynchttpclient.Dsl.asyncHttpClient;

public class Main {

    static final int totalCalls = 300;
    public static void main(String... args) {
        getMany();
    }

    /**
     * Aggregate many things without spinning up hundreds threads. Just 1 due to Java NIO.
     */
    public static void getMany() {
        long startTime = System.nanoTime();
        System.out.println("START ---------------------- !");

        List<String> results = new ArrayList<>();
        CompletableFuture<Response>[] futures = new CompletableFuture[totalCalls];

        // Important to set threads to a low number since we do NOT want to spawn hundreds of
        // threads. At most 1 per client, and they are cleaned up after closing the client.
        // Probably want only one client per request thread.
        DefaultAsyncHttpClientConfig config = new DefaultAsyncHttpClientConfig.Builder()
                .setIoThreadsCount(1).build();
        AsyncHttpClient httpClient = asyncHttpClient(config);
        try {
            for (int i=0; i < totalCalls; i++) {
                CompletableFuture<Response> future = getAsync(httpClient, "https://github.com/debop/hibernate-redis/issues/" + i, result -> results.add(result));
                futures[i] = future;
            }
            // Blocks until all futures are completed. Be sure to handle fast-fail exceptions.
            CompletableFuture.allOf(futures).join();
            // Important to clean up resources
            httpClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (String result : results) {
            System.out.println(result);
        }
        System.out.println("Number of results: " + results.size());
        System.out.println("Total time taken to fetch and aggregate: " + timeElapsedSince(startTime));
        System.out.println("COMPLETE ---------------------- " + httpClient.isClosed() + "!");
    }

    public static CompletableFuture<Response> getAsync(AsyncHttpClient httpClient, String url, Consumer<String> handleResult) {
        final long startTime = System.nanoTime();
        CompletableFuture<Response> future = httpClient.prepareGet(url)
                .execute()
                .toCompletableFuture()
                .thenApplyAsync(response -> {
                    String body = response.getResponseBody();
                    // Adding some indication that each request actually has a proper response
                    String result = url + String.format(" %s ", timeElapsedSince(startTime)) + body.substring(body.length() - 8);
                    handleResult.accept(result);
                    return response;
                });
        return future;
    }

    public static String timeElapsedSince(long startTime) {
        return (System.nanoTime() - startTime)/1000000 + "ms";
    }
}
