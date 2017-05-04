package com.jbox;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.asynchttpclient.Dsl.asyncHttpClient;

public class Main {

    static int callsPerBatch = 20;

    static int loopCounter = 1;
    static boolean loop = true;
    public static void increaseLoopCounter() {
        loopCounter++;
    }
    public static Thread createLoop(AsyncHttpClient httpClient) {
        return new Thread(() -> {
            try {
                while (loop) {
                    System.out.println("getMany iteration: " + loopCounter);
                    getMany(httpClient);
                    increaseLoopCounter();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    // Important to clean up resources
                    httpClient.close();
                    System.out.println("Closed HTTP client");
                } catch (IOException e) {
                    System.err.println("Failed due to IOException");
                    e.printStackTrace();
                }
            }
        });
    }
    public static void main(String... args) {
        // Important to set threads to a low number since we do NOT want to spawn hundreds of
        // threads. At most 1 per client, and they are cleaned up after closing the client.
        // Probably want only one client per request thread.
        DefaultAsyncHttpClientConfig config = new DefaultAsyncHttpClientConfig.Builder()
                .setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3086.0 Safari/537.36")
                .setIoThreadsCount(0).build();
        AsyncHttpClient httpClient = asyncHttpClient(config);

        createLoop(httpClient).start();
        createLoop(httpClient).start();
    }

   /**
     * Aggregate many things without spinning up hundreds threads. Just 1 due to Java NIO.
     * @param httpClient
     */
    public static void getMany(AsyncHttpClient httpClient) {
        System.out.println("START ---------------------- !");
        long startTime = System.nanoTime();

        //CompletableFuture<List<Response>> superFuture = null;
        CompletableFuture<Void> superFuture = null;
        List<String> results = new ArrayList<>();
        List<String> asyncResults = null;
        CompletableFuture<String>[] futures = new CompletableFuture[callsPerBatch];

        try {
            for (int i = 0; i < callsPerBatch; i++) {
                CompletableFuture<String> future = getAsync(httpClient, "https://twitter.com/POTUS/status/859786181537669120?_=" + (i+1), result -> results.add(result));
                futures[i] = future;
            }
            // Blocks until all futures are completed. Be sure to handle fast-fail exceptions.
            superFuture = CompletableFuture.allOf(futures);
            superFuture.get(20000, TimeUnit.MILLISECONDS);
            long collectStartTime = System.nanoTime();
            asyncResults = Arrays.stream(futures).map(CompletableFuture::join).collect(Collectors.toList());
            System.out.println("Time taken to collect completed results: " + timeElapsedSince(collectStartTime));
        } catch (InterruptedException e) {
            System.err.println("Catch: Interrupted");
            e.printStackTrace();
        } catch (ExecutionException e) {
            System.err.println("Catch: ExecutionException A future failed to complete");
            e.printStackTrace();
        } catch (TimeoutException e) {
            System.err.println("Catch: Timeout");
            superFuture.cancel(true);
            e.printStackTrace();
        }

        System.out.println("Number of results: " + results.size());
        System.out.println("Number of asyncResults: " + asyncResults.size());
        if (results.size() != callsPerBatch) {
            System.err.println("Result size mismatch or incomplete");
        }
        if (asyncResults.size() != callsPerBatch) {
            System.err.println("Async result size mismatch or incomplete");
        }
        for (String result : asyncResults) {
            System.out.println(result);
        }
        System.out.println("Total time taken to fetch and aggregate: " + timeElapsedSince(startTime));
        if (superFuture.isCompletedExceptionally()) {
            System.out.println("COMPLETED_EXCEPTIONALLY ---------------------- " + httpClient.isClosed() + "!");
        } else if (superFuture.isCancelled()) {
            System.out.println("CANCELLED ---------------------- " + httpClient.isClosed() + "!");
        } else if (superFuture.isDone()) {
            System.out.println("COMPLETE ---------------------- " + httpClient.isClosed() + "!");
        }
    }

    public static CompletableFuture<String> getAsync(AsyncHttpClient httpClient, String url, Consumer<String> handleResult) {
        long startTime = System.nanoTime();
        ListenableFuture<Response> promise = httpClient.prepareGet(url).execute();
        CompletableFuture<Response> future = promise.toCompletableFuture();
        boolean failOnNon200 = false;
        return future
            .handle((response, ex) -> {
                if (ex != null) {
                    System.err.println("whenComplete exception: " + ex);
                    throw new RuntimeException(ex);
                    //future.completeExceptionally(ex);
                    //return null;
                } else if (response.getStatusCode() != 200 && failOnNon200) {
                    System.err.println("whenComplete exception: Bad status: " + response.getStatusCode());
                    RuntimeException failedStatusCodeException = new RuntimeException("Failed status code: " + response.getStatusCode());
                    throw failedStatusCodeException;
                    //future.completeExceptionally(failedStatusCodeException);
                    //return null;
                }

                String body = response.getResponseBody();
                System.out.println(httpClient.getClientStats().toString());
                String snippet = null;
                if (body.length() <= 8) {
                    System.err.println("Small body: " + url);
                    snippet = "";
                } else {
                    snippet = body.substring(body.length() - 8);
                }
                // Adding some indication that each request actually has a proper response
                String result = url + String.format(" %s %s %s", timeElapsedSince(startTime), response.getStatusCode(), snippet);
                //String result = timeElapsedSince(startTime);
                if (result == null) {
                    System.err.println("Null result");
                }
                handleResult.accept(result);
                return result;
            });
    }

    public static String timeElapsedSince(long startTime) {
        return (System.nanoTime() - startTime)/1000000 + "ms";
    }
}
