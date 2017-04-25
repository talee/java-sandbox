package com.jbox;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Response;
import org.jdeferred.DeferredManager;
import org.jdeferred.Promise;
import org.jdeferred.impl.DefaultDeferredManager;
import org.jdeferred.multiple.MasterProgress;
import org.jdeferred.multiple.MultipleResults;
import org.jdeferred.multiple.OneReject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static org.asynchttpclient.Dsl.asyncHttpClient;

public class Main {

    static final int totalCalls = 99;
    public static void main(String... args) {
        getMany();
    }

    public static void getMany() {
        long startTime = System.nanoTime();
        System.out.println("START ---------------------- !");
        List<String> results = new ArrayList<>();
        CompletableFuture<Response>[] futures = new CompletableFuture[totalCalls];
        DefaultAsyncHttpClientConfig config = new DefaultAsyncHttpClientConfig.Builder()
                .setIoThreadsCount(1).build();
        AsyncHttpClient httpClient = asyncHttpClient(config);
        for (int i=0; i < totalCalls; i++) {
            CompletableFuture<Response> future = getAsync2(httpClient, "https://github.com/debop/hibernate-redis/issues/" + i, result -> results.add(result));
            futures[i] = future;
            System.out.println("Threads: " + config.getIoThreadsCount());
        }
        System.out.println("PRE AllOF: " + config.getIoThreadsCount());
        CompletableFuture.allOf(futures).join();
        try {
            httpClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("POST AllOF: " + config.getIoThreadsCount());
        System.out.println(results.size() + " " + results.get(results.size() - 1));
        for (String result : results) {
            System.out.println(result);
        }
        System.out.println((System.nanoTime() - startTime)/1000000 + "ms");
        System.out.println("COMPLETE ---------------------- " + httpClient.isClosed() + "!");
    }

    public static CompletableFuture<Response> getAsync2(AsyncHttpClient httpClient, String url, Consumer<String> handleResult) {
        CompletableFuture<Response> future = httpClient.prepareGet(url)
                .execute()
                .toCompletableFuture()
                .thenApplyAsync(response -> {
                    String body = response.getResponseBody();
                    String result = url + " " + body.substring(body.length() - 8);
                    handleResult.accept(result);
                    return response;
                });
        return future;
    }

    public static void deferred() {
        Callable<String>[] calls = new Callable[99];
        for (int i=0; i < 99; i++) {
            int finalI = i;
            Callable<String> call = () -> get("https://github.com/debop/hibernate-redis/issues/" + finalI);
            calls[i] = call;
        }
        DeferredManager dm = new DefaultDeferredManager();
        System.out.println("DEFER START ---------------------- !");
        Promise<MultipleResults, OneReject, MasterProgress> promise = dm.when(calls).done(results -> {
            results.forEach(result -> System.out.println(result));
        });

        try {
            promise.waitSafely();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static String get(String url) {
        CloseableHttpClient httpClient = HttpClients.createSystem();
        HttpGet slowGet = new HttpGet(url);
        String result = null;
        try (
                CloseableHttpResponse response = httpClient.execute(slowGet);
        ) {
            String body = "hello worlds nice"; //IOUtils.toString(response.getEntity().getContent(), "UTF-8");
            result = url + " " + body.substring(body.length() - 8);
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static Future<HttpResponse> getAsync(String url, Consumer<String> handleResult) {
        CloseableHttpAsyncClient httpClient = HttpAsyncClients.createSystem();
        HttpGet slowGet = new HttpGet(url);
        Future<HttpResponse> future;
        try  {
            httpClient.start();
            future = httpClient.execute(slowGet, new FutureCallback<HttpResponse>() {
                @Override
                public void completed(HttpResponse response) {
                    String body = null;
                    try {
                        body = IOUtils.toString(response.getEntity().getContent(), "UTF-8");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    String result = url + " " + body.substring(body.length() - 8);
                    System.out.println(result);
                    handleResult.accept(result);
                }

                @Override
                public void failed(Exception e) {
                    System.out.println(e);
                }

                @Override
                public void cancelled() {
                    System.out.println("Cancelled wtf");
                }
            });
            //String body = "hello worlds nice";
        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return future;
    }
}
