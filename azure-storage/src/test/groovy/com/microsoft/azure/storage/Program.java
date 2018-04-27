package com.microsoft.azure.storage;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;

import com.microsoft.azure.storage.blob.*;
import com.microsoft.rest.v2.util.FlowableUtil;

import java.nio.file.Paths;
import java.time.Duration;

public class Program {

    static void downloadBlob(BlockBlobURL blobURL) {


        try {

            // Get the blob
            // Since the blob is small, we'll read the entire blob into memory asynchronously
            // com.microsoft.rest.v2.util.FlowableUtil is a static class that contains helpers to work with Flowable
            blobURL.download(new BlobRange(0, Long.MAX_VALUE), null, false)
                    .flatMapCompletable(response -> {
                        AsynchronousFileChannel channel = AsynchronousFileChannel.open(Paths.get("myfile"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                        return FlowableUtil.writeFile(response.body(), channel);
                    })
                    .blockingAwait();
        } catch (Exception ex){

            System.out.println(ex.toString());

        }

    }

    public static void main(String[] args){
        ContainerURL containerURL;

        try {
            // Retrieve the credentials and initialize SharedKeyCredentials
            String accountName = args[0];
            String accountKey = args[1];

            System.out.println("New Storage SDK");


            // Create a ServiceURL to call the Blob service. We will also use this to construct the ContainerURL
            SharedKeyCredentials creds = new SharedKeyCredentials(accountName, accountKey);

            final ServiceURL serviceURL = new ServiceURL(new URL("https://" + accountName + ".blob.core.windows.net"), StorageURL.createPipeline(creds, new PipelineOptions()));

            // Let's create a container using a blocking call to Azure Storage
            containerURL = serviceURL.createContainerURL("javasdktest");

            Double runningAverage = null;
            while (true) {

                long startTime = System.nanoTime();

                final BlockBlobURL blobURL = containerURL.createBlockBlobURL(args[2]);
                downloadBlob(blobURL);

                long endTime = System.nanoTime();
                if (runningAverage == null) {
                    runningAverage = (double) (endTime - startTime);
                } else {
                    runningAverage = runningAverage * 0.8 + (endTime - startTime) * 0.2;
                }

                System.out.format("Time to download: %s\tWeighed average: %s\n",
                        Duration.ofNanos(endTime - startTime),
                        Duration.ofNanos(runningAverage.longValue()));
            }

        } catch (InvalidKeyException e) {
            System.out.println("Invalid Storage account name/key provided");
        } catch (MalformedURLException e) {
            System.out.println("Invalid URI provided");
        }
    }
}
