//package org.example;
//
//import com.google.api.gax.paging.Page;
//import com.google.cloud.pubsub.v1.Publisher;
//import com.google.cloud.storage.*;
//import com.google.protobuf.ByteString;
//import com.google.pubsub.v1.ProjectTopicName;
//import com.google.pubsub.v1.PubsubMessage;
//import org.apache.beam.sdk.options.ValueProvider;
//import org.apache.beam.sdk.transforms.DoFn;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.*;
//
//public class GCSFolderReaderByThreadFn extends DoFn<List<String>,List<String>> {
//
//    ValueProvider<String> projectId;
//    ValueProvider<String> location;
//    ValueProvider<String> bucketName;
//    ValueProvider<String> topicId;
//    public static String folderPath;
//    public static List<String> list = new ArrayList<>();
//    public static List<String> filteredDates = new ArrayList<>();
//
//    public GCSFolderReaderByThreadFn(FormProcessingPipelineOptions options) {
//        this.projectId = options.getProjectId();
//        this.bucketName = options.getBucketName();
//        this.topicId = options.getTopicId();
//
//    }
//
//    @ProcessElement
//    public void processElement(ProcessContext context) {
//
//        String baseUrl = "https://storage.cloud.google.com/";
//        String formName = "";
//
////        formName = context.element().toString();
//        List<String> receivedData = context.element();
//
//        folderPath = baseUrl + bucketName.get() + "/" + formName + "/";
//        folderPath = folderPath + filteredDates.get(0);
//
//        // Set up queue and executor
//        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
//        ExecutorService executor = Executors.newFixedThreadPool(10);
//
//        // Instantiate a client
//        Storage storage = StorageOptions.newBuilder()
////                .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream("src/main/java/org/example/sdmdemo1-b5eef5a5ebca.json")))
//                .setProjectId(projectId.get())
//                .build().getService();
//
//        // Add all blobs in the folder to the queue
//        Page<Blob> blobs = storage.list(
//                bucketName.get(),
//                Storage.BlobListOption.prefix(folderPath),
//                Storage.BlobListOption.currentDirectory());
//        // Sav blob names into a list
//        for (Blob blob : blobs.iterateAll()) {
//            String blobName = blob.getName();
//            list.add(blobName);
//        }
//
//        String startDate = "2023-02-01";
//        String endDate   = "2023-05-01";
//
//        // Filter Dates
//        dateFilter(list);
//        dateFilter(filteredDates);
//        // Start processing blobs in parallel using threads
//        while (!queue.isEmpty()) {
//            String blob = queue.poll();
//            if (blob != null) {
//                executor.execute(new BlobProcessor(blob,projectId.get(),topicId.get(),bucketName.get()));
//            }
//        }
//
//        // Shutdown executor
//        executor.shutdown();
//        context.output(receivedData);
//    }
//    List<String> dateFilter(List<String> list){
//        for (String path : list) {
//            String dateString = path.substring(path.length() - 8);
//            String regex = "^\\d{4}-(0[1-9]|1[0-2])/$";
//
//            // Date format check
//            if (!dateString.matches(regex)) {
//                System.out.println("Date format incorrect");
//                continue;
//            }
//            if (dateString.startsWith(startDate.substring(0,6)) || dateString.startsWith(endDate)) {
//                filteredDates.add(dateString);
//                queue.add(dateString);
//            }
//            else {
//                int comparison1 = dateString.compareTo(startDate.substring(0,6));
//                int comparison2 = dateString.compareTo(endDate);
//                if (comparison1 >= 0 && comparison2 <= 0) {
//                    queue.add(dateString);
//                    filteredDates.add(dateString);
//                }
//            }
//        }
//
//    }
//    public static void gcsToPubSub(String prefix,String projectId, String topicId, String bucketName) throws Exception {
////        String projectId  = "sdmdemo1";
////        String topicId    = "sdmdemotopic";
////        String bucketName = "sdmdemobucket";
//
//        // Instantiate a client
//        Storage storage = StorageOptions.newBuilder()
////                .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream("src/main/java/org/example/sdmdemo1-b5eef5a5ebca.json")))
//                .setProjectId(projectId)
//                .build().getService();
//
//        Page<Blob> blobs = storage.list(
//                bucketName,
//                Storage.BlobListOption.prefix(prefix),
//                Storage.BlobListOption.currentDirectory());
//        ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
//
//        Publisher publisher = Publisher.newBuilder(topicName).build();
//
//        for (Blob blob : blobs.iterateAll()) {
//            String message = blob.getName();
//            ByteString byteString = ByteString.copyFromUtf8(message);
//            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(byteString).build();
//            publisher.publish(pubsubMessage);
//        }
//
//        publisher.shutdown();
//    }
//
//
//
//    // A class to process each blob in a separate thread
//    private static class BlobProcessor implements Runnable {
//        String blob;
//        String projectId;
//        String topicId;
//        String bucketName;
//
//        public BlobProcessor(String blob,String projectId, String topicId, String bucketName) {
//            this.blob = blob;
//            this.projectId = projectId;
//            this.topicId = topicId;
//            this.bucketName = bucketName;
//        }
//
//        @Override
//        public void run() {
//            try {
//                gcsToPubSub(folderPath + blob,projectId,topicId,bucketName);
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }
//}
