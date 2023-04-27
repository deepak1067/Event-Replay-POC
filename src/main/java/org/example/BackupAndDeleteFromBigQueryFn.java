package org.example;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.CopyJobConfiguration;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableId;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import java.util.List;
import java.util.Map;

@Slf4j
public class BackupAndDeleteFromBigQueryFn extends DoFn<Map<String, String>, List<String>> {

    ValueProvider<String> sourceDatasetName;

    public BackupAndDeleteFromBigQueryFn(EventReplayPipelineOptions options) {
        this.sourceDatasetName = options.getSourceDatasetName();
    }

    @ProcessElement
    public void processBackUpTable(ProcessContext context) {
        log.info("Backup started");
        try {
            Map<String, String> inputData = context.element();
            String sourceTableId = inputData.get("formName");

            // Initialize client that will be used to send requests. This client only needs to be created
            // once, and can be reused for multiple requests.
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            TableId sourceTable = TableId.of(String.valueOf(sourceDatasetName.get()), sourceTableId);
            TableId destinationTable = TableId.of(sourceDatasetName.get(), sourceTableId+"_backup");

            CopyJobConfiguration configuration =
                    CopyJobConfiguration.newBuilder(destinationTable, sourceTable).build();

            Job job = bigquery.create(JobInfo.of(configuration));

            // Blocks until this job completes its execution, either failing or succeeding.
            Job completedJob = job.waitFor();
            if (completedJob == null) {
                System.out.println("Job not executed since it no longer exists.");
            } else if (completedJob.getStatus().getError() != null) {
                System.out.println(
                        "BigQuery was unable to backup table due to an error: \n" + job.getStatus().getError());
            }
            System.out.println("Table backup successfully. Deleting Source table: ");
            deleteTable(String.valueOf(sourceDatasetName), sourceTableId);
        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Table backup job was interrupted. \n" + e.toString());
        }
    }

    public static void deleteTable(String sourceDatasetName, String sourceTableId) {
        try {

            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            boolean success = bigquery.delete(TableId.of(sourceDatasetName, sourceTableId));
            if (success) {
                System.out.println("Table deleted successfully after backup");
            } else {
                System.out.println("Table was not found");
            }
        } catch (BigQueryException e) {
            System.out.println("Table was not deleted. \n" + e.toString());
        }
    }

}
