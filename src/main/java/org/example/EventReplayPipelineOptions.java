package org.example;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;

public interface EventReplayPipelineOptions extends DataflowPipelineOptions {
    @Description("GCP Project Id name")
    ValueProvider<String> getProjectId();

    void setProjectId(ValueProvider<String> value);

    @Description("GCP Location Id name")
    ValueProvider<String> getLocationId();

    void setLocationId(ValueProvider<String> value);

    @Description("GCP BucketName")
    ValueProvider<String> getBucketName();

    void setBucketName(ValueProvider<String> value);

    @Description("BigQuery dataSet name")
    ValueProvider<String> getDataSet();

    void setDataSet(ValueProvider<String> value);
    @Description("Source dataset name")
    ValueProvider<String> getSourceDatasetName();
    void setSourceDatasetName(ValueProvider<String> value);
}
