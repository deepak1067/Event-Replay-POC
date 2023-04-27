package org.example;


import com.google.api.services.bigquery.model.JsonObject;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

@Slf4j
public class FormProcessingPipeline {
    public static void run(EventReplayPipelineOptions options) {

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(FileIO.match().filepattern("/home/knoldus/Documents/event-replay/FormProcessingPOC1/src/main/java/org/example/input.json"))
                .apply(FileIO.readMatches())
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via(fileBytes -> {
                            try {
                                return new String(fileBytes.readFullyAsBytes());
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }))
                .apply("ParseJSON", ParDo.of(new DoFn<String, Map<String,String>>(){
                    @DoFn.ProcessElement
                    public void processElement(ProcessContext context) {
                        String json = context.element();
                        Gson gson = new Gson();
                        Type type = new TypeToken<Map<String,String>>(){}.getType();
                        try {
                            Map<String,String> map = gson.fromJson(json, type);
                            context.output(map);
                        } catch (JsonSyntaxException e) {
                            log.error("Error parsing JSON: {}", e.getMessage());

                        }

                    }
                }))
                .apply("BackupAndDelete", ParDo.of(new BackupAndDeleteFromBigQueryFn(options)));
//                .apply("Transform", ParDo.of(new GCSFolderReaderByThreadFn(options)));

        pipeline.run().waitUntilFinish();


    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(EventReplayPipelineOptions.class);
        EventReplayPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(EventReplayPipelineOptions.class);

        run(options);
    }
}