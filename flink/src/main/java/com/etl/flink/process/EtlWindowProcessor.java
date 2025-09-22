package com.etl.flink.process;

import com.etl.flink.model.EtlConfig;
import com.etl.flink.model.EtlResult;
import com.etl.flink.model.SensorEvent;
import com.etl.flink.model.Transformation;
import com.etl.flink.udf.ElementTransformations;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Processes ETL configurations to create appropriate windowed or non-windowed streams
 */
public class EtlWindowProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(EtlWindowProcessor.class);

    /**
     * Processes a data stream based on ETL configuration, creating windowed aggregations
     * where appropriate and element transformations for others.
     */
    public static DataStream<EtlResult> processWithConfig(
            DataStream<SensorEvent> eventStream,
            EtlConfig config) {

        DataStream<SensorEvent> currentStream = eventStream.filter(event -> event != null);
        List<DataStream<EtlResult>> resultStreams = new ArrayList<>();

        if (config.getTransformations() == null || config.getTransformations().isEmpty()) {
            // No transformations, just convert events to results
            return currentStream.map(event -> createSimpleResult(event, config));
        }

        for (Transformation transformation : config.getTransformations()) {
            String transformationType = transformation.getType();

            if (isElementTransformation(transformationType)) {
                // Apply element transformation to the current stream
                currentStream = applyElementTransformation(currentStream, transformation);
            } else if (isAggregationTransformation(transformationType)) {
                // Create windowed aggregation stream and add to results
                DataStream<EtlResult> aggregationStream = EtlCoFlatMapFunction.createWindowedAggregationStream(
                    currentStream, transformation, config);
                resultStreams.add(aggregationStream);
                LOG.info("Created windowed aggregation stream for {}", transformationType);
            } else {
                LOG.warn("Unknown transformation type: {}", transformationType);
            }
        }

        // If we have aggregation streams, return union of all result streams
        if (!resultStreams.isEmpty()) {
            DataStream<EtlResult> unionStream = resultStreams.get(0);
            for (int i = 1; i < resultStreams.size(); i++) {
                unionStream = unionStream.union(resultStreams.get(i));
            }
            return unionStream;
        }

        // If only element transformations, convert final stream to results
        return currentStream.map(event -> createFinalResult(event, config));
    }

    private static boolean isElementTransformation(String type) {
        return "normalize_string".equals(type) || "lowercase".equals(type) || "uppercase".equals(type) ||
               "filter_greater".equals(type) || "filter_less".equals(type) ||
               "extract_year".equals(type) || "extract_month".equals(type) || "extract_day".equals(type);
    }

    private static boolean isAggregationTransformation(String type) {
        return "sum".equals(type) || "max".equals(type) || "min".equals(type) || "avg".equals(type);
    }

    private static DataStream<SensorEvent> applyElementTransformation(
            DataStream<SensorEvent> stream, Transformation transformation) {
        try {
            var mapFunction = ElementTransformations.createTransformation(
                transformation.getType(), transformation.getParams());
            return stream.map(mapFunction).filter(event -> event != null);
        } catch (Exception e) {
            LOG.error("Failed to create element transformation: {}", transformation.getType(), e);
            return stream; // Return original stream on error
        }
    }

    private static EtlResult createSimpleResult(SensorEvent event, EtlConfig config) {
        EtlResult result = new EtlResult();
        result.setJobId(config.getJobId());
        result.setSource(config.getSource());
        result.setTransformations(config.getTransformations());
        result.setGroupingField("sensor");
        result.setGroupingKey(event.getSensor());
        result.setResult(event);
        result.setProcessedAt(java.time.Instant.now());

        List<String> diagnostics = new ArrayList<>();
        diagnostics.add("No transformations applied, returning original event");
        result.setDiagnostics(diagnostics);

        return result;
    }

    private static EtlResult createFinalResult(SensorEvent event, EtlConfig config) {
        EtlResult result = new EtlResult();
        result.setJobId(config.getJobId());
        result.setSource(config.getSource());
        result.setTransformations(config.getTransformations());
        result.setGroupingField("sensor");
        result.setGroupingKey(event.getSensor());
        result.setResult(event);
        result.setProcessedAt(java.time.Instant.now());

        List<String> diagnostics = new ArrayList<>();
        diagnostics.add("Applied element transformations, returning transformed event");
        result.setDiagnostics(diagnostics);

        return result;
    }
}