package com.tencent.trt.executor.transformer;

import com.tencent.trt.executor.model.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/8/15.
 */
public class TransformStage implements Serializable, CommandHandler {

    public static Logger LOGGER = LoggerFactory.getLogger(TransformStage.class);
    public static final Map<String, Class> TRANSFORMERS = new HashMap<String, Class>() {
        {
            put("fixvalue", FixValue.class);
            put("split_pipe", SplitDelimiter.class);
            put("split_delimiter", SplitDelimiter.class);
            put("parse_json", ParseJson.class);
            put("parse_upstream", ParseUpstream.class);
            put("parse_jsonsplit", ParseJsonSpilt.class);
        }
    };
    private static TransformerExtension extension;
    private final List<Transformer> transformers;
    private final Map<String, RecordSchema> outputSchemas;
    private List<Record> outputs = new ArrayList<Record>();

    static {
        extension = createExtension();
    }

    private static TransformerExtension createExtension() {
        try {
            Class<?> clazz = TransformStage.class.forName("com.tencent.application.transformer.ApplicationTransformers");
            try {
                return (TransformerExtension) clazz.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("failed to create instance: " + clazz);
            }
        } catch (ClassNotFoundException e) {
            // ignore
            return null;
        }
    }

    public TransformStage(final ResultTable resultTable, boolean hasAggregateStage) {
        try {
            transformers = createTransformers(resultTable, hasAggregateStage);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        outputSchemas = createOutputSchemas(resultTable, hasAggregateStage);
    }

    private static List<Transformer> createTransformers(ResultTable resultTable, boolean hasAggregateStage) throws Exception {
        Map<String, String> nameMap = new HashMap<String, String>();
        ArrayList<Transformer> transformers = new ArrayList<Transformer>();
        if (null != resultTable.timeField) {
            if (isTransformer(resultTable.timeField.processor)) {
                transformers.add(createTransformer(resultTable, resultTable.timeField));
            } else if (StringUtils.isBlank(resultTable.timeField.processor)) {
                if (resultTable.timeField.origins.length == 1) {
                    nameMap.put(ResultTable.TIME_FIELD, resultTable.timeField.getSingleOriginField());
                }
            }
        }
        for (ResultField resultField : resultTable.fields) {
            if (resultField.isDimension || !hasAggregateStage) {
                if (StringUtils.isBlank(resultField.processor)) {
                    nameMap.put(resultField.field, resultField.getSingleOriginField());
                } else {
                    Transformer transformer = createTransformer(resultTable, resultField);
                    if (transformer != null) {
                        transformers.add(transformer);
                    }
                }
            }
        }
        if (!nameMap.isEmpty()) {
            transformers.add(0, new Rename(nameMap)); // other transformer might override some field
        }
        if (hasAggregateStage) {
            // copy input into output, so aggregate stage can reference any input it want
            // WITHOUT specifying origins
            transformers.add(0, new CopyInput());
        }
        // the order must be: copy_input => rename => other transformers
        return transformers;
    }

    private static Transformer createTransformer(ResultTable resultTable, ResultField resultField) throws Exception {
        if (isCreatedByOtherProcessor(resultField.processor)) {
            return null;
        }
        Class transformerClass = getTransformerClass(resultTable, resultField);
        Transformer transformer = (Transformer) transformerClass.getConstructor(ResultTable.class, ResultField.class).newInstance(resultTable, resultField);
        return transformer;
    }

    private static Class getTransformerClass(ResultTable resultTable, ResultField resultField) {
        if ("transformer".equals(resultField.processor)) {
            try {
                return ExprTransformer.generateTransformerClass(resultTable, resultField);
            } catch (Exception e) {
                throw new RuntimeException("failed to generate transformer class", e);
            }
        }
        Class transformerClass = TRANSFORMERS.get(resultField.processor);
        if (null == transformerClass) {
            transformerClass = extension.getTransformerClass(resultField.processor);
        }
        if (null == transformerClass) {
            throw new RuntimeException("transformer not found: " + resultField.processor);
        }
        return transformerClass;
    }

    private static boolean isCreatedByOtherProcessor(String processor) {
        return !StringUtils.isBlank(processor) && processor.trim().startsWith("=>");
    }

    private static Map<String, RecordSchema> createOutputSchemas(ResultTable resultTable, boolean hasAggregateStage) {
        HashMap<String, RecordSchema> outputSchemas = new HashMap<String, RecordSchema>();
        for (Upstream parent : resultTable.parents) {
            if (hasAggregateStage) {
                RecordSchema schema = resultTable.getTransformOutputSchema(parent);
                outputSchemas.put(parent.parentResultTableId, schema);
            } else {
                RecordSchema schema = resultTable.getOutputSchema();
                outputSchemas.put(parent.parentResultTableId, schema);
            }
        }
        return outputSchemas;
    }

    public List<Record> transform(Record input) {
        try {
            RecordSchema outputSchema = outputSchemas.get(input.getResultTableId());
            if (null == outputSchema) {
                throw new RuntimeException("missing output schema for: " + input.getResultTableId());
            }
            outputs.clear();
            Record output = outputSchema.createRecord();
            outputs.add(output);
            for (Transformer transformer : transformers) {
                transformer.execute(input, outputs);
            }
            return outputs;
        } catch (Exception e) {
            LOGGER.error("failed to transform: " + input, e);
            outputs.clear();
            return outputs;
        }
    }

    public void batchTransform(List<Record> batchOutputs) {
        // the batchOutputs from parameter is not the same as this.outputs
        for (Transformer transformer : transformers) {
            transformer.batchExecute(batchOutputs);
        }
    }

    public static boolean isTransformer(String name) {
        return extension.isTransformer(name) || TRANSFORMERS.containsKey(name);
    }

    @Override
    public CommandResponse handleCommand(String command, Map<String, Object> args) {
        for (Transformer transformer : transformers) {
            CommandResponse transformerResponse = transformer.handleCommand(command, args);
            if (null != transformerResponse) {
                return transformerResponse;
            }
        }
        return null;
    }
}
