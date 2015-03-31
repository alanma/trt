package com.tencent.trt.executor.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by wentao on 1/17/15.
 */
public abstract class CollectorDataParser extends Transformer {

    public static Logger LOGGER = LoggerFactory.getLogger(CollectorDataParser.class);
    private final boolean shouldDecode;
    private final String encoding;
    private final String originFieldName;
    private final List<Exception> exceptions = new ArrayList<Exception>();
    protected final boolean discardsRecordOnError;
    private final boolean shouldUnGzip;
    private static int BUFSIZE = 1024;
    private ResultTable resultTable;

    public static interface TypeConverter extends Serializable {
        Object convert(Object input);
    }

    private static Map<List<String>, TypeConverter> ONE_TYPE_TO_ANOTHER = new HashMap<List<String>, TypeConverter>() {{
        put(Arrays.asList("string", "=>", "int"), new TypeConverter() {
            @Override
            public Object convert(Object input) {
                return Integer.parseInt(((String) input).trim());
            }
        });
        put(Arrays.asList("string", "=>", "double"), new TypeConverter() {
            @Override
            public Object convert(Object input) {
                return Double.parseDouble(((String) input).trim());
            }
        });
        put(Arrays.asList("string", "=>", "float"), new TypeConverter() {
            @Override
            public Object convert(Object input) {
                return Float.parseFloat(((String) input).trim());
            }
        });
        put(Arrays.asList("string", "=>", "long"), new TypeConverter() {
            @Override
            public Object convert(Object input) {
                return Long.parseLong(((String) input).trim());
            }
        });
    }};

    private static Map<String, TypeConverter> AS_FORCE_CONVERTER = new HashMap<String, TypeConverter>() {{
        put("string", new TypeConverter() {
            @Override
            public Object convert(Object input) {
                return input;
            }
        });
        put("int", new TypeConverter() {
            @Override
            public Object convert(Object input) {
                return Integer.valueOf(input.toString().trim());
            }
        });
        put("long", new TypeConverter() {
            @Override
            public Object convert(Object input) {
                return Long.valueOf(input.toString().trim());
            }
        });
        put("double", new TypeConverter() {
            @Override
            public Object convert(Object input) {
                return Double.valueOf(input.toString().trim());
            }
        });
        put("float", new TypeConverter() {
            @Override
            public Object convert(Object input) {
                return Float.valueOf(input.toString().trim());
            }
        });
    }};

    private static TypeConverter TO_STRING_CONVERTER = new TypeConverter() {
        @Override
        public Object convert(Object input) {
            return input == null ? null : input.toString().trim();
        }
    };

    public static TypeConverter AS_IS_CONVERTER = new TypeConverter() {
        @Override
        public Object convert(Object input) {
            return input;
        }
    };

    public static TypeConverter getConverter(
            String fromType, String toType,
            String fieldName, Map<String, Object> fieldArgs) {
        if (null == fromType) {
            return AS_IS_CONVERTER;
        }
        if (fromType.equals(toType)) {
            return AS_FORCE_CONVERTER.get(fromType);
        }
        if ("string".equals(toType)) {
            return TO_STRING_CONVERTER;
        }
        if ("timestamp".equals(toType)) {
            if ("string".equals(fromType)) {
                String format = (String) fieldArgs.get("format");
                if (null == format) {
                    format = "yyyy-MM-dd HH:mm:ss";
                }
                String timeZone = (String) fieldArgs.get("time_zone");
                if (null == timeZone) {
                    timeZone = "Asia/Shanghai";
                }
                final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
                simpleDateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
                return new TypeConverter() {
                    @Override
                    public Object convert(Object input) {
                        try {
                            Date date = simpleDateFormat.parse((String) input);
                            Integer timestamp = (int) (date.getTime() / 1000);
                            return timestamp;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
            } else {
                return getConverter(fromType, "int", fieldName, fieldArgs);
            }
        }
        TypeConverter typeConverter = ONE_TYPE_TO_ANOTHER.get(Arrays.asList(fromType, "=>", toType));
        if (null == typeConverter) {
            throw new RuntimeException("do not know how to convert field " + fieldName + " from " + fromType + " => " + toType);
        }
        return typeConverter;
    }

    public static interface FieldSetter extends Serializable {
        void set(Record output, Object val);
    }

    public CollectorDataParser(ResultTable resultTable, ResultField resultField) {
        this.resultTable = resultTable;
        try {
            Map<String, Object> args = new ObjectMapper().readValue(resultField.processorArgs, HashMap.class);
            if (args.containsKey("should_decode")) {
                shouldDecode = (Boolean) args.get("should_decode");
            } else {
                shouldDecode = true;
            }
            if (args.containsKey("discards_record_on_error")) {
                discardsRecordOnError = (Boolean) args.get("discards_record_on_error");
            } else {
                discardsRecordOnError = true;
            }
            if (args.containsKey("encoding")) {
                encoding = (String) args.get("encoding");
            } else {
                encoding = "UTF8";
            }
            if (args.containsKey("should_ungzip")) {
                shouldUnGzip = (Boolean) args.get("should_ungzip");
            } else {
                shouldUnGzip = false;
            }
            originFieldName = resultField.getSingleOriginField();
            init(resultTable, args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void init(ResultTable resultTable, Map<String, Object> args) throws Exception;

    @Override
    public void execute(Record input, List<Record> outputs) {
        exceptions.clear();
        String rawStr;
        try {
            rawStr = decode(input);
        } catch (Exception e) {
            exceptions.add(e);
            logBadInput(null, outputs, exceptions);
            return;
        }
        parse(rawStr, outputs, exceptions);
        if (!exceptions.isEmpty() && discardsRecordOnError) {
            logBadInput(rawStr, outputs, exceptions);
            outputs.clear();
        }
    }

    protected abstract void parse(String rawStr, List<Record> outputs, List<Exception> exceptions);

    protected String decode(Record input) throws Exception{
        byte[] rawData = null;
        if (shouldUnGzip) {
            rawData = unGzip((byte[]) input.get(originFieldName));
        }
        if (shouldDecode) {
            //byte[] rawData = (byte[]) input.get(originFieldName);
            if(shouldUnGzip) {
                return new String(rawData, encoding).trim();
            } else {
                return new String((byte[])input.get(originFieldName), encoding).trim();
            }
        } else {
            return ((String) input.get(originFieldName)).trim();
        }
    }

    private byte[] unGzip(byte[] msgGzip) throws Exception {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(msgGzip);
        try {
            GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
            try {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                try {
                    byte[] dataBuffer = new byte[BUFSIZE];
                    int numCount;
                    while ((numCount = gzipInputStream.read(dataBuffer, 0, dataBuffer.length)) != -1) {
                        byteArrayOutputStream.write(dataBuffer, 0, numCount);
                    }
                    return byteArrayOutputStream.toByteArray();
                } finally {
                    byteArrayOutputStream.close();
                }
            } finally {
                gzipInputStream.close();
            }
        } finally {
            byteArrayInputStream.close();
        }
    }

    protected FieldSetter createFieldSetter(final ResultField outputField, final String inputType) throws Exception {
        final String fieldName = outputField.field;
        Map<String, Object> fieldArgs = new HashMap<String, Object>();
        if (!StringUtils.isBlank(outputField.processorArgs)) {
            fieldArgs = new ObjectMapper().readValue(outputField.processorArgs, HashMap.class);
        }
        final TypeConverter converter = getConverter(inputType, outputField.type, fieldName, fieldArgs);
        return new FieldSetter() {
            @Override
            public void set(Record output, Object val) {
                Object value = converter.convert(val);
                // TODO: check every message might be expensive
                if (value != null) {
                    checkType(fieldName, outputField.type, value);
                }
                output.set(fieldName, value);
            }
        };
    }

    protected void checkType(String fieldName, String outputType, Object val) {
        if ("string".equalsIgnoreCase(outputType)) {
            if (!(val instanceof String)) {
                throw new RuntimeException("type error:" + fieldName + " is " + val.getClass() + ", not " + outputType);
            }
        } else if ("int".equalsIgnoreCase(outputType)) {
            if (!(val instanceof Integer)) {
                throw new RuntimeException("type error:" + fieldName + " is " + val.getClass() + ", not " + outputType);
            }
        } else if ("float".equalsIgnoreCase(outputType)) {
            if (!(val instanceof Float)) {
                throw new RuntimeException("type error:" + fieldName + " is " + val.getClass() + ", not " + outputType);
            }
        } else if ("double".equalsIgnoreCase(outputType)) {
            if (!(val instanceof Double)) {
                throw new RuntimeException("type error:" + fieldName + " is " + val.getClass() + ", not " + outputType);
            }
        }
    }

    protected void logBadInput(String input, List<Record> outputs, List<Exception> exceptions) {
        logBadInput(input, outputs, 0, outputs.size(), exceptions);
    }

    protected void logBadInput(String input, List<Record> outputs, int i, int j, List<Exception> exceptions) {
        StringBuilder builder = new StringBuilder();
        builder.append("skip bad input\n");
        builder.append("input: ");
        builder.append(input);
        builder.append('\n');
        for (int k = i; k < j; k++) {
            builder.append("output: ");
            builder.append(outputs.get(k).toString());
            builder.append('\n');
        }
        for (Exception exception : exceptions) {
            builder.append("exception: ");
            builder.append(exception.toString());
            builder.append('\n');
        }
        LOGGER.debug(builder.toString());
    }
}
