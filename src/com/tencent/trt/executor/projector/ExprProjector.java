package com.tencent.trt.executor.projector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.filter.ExpressionToJavassist;
import com.tencent.trt.executor.model.*;
import com.tencent.trt.utils.JsonUtils;
import javassist.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by wentao on 1/18/15.
 */
public abstract class ExprProjector extends Projector {

    public static Logger LOGGER = LoggerFactory.getLogger(ExprProjector.class);

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public ExprProjector(ResultTable resultTable, ResultField resultField) {
        super(resultTable, resultField);
    }

    @Override
    public void groupBuild(Record input) {
    }

    @Override
    public void groupEnd(List<Record> outputs,int windowStart,int windowEnd) {
        beforeOutput();
        for (Record output : outputs) {
            setOutput(output);
        }
    }

    protected void beforeOutput() {

    }

    protected abstract void setOutput(Record output);

    public static Class generateProjectorClass(ResultTable resultTable, final ResultField resultField) throws Exception {
        Map<String, Object> args = readArgs(resultField);
        if ("sum".equals(resultField.processor)) {
            args.clear();
            args.put("expr", String.format("%s += ${%s}", resultField.field, resultField.getSingleOriginField()));
            args.put("output", new ArrayList<String>() {{
                add(resultField.field);
            }});
        } else if ("max".equals(resultField.processor)) {
            args.clear();
            args.put("expr", String.format("%s = Math.max(%s, ${%s})", resultField.field, resultField.field, resultField.getSingleOriginField()));
            args.put("output", new ArrayList<String>() {{
                add(resultField.field);
            }});
        } else if ("min".equals(resultField.processor)) {
            args.clear();
            args.put("expr", String.format("%s = Math.min(%s, ${%s})", resultField.field, resultField.field, resultField.getSingleOriginField()));
            args.put("output", new ArrayList<String>() {{
                add(resultField.field);
            }});
        } else if ("projector".equals(resultField.processor)) {
            // nothing
        } else {
            return null;
        }
        List<String> output = (List<String>) args.get("output");
        List<ResultField> outputFields = new ArrayList<ResultField>();
        if (null == output) {
            outputFields.add(resultField);
        } else {
            for (String fieldName : output) {
                outputFields.add(resultTable.getField(fieldName));
            }
        }
        Map<String, String> temp = (Map<String, String>) args.get("temp");
        String expr = (String) args.get("expr");
        String beforeOutputExpr = (String) args.get("before_output");
        return makeClass(resultTable, resultField, outputFields, temp, expr, beforeOutputExpr);
    }

    private static Map<String, Object> readArgs(ResultField resultField) {
        if (null == resultField.processorArgs) {
            return new HashMap<String, Object>();
        }
        return JsonUtils.readMap(OBJECT_MAPPER, resultField.processorArgs);
    }

    // output: the list of fields to output, if not set will default to only output current field
    // expr: the expr to evaluate for every input
    // before_output: the expr to evaluate after the group end to prepare the final output
    // temp: if the calculation require some middle result, but do not want to include in the output
    private static Class makeClass(ResultTable resultTable, ResultField resultField, List<ResultField> outputFields, Map<String, String> temp, String expr, String beforeOutputExpr) throws Exception {
        ClassPool cp = ClassPool.getDefault();
        
        
        cp.importPackage("com.tencent.trt.executor.model");
        String filterClassName = resultTable.resultTableId + "_" + resultField.field + "_" + UUID.randomUUID().toString().split("-")[0];
        CtClass ctClass = cp.makeClass("com.tencent.trt.generated_projector." + filterClassName);
        CtClass ctExprProjectorClass = cp.getCtClass(ExprProjector.class.getName());
        addOutputFields(outputFields, ctClass);
        addTempFields(temp, ctClass);
        addGroupBuild(ExpressionToJavassist.transform(resultTable, resultField, expr), ctClass, ctExprProjectorClass);
        if (!StringUtils.isBlank(beforeOutputExpr)) {
            addBeforeOutput(ExpressionToJavassist.transform(resultTable, resultField, beforeOutputExpr), ctClass, ctExprProjectorClass);
        }
        addSetOutput(outputFields, ctClass, ctExprProjectorClass);
        return ctClass.toClass();

    }

    private static void addOutputFields(List<ResultField> outputFields, CtClass ctClass) throws Exception {
        for (ResultField field : outputFields) {
            String fieldType = field.type;
            if ("int".equals(fieldType)) {
                ctClass.addField(new CtField(CtClass.intType, field.field, ctClass));
            } else if ("long".equals(fieldType)) {
                ctClass.addField(new CtField(CtClass.longType, field.field, ctClass));
            } else if ("double".equals(fieldType)) {
                ctClass.addField(new CtField(CtClass.doubleType, field.field, ctClass));
            } else {
                throw new UnsupportedOperationException(field.field + " type not supported: " + fieldType);
            }
        }
    }

    private static void addTempFields(Map<String, String> temp, CtClass ctClass) throws Exception {
        if (null == temp) {
            return;
        }
        for (Map.Entry<String, String> entry : temp.entrySet()) {
            String fieldType = entry.getValue();
            if ("int".equals(fieldType)) {
                ctClass.addField(new CtField(CtClass.intType, entry.getKey(), ctClass));
            } else if ("long".equals(fieldType)) {
                ctClass.addField(new CtField(CtClass.longType, entry.getKey(), ctClass));
            } else if ("double".equals(fieldType)) {
                ctClass.addField(new CtField(CtClass.doubleType, entry.getKey(), ctClass));
            } else {
                throw new UnsupportedOperationException("type not supported: " + fieldType);
            }
        }
    }

    private static void addSetOutput(List<ResultField> outputFields, CtClass ctClass, CtClass ctExprProjectorClass) throws Exception {
        CtMethod ctSetOutputMethod = ctExprProjectorClass.getDeclaredMethod("setOutput");
        ctClass.setSuperclass(ctExprProjectorClass);
        CtMethod ctMethod = CtNewMethod.copy(ctSetOutputMethod, ctClass, new ClassMap());
        StringBuilder source = new StringBuilder();
        for (ResultField field : outputFields) {
            String outputField = field.field;
            String fieldType = field.type;
            if ("int".equals(fieldType)) {
                source.append(String.format("$1.set(\"%s\", Integer.valueOf(%s));\n", outputField, outputField));
            } else if ("long".equals(fieldType)) {
                source.append(String.format("$1.set(\"%s\", Long.valueOf(%s));\n", outputField, outputField));
            } else if ("double".equals(fieldType)) {
                source.append(String.format("$1.set(\"%s\", Double.valueOf(%s));\n", outputField, outputField));
            } else {
                throw new UnsupportedOperationException("type not supported: " + fieldType);
            }
        }
        ctMethod.setBody("{ " + source.toString() + " }");
        ctClass.addMethod(ctMethod);
    }

    private static void addGroupBuild(String source, CtClass ctClass, CtClass ctExprProjectorClass) throws Exception {
        CtMethod ctGroupBuildMethod = ctExprProjectorClass.getDeclaredMethod("groupBuild");
        ctClass.setSuperclass(ctExprProjectorClass);
        CtMethod ctMethod = CtNewMethod.copy(ctGroupBuildMethod, ctClass, new ClassMap());
        try {
        	ctMethod.setBody("{ " + source + ";" + " }");
        } catch (Exception e) {
        	throw new RuntimeException("====>source: " + source, e);
        }
        ctClass.addMethod(ctMethod);
    }

    private static void addBeforeOutput(String source, CtClass ctClass, CtClass ctExprProjectorClass) throws Exception {
        CtMethod ctBeforeOutputMethod = ctExprProjectorClass.getDeclaredMethod("beforeOutput");
        ctClass.setSuperclass(ctExprProjectorClass);
        CtMethod ctMethod = CtNewMethod.copy(ctBeforeOutputMethod, ctClass, new ClassMap());
        ctMethod.setBody("{ " + source + ";" + " }");
        ctClass.addMethod(ctMethod);
    }
}
