package com.tencent.trt.executor.transformer;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import javassist.ClassMap;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.filter.ExpressionToJavassist;
import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.utils.JsonUtils;

/**
 * Created by wentao on 2/10/15.
 */
public abstract class ExprTransformer extends Transformer {

    public static Logger LOGGER = LoggerFactory.getLogger(ExprTransformer.class);
    private final ResultField resultField;

    public ExprTransformer(ResultTable resultTable, ResultField resultField) {
        this.resultField = resultField;
    }

    @Override
    public void execute(Record input, List<Record> outputs) {
        for (Record output : outputs) {
            output.set(resultField.field, transformSingleField(input));
        }
    }

    protected abstract Object transformSingleField(Record input);

    protected Object asObject(int i) {
        return Integer.valueOf(i);
    }

    protected Object asObject(double i) {
        return Double.valueOf(i);
    }

    protected Object asObject(long i) {
        return Long.valueOf(i);
    }

    protected Object asObject(boolean i){
        return Boolean.valueOf(i);
    }

    protected Object asObject(Object i) {
        return i;
    }

    public static Class generateTransformerClass(ResultTable resultTable, ResultField resultField) throws Exception {
        Map<String, Object> args = JsonUtils.readMap(new ObjectMapper(), resultField.processorArgs);
        String expr = (String) args.get("expr");
        String javassistSource = "return asObject(" + ExpressionToJavassist.transform(resultTable, resultField, expr) + ");";
        ClassPool cp = ClassPool.getDefault();
        cp.importPackage("com.tencent.trt.executor.model");
        String transformerClassName = resultTable.resultTableId +
                "_" + resultField.field +
                "_" + UUID.randomUUID().toString().split("-")[0];
        CtClass ctClass = cp.makeClass("com.tencent.trt.generated_transformer." + transformerClassName);
        CtClass ctExprTransformerClass = cp.getCtClass(ExprTransformer.class.getName());
        CtMethod ctTransformSingleFieldMethod = ctExprTransformerClass.getDeclaredMethod("transformSingleField");
        ctClass.setSuperclass(ctExprTransformerClass);
        CtMethod ctMethod = CtNewMethod.copy(ctTransformSingleFieldMethod, ctClass, new ClassMap());
        ctMethod.setBody(javassistSource);
        ctClass.addMethod(ctMethod);
        return ctClass.toClass();
    }
}
