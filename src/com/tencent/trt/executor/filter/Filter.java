package com.tencent.trt.executor.filter;

import com.tencent.trt.executor.model.*;
import javassist.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.UUID;

/**
 * Created by wentao on 1/18/15.
 */
public abstract class Filter implements Serializable, CommandHandler {

    public static Logger LOGGER = LoggerFactory.getLogger(Filter.class);
    private static ExpressionExtension filterExtensions;

    static {
        filterExtensions = createFilterExtensions();
    }

    private static ExpressionExtension createFilterExtensions() {
        try {
            Class<?> clazz = Filter.class.forName("com.tencent.application.filter.ApplicationFilterExtensions");
            try {
                return (ExpressionExtension) clazz.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("failed to create instance: " + clazz);
            }
        } catch (ClassNotFoundException e) {
            // ignore
            return null;
        }
    }

    private final ResultTable resultTable;
    private final ResultField resultField;

    public Filter(ResultTable resultTable, ResultField resultField) {
        this.resultTable = resultTable;
        this.resultField = resultField;
    }

    public abstract boolean shouldPass(Record input);

    public ResultTable getResultTable() {
        return resultTable;
    }

    public ResultField getResultField() {
        return resultField;
    }

    public static Filter createFilter(ResultTable resultTable, ResultField resultField) throws Exception {
        String expr = resultField.filter;
        String javassistSource = String.format("return %s;",
                ExpressionToJavassist.transform(resultTable, resultField, expr, filterExtensions));
        ClassPool cp = ClassPool.getDefault();
        cp.importPackage("com.tencent.trt.executor.model");
        cp.importPackage("com.tencent.trt.executor.filter");
        String filterClassName = resultTable.resultTableId +
                "_" + resultField.field +
                "_" + UUID.randomUUID().toString().split("-")[0];
        CtClass ctClass = cp.makeClass("com.tencent.trt.generated_filter." + filterClassName);
        CtClass ctFilterClass = cp.getCtClass(Filter.class.getName());
        CtMethod ctShouldPassMethod = ctFilterClass.getDeclaredMethod("shouldPass");
        ctClass.setSuperclass(ctFilterClass);
        CtMethod ctMethod = CtNewMethod.copy(ctShouldPassMethod, ctClass, new ClassMap());
        ctMethod.setBody(javassistSource);
        ctClass.addMethod(ctMethod);
        Constructor ctor = ctClass.toClass().getConstructor(ResultTable.class, ResultField.class);
        return (Filter) ctor.newInstance(resultTable, resultField);
    }

    @Override
    public CommandResponse handleCommand(String command, Map<String, Object> args) {
        return null;
    }
}
