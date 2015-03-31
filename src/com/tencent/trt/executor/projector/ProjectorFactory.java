package com.tencent.trt.executor.projector;

import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wentao on 1/8/15.
 */
public class ProjectorFactory {

    public static Logger LOGGER = LoggerFactory.getLogger(ProjectorFactory.class);

    private final static Map<String, Class> PROJECTORS = new HashMap<String, Class>() {
        {
            put("count", Count.class);
            put("avg", Avg.class);
        }
    };
    private static ProjectorExtension extension;

    private static final ConcurrentHashMap<String, Class> exprGeneratedClasses = new ConcurrentHashMap<String, Class>();

    static {
        extension = createExtension();
    }

    private static ProjectorExtension createExtension() {
        try {
            Class<?> clazz = ProjectorFactory.class.forName("com.tencent.application.projector.ApplicationProjectors");
            try {
                return (ProjectorExtension) clazz.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("failed to create instance: " + clazz);
            }
        } catch (ClassNotFoundException e) {
            // ignore
            return null;
        }
    }

    public static List<Projector> createProjectors(ResultTable resultTable) {
        try {
            ArrayList<Projector> projectors = new ArrayList<Projector>();
            for (ResultField resultField : resultTable.fields) {
                if (resultField.isDimension) {
                    continue;
                }
                if (resultField.processor.startsWith("=>")) {
                    continue;
                }
                Class projectorClass = getProjectorClass(resultTable, resultField);
                Projector projector = (Projector) projectorClass.getConstructor(ResultTable.class, ResultField.class).newInstance(resultTable, resultField);
                projectors.add(projector);
            }
            return projectors;
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private static Class getProjectorClass(ResultTable resultTable, ResultField resultField) throws Exception {
        Class projectorClass = PROJECTORS.get(resultField.processor);
        if (projectorClass != null) {
            return projectorClass;
        }
        if (null != extension) {
            projectorClass = extension.getProjectorClass(resultField.processor);
            if (projectorClass != null) {
                return projectorClass;
            }
        }
        synchronized (exprGeneratedClasses) {
            String cacheKey = resultTable.resultTableId + "_" + resultField.field;
            if (exprGeneratedClasses.containsKey(cacheKey)) {
                return exprGeneratedClasses.get(cacheKey);
            }
            Class clazz = ExprProjector.generateProjectorClass(resultTable, resultField);
            if (null == clazz) {
                throw new RuntimeException("projector not found: " + resultField.processor);
            } else {
                exprGeneratedClasses.put(cacheKey, clazz);
                return clazz;
            }
        }
    }
}
