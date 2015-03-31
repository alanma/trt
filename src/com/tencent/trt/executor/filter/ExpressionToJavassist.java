package com.tencent.trt.executor.filter;

import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.executor.model.Upstream;
import freemarker.template.Configuration;
import freemarker.template.Template;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wentao on 1/19/15.
 */
public class ExpressionToJavassist {
	private static Map<String, TypeCaster> TYPE_CASTERS = new HashMap<String, TypeCaster>() {
		{
			put("int", new AsPrimitiveInt());
			put("timestamp", new AsPrimitiveInt());
			put("double", new AsPrimitiveDouble());
			put("long", new AsPrimitiveLong());
			put("String", new AsString());
			put("string", new AsString());
			put("boolean", new AsPrimitiveBoolean());
		}
	};

	public static String transform(ResultTable resultTable, ResultField resultField, String expr) throws Exception {
        return transform(resultTable, resultField, expr, null);
    }

	public static String transform(
            ResultTable resultTable, ResultField resultField, String expr,
            ExpressionExtension extension) throws Exception {

		Configuration cfg = new Configuration(Configuration.VERSION_2_3_21);
		cfg.setDefaultEncoding("UTF-8");
		Template template = new Template("source", expr, cfg);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		HashMap model = new HashMap();
		for (Upstream parent : resultTable.parents) {
			for (ResultField field : parent.parentResultTable.fields) {
				TypeCaster typeCaster = TYPE_CASTERS.get(field.type);
                if (null == typeCaster) {
                    throw new RuntimeException("unsupported type: " + field.type);
                }
				model.put(field.field, typeCaster.transform(field.field, String.format("$1.get(\"%s\")", field.field)));
			}
		}
		if (resultField.origins.length == 1) {
            Object me = model.get(resultField.getSingleOriginField());
            if (null == me) {
                throw new RuntimeException("me not found in the field: " + resultField.getSingleOriginField());
            }
            model.put("me", me);
		}
        if (null != extension) {
            extension.extend(model);
        }
		template.process(model, new OutputStreamWriter(baos));
        return baos.toString("UTF-8");
	}

    private static class AsPrimitiveInt extends TypeCaster {

		public String transform(String field, String fieldExpr) {
			return String.format("(com.tencent.trt.executor.filter.TypeCaster.asInt('%s', %s))".replace("'", "\""), field, fieldExpr);
		}
	}

	private static class AsPrimitiveDouble extends TypeCaster {

		public String transform(String field, String fieldExpr) {
			return String.format("(com.tencent.trt.executor.filter.TypeCaster.asDouble('%s', %s))".replace("'", "\""), field, fieldExpr);
		}
	}

	private static class AsPrimitiveLong extends TypeCaster {

		public String transform(String field, String fieldExpr) {
			return String.format("(com.tencent.trt.executor.filter.TypeCaster.asLong('%s', %s))".replace("'", "\""), field, fieldExpr);
		}
	}

	private static class AsString extends TypeCaster {

		public String transform(String field, String fieldExpr) {
			return String.format("(com.tencent.trt.executor.filter.TypeCaster.asString('%s', %s))".replace("'", "\""), field, fieldExpr);
		}
	}

	private static class AsPrimitiveBoolean extends TypeCaster {

		public String transform(String field, String fieldExpr) {
			return String.format("(com.tencent.trt.executor.filter.TypeCaster.asBoolean('%s', %s))".replace("'", "\""), field, fieldExpr);
		}
	}
}
