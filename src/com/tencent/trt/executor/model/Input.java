package com.tencent.trt.executor.model;

import java.io.Serializable;

/**
 * Created by wentao on 2/7/15.
 */
public class Input implements Serializable {

	public final String inputType;
	public final String inputSource;

	public Input(String inputType, String inputSource) {
		this.inputType = inputType;
		this.inputSource = inputSource;
	}
}
