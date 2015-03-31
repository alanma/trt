package com.tencent.trt.utils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class HttpUtils {

	public static String post(String request, String body) throws Exception {
			URL url = new URL(request);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			try {
				connection.setDoOutput(true);
				connection.setDoInput(true);
				connection.setInstanceFollowRedirects(false);
				connection.setRequestMethod("POST");
				connection.setRequestProperty("charset", "utf-8");
				connection.setRequestProperty("Content-Length", "" + Integer.toString(body.getBytes().length));
				connection.setUseCaches(false);
				connection.setConnectTimeout(3000);
				connection.setReadTimeout(60000);
				DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
				try {
					wr.writeBytes(body);
					wr.flush();
				} finally {
					wr.close();
				}
				return handleResponse(connection);
			} finally {
				connection.disconnect();
			}
	}

	public static String get(String request) {
        try {
            URL url = new URL(request);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            try {
                connection.setDoOutput(true);
                connection.setDoInput(true);
                connection.setInstanceFollowRedirects(false);
                connection.setRequestMethod("GET");
                connection.setRequestProperty("charset", "utf-8");
                connection.setUseCaches(false);
                connection.setConnectTimeout(3000);
                connection.setReadTimeout(60000);
                return handleResponse(connection);
            } finally {
                connection.disconnect();
            }
        } catch (HttpError e) {
        	throw new RuntimeException("failed to get: " + request, e);
        } catch (Exception e) {
            throw new RuntimeException("failed to get: " + request, e);
        }
	}

	private static String handleResponse(HttpURLConnection connection) throws Exception {
		int responseCode = connection.getResponseCode();
		if (responseCode >= 200 && responseCode < 300) {
			return readAll(connection.getInputStream());
		} else {
			throw new HttpError(responseCode, readAll(connection.getErrorStream()));
		}
	}

	private static String readAll(InputStream inputStream) throws Exception {
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF8"));
			StringBuilder stringBuilder = new StringBuilder();
			String line;
			while (null != (line = reader.readLine())) {
				stringBuilder.append(line);
			}
			return stringBuilder.toString();
		} finally {
			inputStream.close();
		}
	}

	@SuppressWarnings("serial")
	public static class HttpError extends RuntimeException {
		public final int responseCode;
		public final String output;

		public HttpError(int responseCode, String output) {
            super("[" + responseCode + "]: " + output);
			this.responseCode = responseCode;
			this.output = output;
		}

		@Override
		public String toString() {
			return "Error{" + "responseCode=" + responseCode + ", output='" + output + '\'' + '}';
		}
	}
}
