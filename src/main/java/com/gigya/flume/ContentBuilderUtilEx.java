/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.gigya.flume;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import org.elasticsearch.common.jackson.core.JsonParseException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * Utility methods for using ElasticSearch {@link XContentBuilder}
 */
public class ContentBuilderUtilEx {

	private static final Charset charset = Charset.defaultCharset();

	private ContentBuilderUtilEx() {
	}

	public static void appendField(XContentBuilder builder, String field, Object data) throws IOException {
		builder.field(field, data);
	}

	public static void appendField(XContentBuilder builder, String field, byte[] data) throws IOException {
		appendField(builder, field, data, false);
	}

	public static void appendField(XContentBuilder builder, String field, byte[] data, boolean allowObject)
			throws IOException {
		XContentType contentType = XContentFactory.xContentType(data);
		if (contentType == null || !allowObject) {
			addSimpleField(builder, field, data);
		} else {
			addComplexField(builder, field, contentType, data);
		}
	}

	public static void addSimpleField(XContentBuilder builder, String fieldName, byte[] data) throws IOException {
		builder.field(fieldName, new String(data, charset));
	}

	public static void addComplexField(XContentBuilder builder, String fieldName, XContentType contentType, byte[] data)
			throws IOException {
		XContentParser parser = null;
		try {
			parser = XContentFactory.xContent(contentType).createParser(data);
			Map<String, Object> map = parser.map();
			builder.field(fieldName, map);
		} catch (JsonParseException ex) {
			// If we get an exception here the most likely cause is nested JSON
			// that can't be figured out in the body. At this point just push it
			// through as is, we have already added the field so don't do it again
			addSimpleField(builder, fieldName, data);
		} finally {
			if (parser != null) {
				parser.close();
			}
		}
	}

	public static Map<String, Object> tryParsingToMap(String data) {
		XContentType contentType = XContentFactory.xContentType(data);
		if (null != contentType) {
			XContentParser parser = null;
			try {
				parser = XContentFactory.xContent(contentType).createParser(data);
				Map<String, Object> map = parser.map();
				return map;
			} catch (IOException ex) {
			} finally {
				if (parser != null) {
					parser.close();
				}
			}
		}
		return null;
	}
	
}
