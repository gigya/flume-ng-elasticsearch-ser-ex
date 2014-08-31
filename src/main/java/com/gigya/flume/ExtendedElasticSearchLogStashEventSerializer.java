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

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.SERIALIZER;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.lucene.analysis.compound.DictionaryCompoundWordTokenFilter;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * An extended serializer for flume events into the same format LogStash uses</p>
 * This adds some more features on top of the default ES serializer that is part of 
 * the Flume distribution.</p>
 * For more details see: https://github.com/gigya/flume-ng-elasticsearch-ser-ex 
 * </p>
 * Logstash format:
 * 
 * <pre>
 * {
 *    "@timestamp": "2010-12-21T21:48:33.309258Z",
 *    "@tags": [ "array", "of", "tags" ],
 *    "@type": "string",
 *    "@source": "source of the event, usually a URL."
 *    "@source_host": ""
 *    "@source_path": ""
 *    "@fields":{
 *       # a set of fields for this event
 *       "user": "jordan",
 *       "command": "shutdown -r":
 *     }
 *     "@message": "the original plain-text message"
 *   }
 * </pre>
 * 
 * If the following headers are present, they will map to the above logstash
 * output as long as the logstash fields are not already present.</p>
 * 
 * <pre>
 *  message : String -> @message : String 
 *     or body : String -> @message : String     
 *  timestamp: long -> @timestamp:Date
 *  host: String -> @source_host: String
 *  src_path: String -> @source_path: String
 *  type: String -> @type: String
 *  source: String -> @source: String
 * </pre>
 * 
 *      
 * @author Rotem Hermon
 */
public class ExtendedElasticSearchLogStashEventSerializer implements ElasticSearchEventSerializer {

	/**
	 * Configuration property to set fields that might contain a JSON string, to be
	 * parsed as an object
	 */
	public static final String OBJECT_FIELDS = "objectFields";
	/**
	 * Configuration property, set to true to remove the logstash '@fields' prefix
	 * for custom fields
	 */
	public static final String REMOVE_FIELDS_PREFIX = "removeFieldsPrefix";
	/**
	 * Configuration property, set to true to collect dot notated field names into an 
	 * object (so 'params.f1' and 'params.f2' will be turned when indexed into
	 * an object: { params : {f1 : ... , f2 : ... } }  
	 */
	public static final String COLLATE_OBJECTS = "collateObjects";

	private Map<String, Boolean> objectFields = null;
	private boolean removeFieldsPrefix = false;
	private boolean collateObjects = false;
	
	@Override
	public XContentBuilder getContentBuilder(Event event) throws IOException {
		XContentBuilder builder = jsonBuilder().startObject();
		appendHeaders(builder, event);
		return builder;
	}

	private void appendBody(XContentBuilder builder, Event event) throws IOException, UnsupportedEncodingException {
		byte[] body = event.getBody();
		ContentBuilderUtilEx.appendField(builder, "@message", body, isObjectField("body"));
	}

	private void appendHeaders(XContentBuilder builder, Event event) throws IOException {
		Map<String, String> headers = Maps.newHashMap(event.getHeaders());
		Map<String, Object> collatedFields = null;
		if (collateObjects)
			collatedFields = Maps.newHashMap();

		// look for a "message" header and append as body if exists
		String message = headers.get("message");
		if (!StringUtils.isBlank(message) && StringUtils.isBlank(headers.get("@message"))) {
			ContentBuilderUtilEx.appendField(builder, "@message", message.getBytes(charset), isObjectField("message"));
			headers.remove("message");
		} else {
			// if not, append the body as the message
			appendBody(builder, event);
		}

		String timestamp = headers.get("timestamp");
		if (!StringUtils.isBlank(timestamp) && StringUtils.isBlank(headers.get("@timestamp"))) {
			long timestampMs = Long.parseLong(timestamp);
			builder.field("@timestamp", new Date(timestampMs));
			headers.remove("timestamp");
		}

		String source = headers.get("source");
		if (!StringUtils.isBlank(source) && StringUtils.isBlank(headers.get("@source"))) {
			ContentBuilderUtilEx.appendField(builder, "@source", source.getBytes(charset));
			headers.remove("source");
		}

		String type = headers.get("type");
		if (!StringUtils.isBlank(type) && StringUtils.isBlank(headers.get("@type"))) {
			ContentBuilderUtilEx.appendField(builder, "@type", type.getBytes(charset));
			headers.remove("type");
		}

		String host = headers.get("host");
		if (!StringUtils.isBlank(host) && StringUtils.isBlank(headers.get("@source_host"))) {
			ContentBuilderUtilEx.appendField(builder, "@source_host", host.getBytes(charset));
			headers.remove("host");
		}

		String srcPath = headers.get("src_path");
		if (!StringUtils.isBlank(srcPath) && StringUtils.isBlank(headers.get("@source_path"))) {
			ContentBuilderUtilEx.appendField(builder, "@source_path", srcPath.getBytes(charset));
			headers.remove("src_path");
		}

		if (!removeFieldsPrefix)
			builder.startObject("@fields");
		for (String key : headers.keySet()) {
			if (collateObjects) {
				collectField(key, headers.get(key), collatedFields);
			} else {
				byte[] val = headers.get(key).getBytes(charset);
				ContentBuilderUtilEx.appendField(builder, key, val, isObjectField(key));
			}
		}
		if (collateObjects) {
			for (String fieldName : collatedFields.keySet()) {
				ContentBuilderUtilEx.appendField(builder, fieldName, collatedFields.get(fieldName));
			}
		}
		if (!removeFieldsPrefix)
			builder.endObject();
	}

	private void collectField(String key, String val, Map<String, Object> fields) {
		// see if we have an object dot notation
		int pos = key.indexOf('.');
		if (pos > 0) {
			// this is an object field. get the field name
			String fieldName = key.substring(0, pos);
			String rest = key.substring(pos + 1);
			// get the field object. create a new map if not already there
			Map<String, Object> fieldMap = getFieldMap(fieldName, fields, true);
			// if the field was already set as a primitive type just write this
			// one
			// as a regular field and not as an object
			if (null == fieldMap) {
				fields.put(key, val);
			} else {
				// process the rest of the field
				collectField(rest, val, fieldMap);
			}
		} else {
			// this is a regular field, add the value
			// check that this not overrides an existing object
			Map<String, Object> fieldMap = getFieldMap(key, fields, false);
			if (null == fieldMap) {
				fields.put(key, val);
			}
			else {
				// if this field is configured as an object field, we may have a JSON 
				// string here
				if (isObjectField(key)){
					Map<String,Object> valMap = ContentBuilderUtilEx.tryParsingToMap(val);
					if (null != valMap){
						fieldMap.putAll(valMap);
					}
				}
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, Object> getFieldMap(String key, Map<String, Object> fields, boolean createNew) {
		Map<String, Object> fieldMap = null;
		Object field = fields.get(key);
		if (null == field) {
			if (createNew) {
				fieldMap = Maps.newHashMap();
				fields.put(key, fieldMap);
			}
		} else if (field instanceof Map) {
			fieldMap = (Map<String, Object>) field;
		}
		return fieldMap;
	}

	
	
	private boolean isObjectField(String fieldName) {
		if (null != objectFields && null != fieldName) {
			if (objectFields.containsKey(fieldName))
				return true;
		}
		return false;
	}

	@Override
	public void configure(Context context) {
		// look for the objectFields configuration
		if (StringUtils.isNotBlank(context.getString(OBJECT_FIELDS))) {
			String fields = context.getString(OBJECT_FIELDS);
			if (null != fields) {
				objectFields = new HashMap<String, Boolean>();
				String[] splitted = fields.split(",");
				for (int i = 0; i < splitted.length; i++) {
					String field = splitted[i].trim();
					if (!field.isEmpty())
						objectFields.put(field, true);
				}
			}
		}
		if (StringUtils.isNotBlank(context.getString(REMOVE_FIELDS_PREFIX))) {
			String remove = context.getString(REMOVE_FIELDS_PREFIX);
			if ("true".equalsIgnoreCase(remove) || "1".equalsIgnoreCase(remove)) {
				removeFieldsPrefix = true;
			}
		}
		if (StringUtils.isNotBlank(context.getString(COLLATE_OBJECTS))) {
			String remove = context.getString(COLLATE_OBJECTS);
			if ("true".equalsIgnoreCase(remove) || "1".equalsIgnoreCase(remove)) {
				collateObjects = true;
			}
		}
	}

	@Override
	public void configure(ComponentConfiguration conf) {
		// NO-OP...
	}
}
