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

import static org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer.charset;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Test;

public class TestElasticSearchLogStashEventSerializer {

	@Test
	public void testRoundTrip() throws Exception {
		ExtendedElasticSearchLogStashEventSerializer fixture = new ExtendedElasticSearchLogStashEventSerializer();
		Context context = new Context();
		fixture.configure(context);

		String message = "test body";
		Map<String, String> headers = Maps.newHashMap();
		long timestamp = System.currentTimeMillis();
		headers.put("timestamp", String.valueOf(timestamp));
		headers.put("source", "flume_tail_src");
		headers.put("host", "test@localhost");
		headers.put("src_path", "/tmp/test");
		headers.put("headerNameOne", "headerValueOne");
		headers.put("headerNameTwo", "headerValueTwo");
		headers.put("type", "sometype");
		Event event = EventBuilder.withBody(message.getBytes(charset));
		event.setHeaders(headers);

		XContentBuilder expected = jsonBuilder().startObject();
		expected.field("@message", new String(message.getBytes(), charset));
		expected.field("@timestamp", new Date(timestamp));
		expected.field("@source", "flume_tail_src");
		expected.field("@type", "sometype");
		expected.field("@source_host", "test@localhost");
		expected.field("@source_path", "/tmp/test");
		expected.startObject("@fields");
		expected.field("headerNameTwo", "headerValueTwo");
		expected.field("headerNameOne", "headerValueOne");
		expected.endObject();

		expected.endObject();

		XContentBuilder actual = fixture.getXContentBuilder(event);
		assertEquals(new String(expected.bytes().array()), new String(actual.bytes().array()));
	}

	@Test
	public void shouldRemoveFieldsPrefix() throws Exception {
		ExtendedElasticSearchLogStashEventSerializer fixture = new ExtendedElasticSearchLogStashEventSerializer();
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("objectFields", "params, anotherField");
		parameters.put("removeFieldsPrefix", "true");
		Context context = new Context(parameters);
		fixture.configure(context);

		String message = "test body";
		Map<String, String> headers = Maps.newHashMap();
		long timestamp = System.currentTimeMillis();
		headers.put("timestamp", String.valueOf(timestamp));
		headers.put("source", "flume_tail_src");
		headers.put("host", "test@localhost");
		headers.put("src_path", "/tmp/test");
		headers.put("headerNameOne", "headerValueOne");
		headers.put("headerNameTwo", "headerValueTwo");
		headers.put("type", "sometype");
		Event event = EventBuilder.withBody(message.getBytes(charset));
		event.setHeaders(headers);

		XContentBuilder expected = jsonBuilder().startObject();
		expected.field("@message", new String(message.getBytes(), charset));
		expected.field("@timestamp", new Date(timestamp));
		expected.field("@source", "flume_tail_src");
		expected.field("@type", "sometype");
		expected.field("@source_host", "test@localhost");
		expected.field("@source_path", "/tmp/test");
		expected.field("headerNameTwo", "headerValueTwo");
		expected.field("headerNameOne", "headerValueOne");

		expected.endObject();

		XContentBuilder actual = fixture.getXContentBuilder(event);
		assertEquals(new String(expected.bytes().array()), new String(actual.bytes().array()));
	}

	// @Test
	public void shouldHandleInvalidJSONDuringComplexParsing() throws Exception {
		ExtendedElasticSearchLogStashEventSerializer fixture = new ExtendedElasticSearchLogStashEventSerializer();
		Context context = new Context();
		fixture.configure(context);

		String message = "{flume: somethingnotvalid}";
		Map<String, String> headers = Maps.newHashMap();
		long timestamp = System.currentTimeMillis();
		headers.put("timestamp", String.valueOf(timestamp));
		headers.put("source", "flume_tail_src");
		headers.put("host", "test@localhost");
		headers.put("src_path", "/tmp/test");
		headers.put("headerNameOne", "headerValueOne");
		headers.put("headerNameTwo", "headerValueTwo");
		headers.put("type", "sometype");
		Event event = EventBuilder.withBody(message.getBytes(charset));
		event.setHeaders(headers);

		XContentBuilder expected = jsonBuilder().startObject();
		expected.field("@message", new String(message.getBytes(), charset));
		expected.field("@timestamp", new Date(timestamp));
		expected.field("@source", "flume_tail_src");
		expected.field("@type", "sometype");
		expected.field("@source_host", "test@localhost");
		expected.field("@source_path", "/tmp/test");
		expected.startObject("@fields");
		expected.field("headerNameTwo", "headerValueTwo");
		expected.field("headerNameOne", "headerValueOne");
		expected.endObject();
		expected.endObject();

		XContentBuilder actual = fixture.getXContentBuilder(event);
		String expectedStr = new String(expected.bytes().array());
		String actualStr = new String(actual.bytes().array());
		assertEquals(expectedStr, actualStr);
	}

	@Test
	public void shouldHandleSONInHeaderAsRaw() throws Exception {
		ExtendedElasticSearchLogStashEventSerializer fixture = new ExtendedElasticSearchLogStashEventSerializer();
		Map<String, String> parameters = new HashMap<String, String>();
		Context context = new Context(parameters);
		fixture.configure(context);

		String message = "{flume: somethingnotvalid}";
		String params = "{\"cmd\":\"api.method\",\"email\":\"my@gmail.com\"}";
		Map<String, String> headers = Maps.newHashMap();
		long timestamp = System.currentTimeMillis();
		headers.put("timestamp", String.valueOf(timestamp));
		headers.put("params", params);
		Event event = EventBuilder.withBody(message.getBytes(charset));
		event.setHeaders(headers);

		XContentBuilder expected = jsonBuilder().startObject();
		expected.field("@message", new String(message.getBytes(), charset));
		expected.field("@timestamp", new Date(timestamp));
		expected.startObject("@fields");
		expected.field("params", params);
		expected.endObject();
		expected.endObject();

		XContentBuilder actual = fixture.getXContentBuilder(event);
		String expectedStr = new String(expected.bytes().array());
		String actualStr = new String(actual.bytes().array());
		assertEquals(expectedStr, actualStr);
	}

	@Test
	public void shouldParseJSONInHeader() throws Exception {
		ExtendedElasticSearchLogStashEventSerializer fixture = new ExtendedElasticSearchLogStashEventSerializer();
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("objectFields", "params, anotherField");
		Context context = new Context(parameters);
		fixture.configure(context);

		String message = "{flume: somethingnotvalid}";
		String params = "{\"cmd\":\"api.method\",\"email\":\"my@gmail.com\"}";
		Map<String, String> headers = Maps.newHashMap();
		long timestamp = System.currentTimeMillis();
		headers.put("timestamp", String.valueOf(timestamp));
		headers.put("params", params);
		Event event = EventBuilder.withBody(message.getBytes(charset));
		event.setHeaders(headers);

		XContentBuilder expected = jsonBuilder().startObject();
		expected.field("@message", new String(message.getBytes(), charset));
		expected.field("@timestamp", new Date(timestamp));
		expected.startObject("@fields");
		expected.startObject("params");
		expected.field("cmd", "api.method");
		expected.field("email", "my@gmail.com");
		expected.endObject();
		expected.endObject();
		expected.endObject();

		XContentBuilder actual = fixture.getXContentBuilder(event);
		String expectedStr = new String(expected.bytes().array());
		String actualStr = new String(actual.bytes().array());
		assertEquals(expectedStr, actualStr);
	}
	
	@Test
	public void shouldParseNestedJSONInHeader() throws Exception {
		ExtendedElasticSearchLogStashEventSerializer fixture = new ExtendedElasticSearchLogStashEventSerializer();
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("objectFields", "params, anotherField");
		Context context = new Context(parameters);
		fixture.configure(context);

		String message = "{flume: somethingnotvalid}";
		String params = "{\"cmd\":\"api.method\",\"email\":\"my@gmail.com\", \"nested\" : { \"sub\" : 1 }}";
		Map<String, String> headers = Maps.newHashMap();
		long timestamp = System.currentTimeMillis();
		headers.put("timestamp", String.valueOf(timestamp));
		headers.put("params", params);
		Event event = EventBuilder.withBody(message.getBytes(charset));
		event.setHeaders(headers);

		XContentBuilder expected = jsonBuilder().startObject();
		expected.field("@message", new String(message.getBytes(), charset));
		expected.field("@timestamp", new Date(timestamp));
		expected.startObject("@fields");
		expected.startObject("params");
		expected.field("cmd", "api.method");
		expected.startObject("nested");
		expected.field("sub", 1);
		expected.endObject();
		expected.field("email", "my@gmail.com");
		expected.endObject();
		expected.endObject();
		expected.endObject();

		XContentBuilder actual = fixture.getXContentBuilder(event);
		String expectedStr = new String(expected.bytes().array());
		String actualStr = new String(actual.bytes().array());
		assertEquals(expectedStr, actualStr);
	}

	@Test
	public void shouldParseNestedJSONInHeaderWhenCollating() throws Exception {
		ExtendedElasticSearchLogStashEventSerializer fixture = new ExtendedElasticSearchLogStashEventSerializer();
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("objectFields", "params");
		parameters.put("collateObjects", "true");
		Context context = new Context(parameters);
		fixture.configure(context);

		String message = "{flume: somethingnotvalid}";
		String params = "{\"cmd\":\"api.method\",\"email\":\"my@gmail.com\", \"nested\" : { \"sub\" : 1 }}";
		Map<String, String> headers = Maps.newHashMap();
		long timestamp = System.currentTimeMillis();
		headers.put("timestamp", String.valueOf(timestamp));
		headers.put("params", params);
		Event event = EventBuilder.withBody(message.getBytes(charset));
		event.setHeaders(headers);

		XContentBuilder expected = jsonBuilder().startObject();
		expected.field("@message", new String(message.getBytes(), charset));
		expected.field("@timestamp", new Date(timestamp));
		expected.startObject("@fields");
		expected.startObject("params");
		expected.field("cmd", "api.method");
		expected.startObject("nested");
		expected.field("sub", 1);
		expected.endObject();
		expected.field("email", "my@gmail.com");
		expected.endObject();
		expected.endObject();
		expected.endObject();

		XContentBuilder actual = fixture.getXContentBuilder(event);
		String expectedStr = new String(expected.bytes().array());
		String actualStr = new String(actual.bytes().array());
		assertEquals(expectedStr, actualStr);
	}

	@Test
	public void shouldCollateJSONInHeader() throws Exception {
		ExtendedElasticSearchLogStashEventSerializer fixture = new ExtendedElasticSearchLogStashEventSerializer();
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("objectFields", "params, anotherField");
		parameters.put("collateObjects", "true");
		parameters.put("collateDepth", "-1");
		Context context = new Context(parameters);
		fixture.configure(context);

		String message = "{flume: somethingnotvalid}";
		Map<String, String> headers = Maps.newHashMap();
		long timestamp = System.currentTimeMillis();
		headers.put("timestamp", String.valueOf(timestamp));
		headers.put("params.cmd", "api.call");
		headers.put("params.email", "my@gmail.com");
		headers.put("params.timer.start", "1");
		headers.put("params.timer.end", "2");
		Event event = EventBuilder.withBody(message.getBytes(charset));
		event.setHeaders(headers);

		XContentBuilder expected = jsonBuilder().startObject();
		expected.field("@message", new String(message.getBytes(), charset));
		expected.field("@timestamp", new Date(timestamp));
		expected.startObject("@fields");
		expected.startObject("params");
		expected.field("cmd", "api.call");
		expected.field("email", "my@gmail.com");
		expected.startObject("timer");
		expected.field("start", "1");
		expected.field("end", "2");
		expected.endObject();
		expected.endObject();
		expected.endObject();
		expected.endObject();

		XContentBuilder actual = fixture.getXContentBuilder(event);
		String expectedStr = new String(expected.bytes().array());
		String actualStr = new String(actual.bytes().array());
		assertEquals(expectedStr, actualStr);
	}

	@Test
	public void shouldCollateJSONDefaultOneLevel() throws Exception {
		ExtendedElasticSearchLogStashEventSerializer fixture = new ExtendedElasticSearchLogStashEventSerializer();
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("objectFields", "params, anotherField");
		parameters.put("collateObjects", "true");
		Context context = new Context(parameters);
		fixture.configure(context);

		String message = "{flume: somethingnotvalid}";
		Map<String, String> headers = Maps.newHashMap();
		long timestamp = System.currentTimeMillis();
		headers.put("timestamp", String.valueOf(timestamp));
		headers.put("params.cmd", "api.call");
		headers.put("params.email", "my@gmail.com");
		headers.put("params.timer.start", "1");
		headers.put("params.timer.end", "2");
		Event event = EventBuilder.withBody(message.getBytes(charset));
		event.setHeaders(headers);

		XContentBuilder expected = jsonBuilder().startObject();
		expected.field("@message", new String(message.getBytes(), charset));
		expected.field("@timestamp", new Date(timestamp));
		expected.startObject("@fields");
		expected.startObject("params");
		expected.field("timer.start", "1");
		expected.field("cmd", "api.call");
		expected.field("email", "my@gmail.com");
		expected.field("timer.end", "2");
		expected.endObject();
		expected.endObject();
		expected.endObject();

		XContentBuilder actual = fixture.getXContentBuilder(event);
		String expectedStr = new String(expected.bytes().array());
		String actualStr = new String(actual.bytes().array());
		assertEquals(expectedStr, actualStr);
	}

	@Test
	public void shouldIgnoreInvalidCollateDepth() throws Exception {
		ExtendedElasticSearchLogStashEventSerializer fixture = new ExtendedElasticSearchLogStashEventSerializer();
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("objectFields", "params, anotherField");
		parameters.put("collateObjects", "true");
		parameters.put("collateDepth", "blabla");
		Context context = new Context(parameters);
		fixture.configure(context);

		String message = "{flume: somethingnotvalid}";
		Map<String, String> headers = Maps.newHashMap();
		long timestamp = System.currentTimeMillis();
		headers.put("timestamp", String.valueOf(timestamp));
		headers.put("params.cmd", "api.call");
		headers.put("params.email", "my@gmail.com");
		headers.put("params.timer.start", "1");
		headers.put("params.timer.end", "2");
		Event event = EventBuilder.withBody(message.getBytes(charset));
		event.setHeaders(headers);

		XContentBuilder expected = jsonBuilder().startObject();
		expected.field("@message", new String(message.getBytes(), charset));
		expected.field("@timestamp", new Date(timestamp));
		expected.startObject("@fields");
		expected.startObject("params");
		expected.field("timer.start", "1");
		expected.field("cmd", "api.call");
		expected.field("email", "my@gmail.com");
		expected.field("timer.end", "2");
		expected.endObject();
		expected.endObject();
		expected.endObject();

		XContentBuilder actual = fixture.getXContentBuilder(event);
		String expectedStr = new String(expected.bytes().array());
		String actualStr = new String(actual.bytes().array());
		assertEquals(expectedStr, actualStr);
	}
	@Test
	public void handleCollationAndJSONInHeader() throws Exception {
		ExtendedElasticSearchLogStashEventSerializer fixture = new ExtendedElasticSearchLogStashEventSerializer();
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("objectFields", "params, anotherField");
		parameters.put("collateObjects", "true");
		parameters.put("collateDepth", "-1");
		Context context = new Context(parameters);
		fixture.configure(context);

		String message = "{flume: somethingnotvalid}";
		String params = "{\"another\":\"field\"}";
		Map<String, String> headers = Maps.newHashMap();
		long timestamp = System.currentTimeMillis();
		headers.put("timestamp", String.valueOf(timestamp));
		headers.put("params.cmd", "api.call");
		headers.put("params.email", "my@gmail.com");
		headers.put("params.timer.start", "1");
		headers.put("params.timer.end", "2");
		headers.put("params", params);
		Event event = EventBuilder.withBody(message.getBytes(charset));
		event.setHeaders(headers);

		XContentBuilder expected = jsonBuilder().startObject();
		expected.field("@message", new String(message.getBytes(), charset));
		expected.field("@timestamp", new Date(timestamp));
		expected.startObject("@fields");
		expected.startObject("params");
		expected.field("another", "field");
		expected.field("cmd", "api.call");
		expected.field("email", "my@gmail.com");
		expected.startObject("timer");
		expected.field("start", "1");
		expected.field("end", "2");
		expected.endObject();
		expected.endObject();
		expected.endObject();
		expected.endObject();

		XContentBuilder actual = fixture.getXContentBuilder(event);
		String expectedStr = new String(expected.bytes().array());
		String actualStr = new String(actual.bytes().array());
		assertEquals(expectedStr, actualStr);
	}
}
