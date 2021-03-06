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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TestElasticSearchIndexRequestBuilderFactory {

	Node node;
	Client client;
	private ExtendedElasticSearchIndexRequestBuilderFactory factory;
	static final long FIXED_TIME_MILLIS = 123456789L;
	static final String DEFAULT_INDEX_NAME = "flume";
	static final String DEFAULT_INDEX_TYPE = "log";
	static final String DEFAULT_CLUSTER_NAME = "elasticsearch";
	String timestampedIndexName;
	FastDateFormat dateFormat;
	
	void initDefaults() {
		timestampedIndexName = DEFAULT_INDEX_NAME + '-'
				+ ElasticSearchIndexRequestBuilderFactory.df.format(FIXED_TIME_MILLIS);
		dateFormat = FastDateFormat.getInstance("yyyy.MM.dd", TimeZone.getTimeZone("Etc/UTC"));
	}

	void createNodes() throws Exception {
		Settings settings = ImmutableSettings.settingsBuilder().put("number_of_shards", 1).put("number_of_replicas", 0)
				.put("routing.hash.type", "simple").put("gateway.type", "none").put("path.data", "target/es-test")
				.build();

		node = NodeBuilder.nodeBuilder().settings(settings).local(true).node();
		client = node.client();

		client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
	}

	void shutdownNodes() throws Exception {
		((InternalNode) node).injector().getInstance(Gateway.class).reset();
		client.close();
		node.close();
	}

	@Before
	public void setupFactory() throws Exception {
		initDefaults();
		createNodes();
		factory = new ExtendedElasticSearchIndexRequestBuilderFactory();
	}

	@After
	public void tearDown() throws Exception {
		shutdownNodes();
	}

	@Test
	public void shouldSetIndexNameFromTimestampHeaderWhenPresent() throws Exception {
		String indexPrefix = "qwerty";
		String indexType = "uiop";
		Event event = new SimpleEvent();
		event.getHeaders().put("timestamp", "1213141516");

		IndexRequestBuilder indexRequestBuilder = factory.createIndexRequest(client, indexPrefix, indexType, event);

		assertEquals(indexPrefix + '-' + dateFormat.format(1213141516L),
				indexRequestBuilder.request().index());
	}

	@Test
	public void shouldGenerateObjectID() throws Exception {
		String indexPrefix = "qwerty";
		String indexType = "uiop";
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("generateId", "true");
		Context context = new Context(parameters);
		factory.configure(context);

		String message = "test body";
		Map<String, String> headers = Maps.newHashMap();
		long timestamp = System.currentTimeMillis();
		headers.put("timestamp", String.valueOf(timestamp));
		Event event = EventBuilder.withBody(message.getBytes(charset));
		event.setHeaders(headers);

		IndexRequestBuilder indexRequestBuilder = factory.createIndexRequest(client, indexPrefix, indexType, event);

		assertFalse(indexRequestBuilder.request().autoGeneratedId());
		assertTrue(indexRequestBuilder.request().id() != null && !indexRequestBuilder.request().id().isEmpty()); 
	}

	@Test
	public void shouldGenerateSameObjectIDForSameData() throws Exception {
		String indexPrefix = "qwerty";
		String indexType = "uiop";
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("generateId", "true");
		Context context = new Context(parameters);
		factory.configure(context);

		String message = "test body";
		Map<String, String> headers = Maps.newHashMap();
		long timestamp = System.currentTimeMillis();
		headers.put("timestamp", String.valueOf(timestamp));
		Event event1 = EventBuilder.withBody(message.getBytes(charset));
		event1.setHeaders(headers);
		Event event2 = EventBuilder.withBody(message.getBytes(charset));
		event2.setHeaders(headers);

		IndexRequestBuilder indexRequestBuilder1 = factory.createIndexRequest(client, indexPrefix, indexType, event1);
		IndexRequestBuilder indexRequestBuilder2 = factory.createIndexRequest(client, indexPrefix, indexType, event2);

		assertTrue(indexRequestBuilder1.request().id() != null && !indexRequestBuilder1.request().id().isEmpty()); 
		assertTrue(indexRequestBuilder2.request().id() != null && !indexRequestBuilder2.request().id().isEmpty());
		assertEquals(indexRequestBuilder1.request().id(), indexRequestBuilder2.request().id());
	}
	
	@Test
	public void shouldNotGenerateSameObjectIDForDifferentData() throws Exception {
		String indexPrefix = "qwerty";
		String indexType = "uiop";
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("generateId", "true");
		Context context = new Context(parameters);
		factory.configure(context);

		String message = "test body";
		Map<String, String> headers1 = Maps.newHashMap();
		Map<String, String> headers2 = Maps.newHashMap();
		long timestamp = System.currentTimeMillis();
		headers1.put("timestamp", String.valueOf(timestamp));
		headers2.put("timestamp", String.valueOf(timestamp));
		headers2.put("another", "one");
		Event event1 = EventBuilder.withBody(message.getBytes(charset));
		event1.setHeaders(headers1);
		Event event2 = EventBuilder.withBody(message.getBytes(charset));
		event2.setHeaders(headers2);

		IndexRequestBuilder indexRequestBuilder1 = factory.createIndexRequest(client, indexPrefix, indexType, event1);
		IndexRequestBuilder indexRequestBuilder2 = factory.createIndexRequest(client, indexPrefix, indexType, event2);

		assertTrue(indexRequestBuilder1.request().id() != null && !indexRequestBuilder1.request().id().isEmpty()); 
		assertTrue(indexRequestBuilder2.request().id() != null && !indexRequestBuilder2.request().id().isEmpty());
		assertFalse(indexRequestBuilder1.request().id().equals(indexRequestBuilder2.request().id()));
	}
	
}

