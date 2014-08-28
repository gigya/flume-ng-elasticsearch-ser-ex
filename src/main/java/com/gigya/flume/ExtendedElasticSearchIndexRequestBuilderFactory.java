package com.gigya.flume;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.elasticsearch.AbstractElasticSearchIndexRequestBuilderFactory;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.BytesStream;

public class ExtendedElasticSearchIndexRequestBuilderFactory extends AbstractElasticSearchIndexRequestBuilderFactory {
	public static final String GENERATE_ID = "generateId";
	private boolean generateId = false;

	private ElasticSearchEventSerializer serializer = new ExtendedElasticSearchLogStashEventSerializer();

	public ExtendedElasticSearchIndexRequestBuilderFactory() {
		super(FastDateFormat.getInstance("yyyy.MM.dd", TimeZone.getTimeZone("Etc/UTC")));
	}

	public ExtendedElasticSearchIndexRequestBuilderFactory(ElasticSearchEventSerializer serializer) {
		super(FastDateFormat.getInstance("yyyy.MM.dd", TimeZone.getTimeZone("Etc/UTC")));

		this.serializer = serializer;
	}

	@Override
	public void configure(Context context) {
		serializer.configure(context);
		if (StringUtils.isNotBlank(context.getString(GENERATE_ID))) {
			String remove = context.getString(GENERATE_ID);
			if ("true".equalsIgnoreCase(remove) || "1".equalsIgnoreCase(remove)) {
				generateId = true;
			}
		}
	}

	@Override
	public void configure(ComponentConfiguration config) {
		serializer.configure(config);
	}

	@Override
	protected void prepareIndexRequest(IndexRequestBuilder indexRequest, String indexName, String indexType, Event event)
			throws IOException {
		BytesStream contentBuilder = serializer.getContentBuilder(event);
		BytesReference contentBytes = contentBuilder.bytes();
		indexRequest.setIndex(indexName)
		.setType(indexType)
		.setSource(contentBytes);
		if (generateId){
			String hashId;
			try {
				MessageDigest md = MessageDigest.getInstance("MD5");
				byte[] thedigest = md.digest(contentBytes.array());
				hashId = Base64.encodeBytes(thedigest, Base64.URL_SAFE);
				
			} catch (NoSuchAlgorithmException e) {
				Integer hash = contentBytes.hashCode();
				hashId = hash.toString();
			}
			indexRequest.setId(hashId.toString());
		}
	}

}
