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

/**
 * An extended serializer for flume events into the same format LogStash uses</p>
 * This adds some more features on top of the default ES serializer that is part of 
 * the Flume distribution.</p>
 * 
 * For more details on added features:
 * @see https://github.com/gigya/flume-ng-elasticsearch-ser-ex
 * 
 * @note This builder will not work when using the REST client of the ES sink 
 * of Flume 1.5.X. The REST client does not use a builder.
 * 
 * 
 * @author Rotem Hermon
 *
 */
public class ExtendedElasticSearchIndexRequestBuilderFactory extends AbstractElasticSearchIndexRequestBuilderFactory {
	/**
	 * Configuration property, set to true to generate an _id for the indexed event, 
	 * not letting ES to auto generate an _id. The _id is an MD5 of the serialized event. 
	 */
	public static final String GENERATE_ID = "generateId";
	private boolean generateId = false;

	private ElasticSearchEventSerializer serializer = new ExtendedElasticSearchLogStashEventSerializer();

	public ExtendedElasticSearchIndexRequestBuilderFactory() {
		super(FastDateFormat.getInstance("yyyy.MM.dd", TimeZone.getTimeZone("Etc/UTC")));
	}

	public ExtendedElasticSearchIndexRequestBuilderFactory(FastDateFormat fd) {
		super(fd);
	}

	public ExtendedElasticSearchIndexRequestBuilderFactory(ElasticSearchEventSerializer serializer) {
		super(FastDateFormat.getInstance("yyyy.MM.dd", TimeZone.getTimeZone("Etc/UTC")));

		this.serializer = serializer;
	}

	public ExtendedElasticSearchIndexRequestBuilderFactory(ElasticSearchEventSerializer serializer, FastDateFormat fd) {
		super(fd);
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
	protected void prepareIndexRequest(IndexRequestBuilder indexRequest, String indexName, String indexType, Event event) throws IOException {
		BytesStream contentBuilder = serializer.getContentBuilder(event);
		BytesReference contentBytes = contentBuilder.bytes();
		indexRequest.setIndex(indexName).setType(indexType).setSource(contentBytes);
		if (generateId) {
			// if we need to generate an _id for the event, get an MD5 hash for
			// the serialized
			// event bytes.
			String hashId = null;
			try {
				byte[] bytes = contentBytes.toBytes();
				if (contentBytes.length() > 0 && null != bytes) {
					MessageDigest md = MessageDigest.getInstance("MD5");
					byte[] thedigest = md.digest(bytes);
					hashId = Base64.encodeBytes(thedigest, Base64.URL_SAFE);
				}
			} catch (NoSuchAlgorithmException e) {
				Integer hash = contentBytes.hashCode();
				hashId = hash.toString();
			}
			if (null != hashId && !hashId.isEmpty())
				indexRequest.setId(hashId.toString());
		}
	}

}
