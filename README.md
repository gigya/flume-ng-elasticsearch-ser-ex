flume-ng-elasticsearch-ser-ex
=============================

This is an extended logstash format serializer for the [Elasticsearch sink for Flume](http://flume.apache.org/FlumeUserGuide.html#elasticsearchsink).

We're using Flume at [Gigya](http://gigya.com) for shipping logs and application events to Elasticsearch, later querying them using [kibana](http://www.elasticsearch.org/overview/kibana/).   
This analyzer fixes some issues we've encountered with the original serializer, and adds additional functionality that we found useful.

### Using ###
To use this serializer follow the instructions for configuring the [Elasticsearch sink for Flume](http://flume.apache.org/FlumeUserGuide.html#elasticsearchsink).   
Then configure the sink to use the extended serializer:
```
a1.sinks.k1.type = org.apache.flume.sink.elasticsearch.ElasticSearchSink
a1.sinks.k1.serializer = com.gigya.flume.ExtendedElasticSearchIndexRequestBuilderFactory
```

### Features ###

This serializer fixes two issues that exist in the original serializer:

1. Headers fields appearing twice in the serialized JSON objects.
2. A problem with serializing headers that contains a JSON string.

It also adds the following options:

##### Using the message header as @message #####
The original serializer always uses the event body as the logstash *@message* field.   
The extended serializer will look first for a *"message"* header in the Flume event. If that doesn't exist it will use the event body. 

##### Removing the @fields prefix for custom fields #####
The original logstash format places custom fields under a *@fields* node. This is not really needed anymore (and is just annoying when using kibana to view events).   
You can remove the @fields level and place all custom fields under the root node using this setting (default is false):
```
a1.sinks.k1.serializer.removeFieldsPrefix = true
```

##### Handling JSON strings in header fields #####
By default the extended serializer does not detect JSON strings in headers, but serializes the value as a string.
 
You can define specific headers that might contain JSON strings. The serializer will try to detect if the value in those headers is indeed a JSON. If so it will serialize it as an object and not as a string. 

So when the Flume event contains a header with a JSON string, i.e.:
```
f1 = "{ 'a' : 1 }"
```
It will be serialized as an object:
```
{   
   "f1" : {
     "a" : 1   
   }
}
```
To define which headers might contain an object use a comma separated list of header names with this setting:
```
a1.sinks.k1.serializer.objectFields = f1,f2
```
    
##### Collating objects #####
Instead of using a single header with a JSON string as a value, you can specify fields of an object in separate header fields, using a dot notation of the full object field path.

So for example, adding these events headers: 
```
params.f1.a = 1
params.f2.b = 2
```
Will be turned into:
```
{   
   "params" : {   
      "f1" : {
         "a" : 1   
      },
      "f2" : {
         "b" : 1   
      }
   }
}
```
The serializer can be configured to collate objects from headers using this setting (default is false):
```
a1.sinks.k1.serializer.collateObjects = true
```
##### Generating document IDs for events #####
The default Elasticsearch sink writes the events to Elasticsearch without specifying an ID for each document, letting Elasticsearch generate a new random ID for every event written. 

This can cause a few problems. First, if the same event is indexed to Elasticsearch more than once, it will get a different ID (and will be considered a "new" event) every time it is indexed. This means that the data collected in Elasticsearch might be inaccurate (by containing duplicate event documents).

Futher more, since events are indexed by the sink in batches, when one of the batch documents causes an indexing failure (because of a mapping conflict, for example), the whole transaction will fail, and Flume will retry it again. So the same event might be indexed over and over, causing corruption and explosion of data in Elasticsearch.

To overcome these issues the extended serializer allows generating document IDs for events when indexing. If configured, the serializer will set the ID of each document in the batch by generating an MD5 hash of the serialized event. So the same event will always get a consistant document ID.

You can configure the serializer to generate document IDs using this setting:
```
a1.sinks.k1.serializer.generateId = true
```

**Important Note:**
The generation of document IDs happens in the request builder factory class (ExtendedElasticSearchIndexRequestBuilderFactory). This will **not** work if using the new Elasticsearch HTTP API option, available in Flume 1.5.0 ([see here](https://issues.apache.org/jira/browse/FLUME-2225)). This is because the added support for the HTTP API does not use the request builder factory and does not provide an option to manipulate the metadata for the indexed document.
  

 