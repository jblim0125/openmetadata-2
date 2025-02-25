# Profiler

This workflow allows you to profile your container assets and gain insights into their structure (e.g. of metrics computed: `max`, `min`, `mean`, etc)

## Configuration

$$section
### Bucket Filter Pattern $(id="bucketFilterPattern")

Object Storage Bucket filter patterns to control whether to include bucket as part of metadata ingestion.

**Include**: Explicitly include bucket by adding a list of regular expressions to the `Include` field. We will include all bucket with names matching one or more of the supplied regular expressions. All other bucket will be excluded.

For example, to include only those bucket whose name starts with the word `demo`, add the regex pattern in the include field as `^demo.*`.

**Exclude**: Explicitly exclude bucket by adding a list of regular expressions to the `Exclude` field. We will exclude all bucket with names matching one or more of the supplied regular expressions. All other bucket will be included.

For example, to exclude all bucket with the name containing the word `demo`, add the regex pattern in the exclude field as `.*demo.*`.

$$

$$section
### Container Filter Pattern $(id="containerFilterPattern")

Container(fileName) filter patterns to control whether to include container as part of metadata ingestion.

**Include**: Explicitly include container by adding a list of regular expressions to the `Include` field. We will include all container with names matching one or more of the supplied regular expressions. All other container will be excluded.

For example, to include only those container whose name starts with the word `demo`, add the regex pattern in the include field as `^demo.*`.

**Exclude**: Explicitly exclude container by adding a list of regular expressions to the `Exclude` field. We will exclude all container with names matching one or more of the supplied regular expressions. All other container will be included.

For example, to exclude all container with the name containing the word `demo`, add the regex pattern in the exclude field as `.*demo.*`.

$$

$$section
### Enable Debug Logs $(id="enableDebugLog")

Set the `Enable Debug Log` toggle to set the logging level of the process to debug. You can check these logs in the Ingestion tab of the service and dig deeper into any errors you might find.
$$

$$section
### Use FQN For Filtering Views $(id="useFqnForFiltering")
Set this flag when you want to apply the filters on Fully Qualified Names (e.g `service_name.container_name`) instead of applying them to the raw name of the asset (e.g `container_name`). 

This Flag is useful in scenarios when you have different contents with same name in multiple bucket or path and you want to filter out only one of them. 

$$

$$section
### Ingest Sample Data $(id="generateSampleData")

Set the Ingest Sample Data toggle to control whether to ingest sample data as part of profiler ingestion. If this is enabled, 100 rows will be ingested by default. You can update the number of rows in the "DatabaseServiceProfilerPipeline Advanced Config" section (i.e. `Sample Data Rows Count` setting). 
$$

$$section
### Compute Metrics $(id="computeMetrics")

Set the `Compute Metrics` toggle off to not perform any metric computation during the profiler ingestion workflow. Used in combination with `Ingest Sample Data` toggle on allows you to only ingest sample data.
$$

$$section
### Auto Tag PII $(id="processPiiSensitive")

Set the `Auto Tag PII` toggle to control whether to automatically tag columns that might contain sensitive information as part of profiler ingestion. 

If `Ingest Sample Data` is enabled, OpenMetadata will leverage machine learning to infer which column may contain PII sensitive data. If disabled, OpenMetadata will infer this information from the column name. Use the `Confidence` setting in the "DatabaseServiceProfilerPipeline Advanced Config" to set the confience level when infering the PII status of a column.
$$


$$section
### Profile Sample Type $(id="profileSampleType")
The sample type can be set to either:  

* **Percentage**: this will use a percentage to sample the table (e.g. if table has 100 rows, and we set sample percentage tp 50%, the profiler will use 50 random rows to compute the metrics).
* **Row Count**: this will use a number of rows to sample the table (e.g. if table has 100 rows, and we set row count to 10, the profiler will use 10 random rows to compute the metrics).
$$

$$section
### Profile Sample $(id="profileSample")
Percentage of data or number of rows to use when sampling tables to compute the profiler metrics. By default (i.e. if left blank), the profiler will run against the entire table.
$$

$$section
### PII Inference Confidence Level $(id="confidence")
Confidence level to use when infering whether a column shoul be flagged as PII or not (between 0 and 100). A number closer to 100 will yield less false positive but potentially more false negative. 
$$

$$section
### Sample Data Rows Count $(id="sampleDataCount")
Set the number of rows to ingest when `Ingest Sample Data` toggle is on. Defaults to 50.
$$

$$section
### Thread Count $(id="threadCount")
Number of threads that will be used when computing the profiler metrics. A high number can have negative performance effect.

We recommend to use the default value unless you have a good understanding of multithreading and your database is capable of handling multiple concurrent connections.
$$

$$section
### Timeout (Seconds) $(id="timeoutSeconds")

This will set the duration a profiling job against a table should wait before interrupting its execution and moving on to profiling the next table.

It is important to note that the profiler will wait for the hanging query to **terminate** before killing the execution. If there is a risk for your profiling job to hang, it is important to also set a query/connection timeout on your database engine. The default value for the profiler timeout is 12 hours.
$$

$$section
### Number of Retries $(id="retries")

Times to retry the workflow in case it ends with a failure.
$$