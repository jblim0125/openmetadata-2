# Metadata

Storage Service Metadata Pipeline Configuration.

## Configuration

$$section
### Enable Debug Logs $(id="enableDebugLog")

Set the `Enable Debug Log` toggle to set the logging level of the process to debug. You can check these logs in the Ingestion tab of the service and dig deeper into any errors you might find.
$$

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

Container filter patterns are used to control whether to include Containers as part of metadata ingestion.

**Include**: Explicitly include Containers by adding a list of regular expressions to the `Include` field. OpenMetadata will include all Containers with names matching one or more of the supplied regular expressions. All other Containers will be excluded.

For example, to include only those Containers whose name starts with the word `demo`, add the regex pattern in the include field as `^demo.*`.

**Exclude**: Explicitly exclude Containers by adding a list of regular expressions to the `Exclude` field. OpenMetadata will exclude all Containers with names matching one or more of the supplied regular expressions. All other Containers will be included.

For example, to exclude all Containers with the name containing the word `demo`, add regex pattern in the exclude field as `.*demo.*`.

Checkout [this](https://docs.open-metadata.org/connectors/ingestion/workflows/metadata/filter-patterns/database#database-filter-pattern) document for further examples on filter patterns.
$$

$$section
### Use FQN For Filtering Views $(id="useFqnForFiltering")
Set this flag when you want to apply the filters on Fully Qualified Names (e.g `service_name.bucket_name.container_name`) instead of applying them to the raw name of the asset (e.g `container_name`).

This Flag is useful in scenarios when you have different contents with same name in multiple bucket or path and you want to filter out only one of them.

$$

$$section
### Number of Retries $(id="retries")

Times to retry the workflow in case it ends with a failure.
$$