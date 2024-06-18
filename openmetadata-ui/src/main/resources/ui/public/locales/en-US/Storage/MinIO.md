# MinIO

In this section, we provide guides and references to use the MinIO connector.

By default, the MinIO connector will ingest struct data (Buckets). 

## Connection Details

$$section
### Access Key ID $(id="AccessKeyId")

Access keys consist of two parts:
1. An access key ID (for example, `AKIAIOSFODNN7EXAMPLE`),
2. And a secret access key (for example, `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`).

You must use both the access key ID and secret access key together to authenticate your requests.
$$

$$section
### Secret Access Key $(id="SecretAccessKey")

Secret access key (for example, `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`).
$$

$$section
### Region $(id="Region")

Each Storage Region is a separate geographic area in which clusters data centers. 

We need to know the region the service you want reach belongs to.
$$

$$section
### Session Token $(id="SessionToken")

If you are using temporary credentials to access your services, you will need to inform the Access Key ID and Secrets Access Key. 
Also, these will include an Session Token.
$$

$$section
### Endpoint URL $(id="endPointURL")

To connect programmatically to service, you use an endpoint. An *endpoint* is the URL of the entry point for web service. 
$$

$$section
### Bucket Name $(id="bucketName")

Provide the names of buckets that you would want to ingest, if you want to ingest metadata from all buckets or apply a 
filter to ingest buckets then leave this field empty.
$$
