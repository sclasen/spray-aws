## spray-aws

* spray based client for aws services
* uses the marshallers and unmarshallers from the aws-java-sdk
* ~~excludes the httpclient dep from aws-java-sdk, its only needed as 'provided' to compile this project~~ 1.7 aws sdk kills this :(

### tuning

For high throughput applications you will most certainly want to increase `spray.can.host-connector.max-connections` which defaults
to only `4`. `400` is the default in the akka-persistence-dynamodb journal which uses this library.

### contributors

thanks 

@justinsb -> spray-kinesis
@petter-fidesmo -> spray-sqs
