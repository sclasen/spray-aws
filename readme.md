## spray-aws

* spray based client for aws services
* uses the marshallers and unmarshallers from the aws-java-sdk
* stubs out the apache httpclient classes so it doesnt get transitively pulled in to your project. (You are using spray after all)