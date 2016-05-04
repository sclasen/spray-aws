package com.sclasen.spray.aws.s3

import java.util.{ List => JList }

import akka.actor.{ ActorRefFactory, ActorSystem }
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.{ AWSCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.services.s3.internal.{ S3ErrorResponseHandler, S3MetadataResponseHandler, S3ObjectResponseHandler }
import com.amazonaws.services.s3.Headers
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.model.transform.Unmarshallers
import com.sclasen.spray.aws._

import scala.collection.JavaConverters._
import scala.concurrent.Future

case class S3ClientProps(credentialsProvider: AWSCredentialsProvider, operationTimeout: Timeout, system: ActorSystem,
  factory: ActorRefFactory, materializer: ActorMaterializer, endpoint: String)
    extends SprayAWSClientProps {
  val service = "s3"
  override val doubleEncodeForSigning = false
}

object S3ClientProps {
  val defaultEndpoint = "https://s3.amazonaws.com"
  def apply(key: String, secret: String, operationTimeout: Timeout, system: ActorSystem, factory: ActorRefFactory, materializer: ActorMaterializer, endpoint: String = defaultEndpoint) =
    new S3ClientProps(new StaticCredentialsProvider(new BasicAWSCredentials(key, secret)), operationTimeout, system, factory, materializer, endpoint)
}

object MarshallersAndUnmarshallers {

  import com.amazonaws.http.HttpMethodName
  import com.amazonaws.services.s3.internal.{ Constants, S3XmlResponseHandler }
  import com.amazonaws.transform.Marshaller
  import com.amazonaws.{ AmazonWebServiceRequest, DefaultRequest, Request }

  class S3Marshaller[X <: AmazonWebServiceRequest](httpMethod: HttpMethodName)
      extends Marshaller[Request[X], X] {
    def marshall(originalRequest: X): Request[X] = {
      val request = new DefaultRequest[X](originalRequest, Constants.S3_SERVICE_NAME)
      request.setHttpMethod(httpMethod)
      request.addHeader("x-amz-content-sha256", "required")

      originalRequest.getClass.getMethods.find(_.getName == "getBucketName").map { method =>
        val bucketName: String = method.invoke(originalRequest).asInstanceOf[String]
        request.setResourcePath(s"/${bucketName}")

        originalRequest.getClass.getMethods.find(_.getName == "getKey").map { method =>
          val key: String = method.invoke(originalRequest).asInstanceOf[String]
          request.setResourcePath(s"/${bucketName}/${key}")
        }
      }

      request
    }
  }

  implicit val listBucketsM = new S3Marshaller[ListBucketsRequest](HttpMethodName.GET)
  implicit val listBucketsU = new S3XmlResponseHandler[JList[Bucket]](new Unmarshallers.ListBucketsUnmarshaller)

  implicit val createBucketM = new S3Marshaller[CreateBucketRequest](HttpMethodName.PUT)
  implicit val deleteBucketM = new S3Marshaller[DeleteBucketRequest](HttpMethodName.DELETE)

  implicit val putObjectM = new S3Marshaller[PutObjectRequest](HttpMethodName.PUT) {
    override def marshall(putObject: PutObjectRequest): Request[PutObjectRequest] = {
      val request = super.marshall(putObject)

      if (Option(putObject.getFile).isDefined) {
        throw new Exception("File upload not supported")
      } else {
        import scala.collection.JavaConversions._
        request.getHeaders.putAll(putObject.getMetadata.getRawMetadata.map { case (k, v) => k -> v.toString })
        request.getHeaders.putAll(putObject.getMetadata.getUserMetadata.map { case (k, v) => s"${Headers.S3_USER_METADATA_PREFIX}-$k" -> v })
        Option(putObject.getRedirectLocation).foreach(request.getHeaders.put(Headers.REDIRECT_LOCATION, _))
        Option(putObject.getStorageClass).foreach(request.getHeaders.put(Headers.STORAGE_CLASS, _))
        Option(putObject.getCannedAcl).foreach(acl => request.getHeaders.put(Headers.S3_CANNED_ACL, acl.toString))
        // TODO, handle putObject.getAccessControlList, cf: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html
        request.setContent(putObject.getInputStream)
        request
      }
    }
  }
  implicit val putObjectU = new S3MetadataResponseHandler()
  implicit val getObjectM = new S3Marshaller[GetObjectRequest](HttpMethodName.GET)
  implicit val deleteObjectM = new S3Marshaller[DeleteObjectRequest](HttpMethodName.DELETE)

  implicit val listObjectsM = new S3Marshaller[ListObjectsRequest](HttpMethodName.GET) {
    override def marshall(listObjects: ListObjectsRequest): Request[ListObjectsRequest] = {
      val request = super.marshall(listObjects)
      Option(listObjects.getPrefix).foreach(request.addParameter("prefix", _))
      Option(listObjects.getMarker).foreach(request.addParameter("marker", _))
      Option(listObjects.getDelimiter).foreach(request.addParameter("delimiter", _))
      Option(listObjects.getMaxKeys).foreach(maxKeys => request.addParameter("max-keys", maxKeys.toString))
      Option(listObjects.getEncodingType).foreach(request.addParameter("encoding-type", _))
      request
    }
  }

  implicit val listObjectsU = new S3XmlResponseHandler[ObjectListing](new Unmarshallers.ListObjectsUnmarshaller)

  implicit val objectU = new S3ObjectResponseHandler()
  implicit val voidU = new S3XmlResponseHandler[Unit](null)
}

class S3Client(val props: S3ClientProps) extends SprayAWSClient(props) {

  import com.sclasen.spray.aws.s3.MarshallersAndUnmarshallers._


  implicit val system = props.system

  implicit val materializer = props.materializer

  val log = props.system.log
  def errorResponseHandler = new S3ErrorResponseHandler

  def listBuckets(aws: ListBucketsRequest): Future[Either[AmazonServiceException, Seq[Bucket]]] = {
    pipeline(request(aws)).map(response[JList[Bucket]]).map(_.right.map(_.asScala.toSeq))
  }

  def createBucket(aws: CreateBucketRequest): Future[Either[AmazonServiceException, Unit]] = {
    pipeline(request(aws)).map(response[Unit])
  }

  def deleteBucket(aws: DeleteBucketRequest): Future[Either[AmazonServiceException, Unit]] = {
    pipeline(request(aws)).map(response[Unit])
  }

  def putObject(aws: PutObjectRequest): Future[Either[AmazonServiceException, ObjectMetadata]] = {
    pipeline(request(aws)).map(response[ObjectMetadata])
  }

  def getObject(aws: GetObjectRequest): Future[Either[AmazonServiceException, S3Object]] = {
    pipeline(request(aws)).map(response[S3Object])
  }

  def deleteObject(aws: DeleteObjectRequest): Future[Either[AmazonServiceException, Unit]] = {
    pipeline(request(aws)).map(response[Unit])
  }

  def listObjects(aws: ListObjectsRequest): Future[Either[AmazonServiceException, ObjectListing]] = {
    pipeline(request(aws)).map(response[ObjectListing])
  }
}
