package com.sclasen.spray.aws.s3

import java.util.{ List => JList }
import java.io.ByteArrayInputStream
import scala.concurrent.Future
import scala.collection.JavaConverters._

import akka.util.Timeout
import akka.actor.{ ActorRefFactory, ActorSystem }

import com.amazonaws.AmazonServiceException
import com.amazonaws.http.HttpResponseHandler
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.model.transform.Unmarshallers
import com.amazonaws.services.s3.internal.{ S3ErrorResponseHandler, S3ObjectResponseHandler, S3MetadataResponseHandler }

import com.sclasen.spray.aws._

case class S3ClientProps(key: String, secret: String, operationTimeout: Timeout, system: ActorSystem,
  factory: ActorRefFactory, endpoint: String = "https://s3.amazonaws.com")
    extends SprayAWSClientProps {
  val service = "s3"
  override val doubleEncodeForSigning = false
}

object MarshallersAndUnmarshallers {

  import com.amazonaws.transform.{ Marshaller, Unmarshaller }
  import com.amazonaws.{ DefaultRequest, Request }
  import com.amazonaws.services.s3.internal.{ Constants, S3XmlResponseHandler }
  import com.amazonaws.{ AmazonWebServiceRequest, AmazonWebServiceResponse }
  import com.amazonaws.http.HttpMethodName

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

      Option(listObjects.getPrefix).map { prefix =>
        request.addParameter("prefix", prefix)
      }

      Option(listObjects.getMarker).map { marker =>
        request.addParameter("marker", marker)
      }

      Option(listObjects.getDelimiter).map { delimiter =>
        request.addParameter("delimiter", delimiter)
      }

      Option(listObjects.getMaxKeys).map { maxKeys =>
        request.addParameter("max-keys", maxKeys.toString)
      }

      Option(listObjects.getEncodingType).map { encodingType =>
        request.addParameter("encoding-type", encodingType)
      }

      request
    }
  }

  implicit val listObjectsU = new S3XmlResponseHandler[ObjectListing](new Unmarshallers.ListObjectsUnmarshaller)

  implicit val objectU = new S3ObjectResponseHandler()
  implicit val voidU = new S3XmlResponseHandler[Unit](null)
}

class S3Client(val props: S3ClientProps) extends SprayAWSClient(props) {

  import MarshallersAndUnmarshallers._

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
