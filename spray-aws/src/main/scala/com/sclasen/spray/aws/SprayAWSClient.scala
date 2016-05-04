package com.sclasen.spray.aws

import java.io.ByteArrayInputStream
import java.net.{ URI, URLEncoder }
import java.util.{ List => JList }

import scala.concurrent.Await

import akka.actor.{ ActorSystem, _ }
import akka.event.LoggingAdapter
import akka.io.IO
import akka.util.{ Timeout, ByteString }
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.Http
import akka.http.ClientConnectionSettings
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.amazonaws.auth._
import com.amazonaws.http.{ HttpMethodName, HttpResponseHandler, HttpResponse => AWSHttpResponse }
import com.amazonaws.transform.Marshaller
import com.amazonaws.{ AmazonServiceException, AmazonWebServiceResponse, DefaultRequest, Request }

import scala.collection.JavaConverters._
import scala.concurrent.Future

trait SprayAWSClientProps {
  def operationTimeout: Timeout

  def credentialsProvider: AWSCredentialsProvider

  def system: ActorSystem

  def materializer: ActorMaterializer

  def factory: ActorRefFactory

  def service: String

  def endpoint: String

  def doubleEncodeForSigning: Boolean = true
}

abstract class SprayAWSClient(props: SprayAWSClientProps) {

  implicit val timeout = props.operationTimeout

  implicit val excn = props.system.dispatcher

  def log: LoggingAdapter

  def errorResponseHandler: HttpResponseHandler[AmazonServiceException]

  val endpointUri = new URI(props.endpoint)
  val port = {
    if (endpointUri.getPort > 0) endpointUri.getPort
    else {
      if (props.endpoint.startsWith("https")) 443
      else 80
    }
  }
  val ssl = props.endpoint.startsWith("https")
  val clientSettings = ClientConnectionSettings(props.system)

  def pipeline(request: HttpRequest)(implicit system: ActorSystem, materializer: ActorMaterializer): Future[HttpResponse] = {
    val connectionFlow: Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]] = if (ssl) {
      Http().outgoingConnectionTls(endpointUri.getHost, port = port)
    } else {
      Http().outgoingConnection(endpointUri.getHost, port = port)
    }

    Source.single(request)
      .via(connectionFlow)
      .runWith(Sink.head)
  }

  lazy val signer: Signer = {
    val s = new AWS4Signer(props.doubleEncodeForSigning)
    s.setServiceName(props.service)
    s
  }

  val defaultContentType = "application/x-amz-json-1.0"

  /* The AWS signer encodes every part of the URL the same way.
   * Therefore we need to create the query string here using stricter URL encoding
   */
  def awsURLEncode(s: String) = Option(s).map(ss => URLEncoder.encode(ss, "UTF-8")).getOrElse("")

  def encodeQuery[T](awsReq: Request[T]) =
    awsReq.getParameters.asScala.toList.map({
      case (k, v) => s"${awsURLEncode(k)}=${awsURLEncode(v)}"
    }).mkString("&")

  def formData[T](awsReq: Request[T]) =
    HttpEntity(MediaTypes.`application/x-www-form-urlencoded`, encodeQuery(awsReq))

  def request[T](t: T)(implicit marshaller: Marshaller[Request[T], T]): HttpRequest = {
    import HttpProtocols._
    val awsReq = marshaller.marshall(t)
    awsReq.setEndpoint(endpointUri)
    awsReq.getHeaders.put("User-Agent", clientSettings.userAgentHeader.map(_.value).getOrElse("spray-aws"))
    val contentType = Option(awsReq.getHeaders.get("Content-Type"))
    signer.sign(awsReq, props.credentialsProvider.getCredentials())
    awsReq.getHeaders.remove("Host")
    awsReq.getHeaders.remove("User-Agent")
    awsReq.getHeaders.remove("Content-Length")
    awsReq.getHeaders.remove("Content-Type")
    var path: String = awsReq.getResourcePath
    if (path == "" || path == null) path = "/"
    val request = if (awsReq.getContent != null) {
      val body: Array[Byte] = Stream.continually(awsReq.getContent.read).takeWhile(-1 != _).map(_.toByte).toArray
      val mediaType = MediaType.custom(contentType.getOrElse(defaultContentType), MediaType.Encoding.Open)
      HttpRequest(awsReq.getHttpMethod, Uri(path = Uri.Path(path)), headers(awsReq), HttpEntity(mediaType, body), `HTTP/1.1`)
    } else {
      val method: HttpMethod = awsReq.getHttpMethod
      method match {
        case HttpMethods.POST => {
          headers(awsReq) match {
            case Nil => Post(path, formData(awsReq))
            case x :: xs => Post(path, formData(awsReq)) ~> addHeaders(x, xs:_*)
          }
        }
        case HttpMethods.PUT =>
          headers(awsReq) match {
            case Nil => Put(path, formData(awsReq))
            case x :: xs => Put(path, formData(awsReq)) ~> addHeaders(x, xs:_*)
          }
        case method =>
          val uri = Uri(path = Uri.Path(path), query = Uri.Query.Raw(encodeQuery(awsReq)))
          HttpRequest(method, uri, headers(awsReq))
      }
    }
    request
  }

  def response[T](response: HttpResponse)(
    implicit handler: HttpResponseHandler[AmazonWebServiceResponse[T]],
             actorSystem: ActorSystem,
             materializer: ActorMaterializer
  ): Either[AmazonServiceException, T] = {
    import scala.util.Try

    val req = new DefaultRequest[T](props.service)
    val awsResp = new AWSHttpResponse(req, null)
    val bs: Try[ByteString] = Try(Await.result(response.entity.toStrict(timeout.duration).map { _.data }, timeout.duration))

    if (bs.isFailure) {
      return Left(new com.amazonaws.AmazonServiceException(s"Unable to get response for $response"))
    }

    awsResp.setContent(new ByteArrayInputStream(bs.get.toArray))
    awsResp.setStatusCode(response.status.intValue)
    awsResp.setStatusText(response.status.defaultMessage)
    if (200 <= awsResp.getStatusCode && awsResp.getStatusCode < 300) {
      val handle: AmazonWebServiceResponse[T] = handler.handle(awsResp)
      val resp = handle.getResult
      Right(resp)
    } else {
      response.headers.foreach {
        h => awsResp.addHeader(h.name, h.value)
      }
      Left(errorResponseHandler.handle(awsResp))
    }
  }

  def headers(req: Request[_]): List[HttpHeader] = {
    req.getHeaders.asScala.map {
      case (k, v) =>
        RawHeader(k, v)
    }.toList
  }

  def fold[T](fe: Future[Either[AmazonServiceException, T]]): Future[T] = fe.map(_.fold(e => throw e, t => t))
  import HttpMethods._
  implicit def bridgeMethods(m: HttpMethodName): HttpMethod = m match {
    case HttpMethodName.POST => POST
    case HttpMethodName.GET => GET
    case HttpMethodName.PUT => PUT
    case HttpMethodName.DELETE => DELETE
    case HttpMethodName.HEAD => HEAD
    case HttpMethodName.PATCH => PATCH
  }

}
