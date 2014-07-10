package com.sclasen.spray.aws

import akka.actor._
import akka.io.IO
import collection.JavaConverters._
import com.amazonaws.auth.{ AbstractAWSSigner, Signer, AWS4Signer, BasicAWSCredentials }
import com.amazonaws.transform.{ JsonErrorUnmarshaller, JsonUnmarshallerContext, Unmarshaller, Marshaller }
import com.amazonaws.util.StringInputStream
import com.amazonaws.util.json.JSONObject
import com.amazonaws.http.{ HttpResponse => AWSHttpResponse, JsonErrorResponseHandler, JsonResponseHandler, HttpMethodName, HttpResponseHandler }
import com.amazonaws.{ AmazonServiceException, Request, AmazonWebServiceResponse, DefaultRequest }
import java.net.{ URLEncoder, URI }
import java.util.{ List => JList }
import spray.http.HttpProtocols._
import akka.event.LoggingAdapter
import spray.can.Http
import spray.can.Http._
import spray.http.HttpHeaders.RawHeader
import akka.actor.ActorSystem
import akka.util.{ ByteString, Timeout }
import spray.http._
import spray.http.HttpMethods._
import spray.client.pipelining._
import akka.pattern._
import scala.concurrent.{ Future, Await }
import spray.can.client.ClientConnectionSettings
import scala.util.control.NoStackTrace
import spray.client.pipelining
import spray.http.Uri.Query

trait SprayAWSClientProps {
  def operationTimeout: Timeout

  def key: String

  def secret: String

  def system: ActorSystem

  def factory: ActorRefFactory

  def service: String

  def endpoint: String
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

  def connection = {
    implicit val s = props.system
    (IO(Http) ? HostConnectorSetup(endpointUri.getHost, port = port, sslEncryption = ssl)).map {
      case HostConnectorInfo(hostConnector, _) => hostConnector
    }
  }

  def pipeline(req: HttpRequest) = connection.flatMap(sendReceive(_).apply(req))

  val credentials = new BasicAWSCredentials(props.key, props.secret)

  lazy val signer: Signer = {
    val s = new AWS4Signer()
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
    val awsReq = marshaller.marshall(t)
    awsReq.setEndpoint(endpointUri)
    awsReq.getHeaders.put("User-Agent", clientSettings.userAgentHeader.map(_.value).getOrElse("spray-aws"))
    val contentType = Option(awsReq.getHeaders.get("Content-Type"))
    signer.sign(awsReq, credentials)
    awsReq.getHeaders.remove("Host")
    awsReq.getHeaders.remove("User-Agent")
    awsReq.getHeaders.remove("Content-Length")
    awsReq.getHeaders.remove("Content-Type")
    var path: String = awsReq.getResourcePath
    if (path == "" || path == null) path = "/"
    val request = if (awsReq.getContent != null) {
      val body = awsReq.getContent.asInstanceOf[StringInputStream].getString
      val mediaType = MediaType.custom(contentType.getOrElse(defaultContentType))
      HttpRequest(awsReq.getHttpMethod, path, headers(awsReq), HttpEntity(mediaType, body), `HTTP/1.1`)
    } else {
      val method: HttpMethod = awsReq.getHttpMethod
      method match {
        case HttpMethods.POST =>
          Post(path, formData(awsReq)) ~> addHeaders(headers(awsReq))
        case HttpMethods.PUT =>
          Put(path, formData(awsReq)) ~> addHeaders(headers(awsReq))
        case method =>
          val uri = Uri(path = Uri.Path(path), query = Uri.Query.Raw(encodeQuery(awsReq)))
          HttpRequest(method, uri, headers(awsReq))
      }
    }
    request
  }

  def response[T](response: HttpResponse)(implicit handler: HttpResponseHandler[AmazonWebServiceResponse[T]]): Either[AmazonServiceException, T] = {
    val req = new DefaultRequest[T](props.service)
    val awsResp = new AWSHttpResponse(req, null)
    awsResp.setContent(new StringInputStream(response.entity.asString))
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

  implicit def bridgeMethods(m: HttpMethodName): HttpMethod = m match {
    case HttpMethodName.POST => POST
    case HttpMethodName.GET => GET
    case HttpMethodName.PUT => PUT
    case HttpMethodName.DELETE => DELETE
    case HttpMethodName.HEAD => HEAD
  }

}
