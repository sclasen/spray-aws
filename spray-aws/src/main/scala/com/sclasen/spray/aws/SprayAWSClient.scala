package com.sclasen.spray.aws

import akka.actor._
import akka.io.IO
import collection.JavaConverters._
import com.amazonaws.auth.{ AWS4Signer, BasicAWSCredentials }
import com.amazonaws.transform.{ JsonErrorUnmarshaller, JsonUnmarshallerContext, Unmarshaller, Marshaller }
import com.amazonaws.util.StringInputStream
import com.amazonaws.util.json.JSONObject
import com.amazonaws.http.{ HttpResponse => AWSHttpResponse, JsonErrorResponseHandler, JsonResponseHandler, HttpMethodName }
import com.amazonaws.{ AmazonServiceException, Request, AmazonWebServiceResponse, DefaultRequest }
import java.net.URI
import java.util.{ List => JList }
import org.codehaus.jackson.JsonFactory
import spray.http.HttpProtocols._
import akka.event.LoggingAdapter
import spray.can.{ HostConnectorInfo, HostConnectorSetup, Http }
import spray.http.HttpHeaders.RawHeader
import spray.http.HttpResponse
import akka.actor.ActorSystem
import akka.util.Timeout
import spray.http._
import spray.http.HttpMethods._
import spray.client.pipelining._
import akka.pattern._
import scala.concurrent.Await
import spray.can.client.ClientConnectionSettings

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

  def exceptionUnmarshallers: JList[JsonErrorUnmarshaller]

  /** **********************************************************************************************/
  /* I PROMISE YOU THAT THE PROTOCOL NEEDS TO BE http NOT https HERE and THE PORT NEEDS TO BE 443 */
  val endpointUri = new URI(s"https://${props.endpoint}")
  /* ^  It tricks the AwsSigner and spray into agreeing on host headers  ^*/
  /** **********************************************************************/
  val jsonFactory = new JsonFactory()
  val clientSettings = ClientConnectionSettings(props.system)

  val connection = {
    implicit val s = props.system
    Await.result((IO(Http) ? HostConnectorSetup(props.endpoint, port = 443, sslEncryption = true)).map {
      case HostConnectorInfo(hostConnector, _) => hostConnector
    }, timeout.duration)
  }

  val pipeline = sendReceive(connection)

  val credentials = new BasicAWSCredentials(props.key, props.secret)
  val signer = new AWS4Signer()
  signer.setServiceName(props.service)

  val `application/x-amz-json-1.0` = MediaType.custom("application/x-amz-json-1.0")

  def request[T](t: T)(implicit marshaller: Marshaller[Request[T], T]): HttpRequest = {
    val awsReq = marshaller.marshall(t)
    awsReq.setEndpoint(endpointUri)
    awsReq.getHeaders.put("User-Agent", clientSettings.userAgentHeader)
    val body = awsReq.getContent.asInstanceOf[StringInputStream].getString
    signer.sign(awsReq, credentials)
    awsReq.getHeaders.remove("Host")
    awsReq.getHeaders.remove("User-Agent")
    awsReq.getHeaders.remove("Content-Length")
    awsReq.getHeaders.remove("Content-Type")
    var path: String = awsReq.getResourcePath
    if (path == "") path = "/"
    val request = HttpRequest(awsReq.getHttpMethod, path, headers(awsReq), HttpEntity(`application/x-amz-json-1.0`, body), `HTTP/1.1`)
    request
  }

  def response[T](response: HttpResponse)(implicit unmarshaller: Unmarshaller[T, JsonUnmarshallerContext]): T = {
    val req = new DefaultRequest[T](props.service)
    val awsResp = new AWSHttpResponse(req, null)
    awsResp.setContent(new StringInputStream(response.entity.asString))
    awsResp.setStatusCode(response.status.intValue)
    awsResp.setStatusText(response.status.defaultMessage)
    if (awsResp.getStatusCode == 200) {
      val handler = new JsonResponseHandler[T](unmarshaller)
      val handle: AmazonWebServiceResponse[T] = handler.handle(awsResp)
      val resp = handle.getResult
      resp
    } else {
      response.headers.foreach {
        h => awsResp.addHeader(h.name, h.value)
      }
      val errorResponseHandler = new JsonErrorResponseHandler(exceptionUnmarshallers.asInstanceOf[JList[Unmarshaller[AmazonServiceException, JSONObject]]])
      throw errorResponseHandler.handle(awsResp)
    }
  }

  def headers(req: Request[_]): List[HttpHeader] = {
    req.getHeaders.asScala.map {
      case (k, v) =>
        RawHeader(k, v)
    }.toList
  }

  implicit def bridgeMethods(m: HttpMethodName): HttpMethod = m match {
    case HttpMethodName.POST => POST
    case HttpMethodName.GET => GET
    case HttpMethodName.PUT => PUT
    case HttpMethodName.DELETE => DELETE
    case HttpMethodName.HEAD => HEAD
  }

}

