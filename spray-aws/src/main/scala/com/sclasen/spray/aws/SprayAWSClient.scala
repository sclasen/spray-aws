package com.sclasen.spray.dynamodb

import akka.actor.{ActorRefFactory, ActorSystem, ActorContext, Props}
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
import spray.can.client.DefaultHttpClient
import spray.client.HttpConduit
import spray.http.HttpMethods._
import spray.http.HttpProtocols._
import spray.http.MediaTypes.CustomMediaType
import spray.http.HttpMethod
import spray.http.HttpRequest
import spray.http.HttpHeaders.RawHeader
import spray.http.HttpHeader
import spray.http.HttpBody
import spray.http.HttpResponse
import akka.util.Timeout
import akka.event.LoggingAdapter

trait SprayAWSClientProps {
  def operationTimeout: Timeout

  def key: String

  def secret: String

  def system:ActorSystem

  def factory:ActorRefFactory

  def service:String

  def endpoint:String
}

abstract class SprayAWSClient(props:SprayAWSClientProps) {

  implicit val timeout = props.operationTimeout

  implicit val excn = props.system.dispatcher

  def log: LoggingAdapter

  def exceptionUnmarshallers: JList[JsonErrorUnmarshaller]

  /************************************************************************************************/
  /* I PROMISE YOU THAT THE PROTOCOL NEEDS TO BE http NOT https HERE and THE PORT NEEDS TO BE 443 */
  val endpointUri = new URI(s"http://${props.endpoint}:443")
  /* ^  It tricks the AwsSigner and spray into agreeing on host headers  ^*/
  /************************************************************************/
  val jsonFactory = new JsonFactory()

  val connection = DefaultHttpClient(props.system)
  val conduit = props.factory.actorOf(
    props = Props(new HttpConduit(connection, props.endpoint, port = 443, sslEnabled = true))
  )

  val pipeline = HttpConduit.sendReceive(conduit)

  val credentials = new BasicAWSCredentials(props.key, props.secret)
  val signer = new AWS4Signer()
  signer.setServiceName(props.service)

  val `application/x-amz-json-1.0` = CustomMediaType("application/x-amz-json-1.0")

  def request[T](t: T)(implicit marshaller: Marshaller[Request[T], T]): HttpRequest = {
    val awsReq = marshaller.marshall(t)
    awsReq.setEndpoint(endpointUri)
    awsReq.getHeaders.put("User-Agent","spray-can/1.1-M7")
    val body = awsReq.getContent.asInstanceOf[StringInputStream].getString
    signer.sign(awsReq, credentials)
    var path: String = awsReq.getResourcePath
    if (path == "") path = "/"
    val request = HttpRequest(awsReq.getHttpMethod, path, headers(awsReq), HttpBody(`application/x-amz-json-1.0`, body), `HTTP/1.1`)
    request
  }

  def response[T](response: HttpResponse)(implicit unmarshaller: Unmarshaller[T, JsonUnmarshallerContext]): T = {
    val req = new DefaultRequest[T](props.service)
    val awsResp = new AWSHttpResponse(req, null)
    awsResp.setContent(new StringInputStream(response.entity.asString))
    awsResp.setStatusCode(response.status.value)
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

