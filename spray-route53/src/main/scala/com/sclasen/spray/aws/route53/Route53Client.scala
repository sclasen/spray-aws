package com.sclasen.spray.aws.route53

import akka.util.Timeout
import akka.actor.{ ActorRefFactory, ActorSystem }
import akka.stream.ActorMaterializer
import com.amazonaws.internal.StaticCredentialsProvider
import com.sclasen.spray.aws.{ SprayAWSClient, SprayAWSClientProps }
import com.amazonaws.transform.{ Marshaller, StandardErrorUnmarshaller, Unmarshaller }
import com.amazonaws.{ Request, AmazonWebServiceResponse, AmazonServiceException }
import org.w3c.dom.Node
import collection.JavaConverters._
import com.amazonaws.services.route53.model.transform._
import com.amazonaws.services.route53.model._
import com.amazonaws.http.{ StaxResponseHandler, DefaultErrorResponseHandler, HttpResponseHandler }
import com.amazonaws.services.route53.model.{ ChangeResourceRecordSetsResult, ChangeResourceRecordSetsRequest }
import scala.concurrent.Future
import com.amazonaws.services.route53.internal.Route53IdRequestHandler
import com.amazonaws.util.TimingInfo
import com.amazonaws.auth.{ AWSCredentialsProvider, BasicAWSCredentials, AWS3Signer, Signer }

case class Route53ClientProps(credentialsProvider: AWSCredentialsProvider, operationTimeout: Timeout, system: ActorSystem, factory: ActorRefFactory, materializer: ActorMaterializer, endpoint: String) extends SprayAWSClientProps {
  val service = "route53"
}

object Route53ClientProps {
  val defaultEndpoint = "https://route53.amazonaws.com"
  def apply(key: String, secret: String, operationTimeout: Timeout, system: ActorSystem, factory: ActorRefFactory, materializer: ActorMaterializer, endpoint: String = defaultEndpoint) =
    new Route53ClientProps(new StaticCredentialsProvider(new BasicAWSCredentials(key, secret)), operationTimeout, system, factory, materializer, endpoint)
}

object MarshallersAndUnmarshallers {
  val route53ExceptionUnmarshallers = List[Unmarshaller[AmazonServiceException, Node]](
    new DelegationSetNotAvailableExceptionUnmarshaller(),
    new HealthCheckAlreadyExistsExceptionUnmarshaller(),
    new HealthCheckInUseExceptionUnmarshaller(),
    new HostedZoneAlreadyExistsExceptionUnmarshaller(),
    new HostedZoneNotEmptyExceptionUnmarshaller(),
    new IncompatibleVersionExceptionUnmarshaller(),
    new InvalidChangeBatchExceptionUnmarshaller(),
    new InvalidDomainNameExceptionUnmarshaller(),
    new InvalidInputExceptionUnmarshaller(),
    new NoSuchChangeExceptionUnmarshaller(),
    new NoSuchHealthCheckExceptionUnmarshaller(),
    new NoSuchHostedZoneExceptionUnmarshaller(),
    new PriorRequestNotCompleteExceptionUnmarshaller(),
    new TooManyHealthChecksExceptionUnmarshaller(),
    new TooManyHostedZonesExceptionUnmarshaller(),
    new StandardErrorUnmarshaller()
  ).toBuffer.asJava

  implicit val CRRSReq = new ChangeResourceRecordSetsRequestMarshaller()
  implicit val CRRSRes = new StaxResponseHandler(ChangeResourceRecordSetsResultStaxUnmarshaller.getInstance())

  implicit val CHCReq = new CreateHealthCheckRequestMarshaller()
  implicit val CHCRes = new StaxResponseHandler(CreateHealthCheckResultStaxUnmarshaller.getInstance())

  implicit val CHZReq = new CreateHostedZoneRequestMarshaller()
  implicit val CHZRes = new StaxResponseHandler(CreateHostedZoneResultStaxUnmarshaller.getInstance())

  implicit val DHZReq = new DeleteHostedZoneRequestMarshaller()
  implicit val DHZRes = new StaxResponseHandler(DeleteHostedZoneResultStaxUnmarshaller.getInstance())

  implicit val DHCReq = new DeleteHealthCheckRequestMarshaller()
  implicit val DHCRes = new StaxResponseHandler(DeleteHealthCheckResultStaxUnmarshaller.getInstance())

  implicit val GCReq = new GetChangeRequestMarshaller()
  implicit val GCRes = new StaxResponseHandler(GetChangeResultStaxUnmarshaller.getInstance())

  implicit val GHCReq = new GetHealthCheckRequestMarshaller()
  implicit val GHCRes = new StaxResponseHandler(GetHealthCheckResultStaxUnmarshaller.getInstance())

  implicit val LHCReq = new ListHealthChecksRequestMarshaller()
  implicit val LHCRes = new StaxResponseHandler(ListHealthChecksResultStaxUnmarshaller.getInstance())

  implicit val LHZReq = new ListHostedZonesRequestMarshaller()
  implicit val LHZRes = new StaxResponseHandler(ListHostedZonesResultStaxUnmarshaller.getInstance())

  implicit val LRRSReq = new ListResourceRecordSetsRequestMarshaller()
  implicit val LRRSRes = new StaxResponseHandler(ListResourceRecordSetsResultStaxUnmarshaller.getInstance())

  val idHandler = new Route53IdRequestHandler

}

class Route53Client(val props: Route53ClientProps) extends SprayAWSClient(props) {
  import MarshallersAndUnmarshallers._

  val log = props.system.log

  override lazy val signer: Signer = new AWS3Signer()

  implicit val system = props.system

  implicit val materializer = props.materializer

  def errorResponseHandler = new DefaultErrorResponseHandler(route53ExceptionUnmarshallers)

  def sendChangeResourceRecordSets(req: ChangeResourceRecordSetsRequest): Future[ChangeResourceRecordSetsResult] =
    fold(changeResourceRecordSets(req))

  def changeResourceRecordSets(req: ChangeResourceRecordSetsRequest): Future[Either[AmazonServiceException, ChangeResourceRecordSetsResult]] =
    handle[ChangeResourceRecordSetsRequest, ChangeResourceRecordSetsResult](req)

  def sendCreateHealthCheck(req: CreateHealthCheckRequest): Future[CreateHealthCheckResult] =
    fold(createHealthCheck(req))

  def createHealthCheck(req: CreateHealthCheckRequest): Future[Either[AmazonServiceException, CreateHealthCheckResult]] =
    handle[CreateHealthCheckRequest, CreateHealthCheckResult](req)

  def sendCreateHostedZone(req: CreateHostedZoneRequest): Future[CreateHostedZoneResult] =
    fold(createHostedZone(req))

  def createHostedZone(req: CreateHostedZoneRequest): Future[Either[AmazonServiceException, CreateHostedZoneResult]] =
    handle[CreateHostedZoneRequest, CreateHostedZoneResult](req)

  def sendDeleteHostedZone(req: DeleteHostedZoneRequest): Future[DeleteHostedZoneResult] =
    fold(deleteHostedZone(req))

  def deleteHostedZone(req: DeleteHostedZoneRequest): Future[Either[AmazonServiceException, DeleteHostedZoneResult]] =
    handle[DeleteHostedZoneRequest, DeleteHostedZoneResult](req)

  def sendDeleteHealthCheck(req: DeleteHealthCheckRequest): Future[DeleteHealthCheckResult] =
    fold(deleteHealthCheck(req))

  def deleteHealthCheck(req: DeleteHealthCheckRequest): Future[Either[AmazonServiceException, DeleteHealthCheckResult]] =
    handle[DeleteHealthCheckRequest, DeleteHealthCheckResult](req)

  def sendGetChange(req: GetChangeRequest): Future[GetChangeResult] = fold(getChange(req))

  def getChange(req: GetChangeRequest): Future[Either[AmazonServiceException, GetChangeResult]] =
    handle[GetChangeRequest, GetChangeResult](req)

  def sendGetHealthCheck(req: GetHealthCheckRequest): Future[GetHealthCheckResult] = fold(getHealthCheck(req))

  def getHealthCheck(req: GetHealthCheckRequest): Future[Either[AmazonServiceException, GetHealthCheckResult]] =
    handle[GetHealthCheckRequest, GetHealthCheckResult](req)

  def sendListHealthChecks(req: ListHealthChecksRequest): Future[ListHealthChecksResult] = fold(listHealthChecks(req))

  def listHealthChecks(req: ListHealthChecksRequest): Future[Either[AmazonServiceException, ListHealthChecksResult]] =
    handle[ListHealthChecksRequest, ListHealthChecksResult](req)

  def sendListHostedZones(req: ListHostedZonesRequest): Future[ListHostedZonesResult] = fold(listHostedZones(req))

  def listHostedZones(req: ListHostedZonesRequest): Future[Either[AmazonServiceException, ListHostedZonesResult]] =
    handle[ListHostedZonesRequest, ListHostedZonesResult](req)

  def sendListResourceRecordSets(req: ListResourceRecordSetsRequest): Future[ListResourceRecordSetsResult] = fold(listResourceRecordSets(req))

  def listResourceRecordSets(req: ListResourceRecordSetsRequest): Future[Either[AmazonServiceException, ListResourceRecordSetsResult]] =
    handle[ListResourceRecordSetsRequest, ListResourceRecordSetsResult](req)

  def handle[I, O](req: I)(implicit marshaller: Marshaller[Request[I], I], handler: HttpResponseHandler[AmazonWebServiceResponse[O]]): Future[Either[AmazonServiceException, O]] = {
    val awsReq = marshaller.marshall(req)
    pipeline(request(req)).map(response[O]).map {
      _.right.map { res =>
        idHandler.afterResponse(awsReq, res, TimingInfo.startTiming())
        res
      }
    }
  }

}
