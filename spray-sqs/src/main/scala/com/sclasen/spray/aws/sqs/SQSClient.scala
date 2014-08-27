package com.sclasen.spray.aws.sqs

import akka.actor.{ ActorRefFactory, ActorSystem }
import com.amazonaws.auth.{ AWSCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.internal.StaticCredentialsProvider

import org.w3c.dom.Node

import collection.JavaConverters._
import com.amazonaws.services.sqs.model._
import com.amazonaws.services.sqs.model.transform._
import com.amazonaws.transform.StandardErrorUnmarshaller
import com.amazonaws.transform.{ VoidStaxUnmarshaller, Unmarshaller }
import com.amazonaws.http.{ StaxResponseHandler, DefaultErrorResponseHandler }

import concurrent.Future
import akka.util.Timeout
import com.sclasen.spray.aws._
import com.amazonaws.AmazonServiceException

case class SQSClientProps(credentialsProvider: AWSCredentialsProvider, operationTimeout: Timeout, system: ActorSystem, factory: ActorRefFactory, endpoint: String) extends SprayAWSClientProps {
  val service = "sqs"
}

object SQSClientProps {
  val defaultEndpoint = "https://sqs.us-east-1.amazonaws.com"
  def apply(key: String, secret: String, operationTimeout: Timeout, system: ActorSystem, factory: ActorRefFactory, endpoint: String = defaultEndpoint) =
    new SQSClientProps(new StaticCredentialsProvider(new BasicAWSCredentials(key, secret)), operationTimeout, system, factory, endpoint)
}

object MarshallersAndUnmarshallers {
  implicit val unitU = new StaxResponseHandler(new VoidStaxUnmarshaller[Unit]())

  implicit val addPermissionM = new AddPermissionRequestMarshaller()

  implicit val changeMessageVisibilityBatchM = new ChangeMessageVisibilityBatchRequestMarshaller()
  implicit val changeMessageVisibilityBatchU = new StaxResponseHandler(ChangeMessageVisibilityBatchResultStaxUnmarshaller.getInstance)

  implicit val createQueueM = new CreateQueueRequestMarshaller()
  implicit val createQueueU = new StaxResponseHandler(CreateQueueResultStaxUnmarshaller.getInstance)

  implicit val deleteMessageBatchM = new DeleteMessageBatchRequestMarshaller()
  implicit val deleteMessageBatchU = new StaxResponseHandler(DeleteMessageBatchResultStaxUnmarshaller.getInstance)

  implicit val deleteMessageM = new DeleteMessageRequestMarshaller()

  implicit val deleteQueueM = new DeleteQueueRequestMarshaller()

  implicit val getQueueAttributesM = new GetQueueAttributesRequestMarshaller()
  implicit val getQueueAttributesU = new StaxResponseHandler(GetQueueAttributesResultStaxUnmarshaller.getInstance)

  implicit val getQueueUrlM = new GetQueueUrlRequestMarshaller()
  implicit val getQueueUrlU = new StaxResponseHandler(GetQueueUrlResultStaxUnmarshaller.getInstance)

  implicit val listDeadLetterSourceQueuesM = new ListDeadLetterSourceQueuesRequestMarshaller()
  implicit val listDeadLetterSourceQueuesU = new StaxResponseHandler(ListDeadLetterSourceQueuesResultStaxUnmarshaller.getInstance)

  implicit val listQueuesM = new ListQueuesRequestMarshaller()
  implicit val listQueuesU = new StaxResponseHandler(ListQueuesResultStaxUnmarshaller.getInstance)

  implicit val receiveMessageM = new ReceiveMessageRequestMarshaller()
  implicit val receiveMessageU = new StaxResponseHandler(ReceiveMessageResultStaxUnmarshaller.getInstance)

  implicit val removePermissionM = new RemovePermissionRequestMarshaller()

  implicit val sendMessageBatchM = new SendMessageBatchRequestMarshaller()
  implicit val sendMessageBatchU = new StaxResponseHandler(SendMessageBatchResultStaxUnmarshaller.getInstance)

  implicit val sendMessageM = new SendMessageRequestMarshaller()
  implicit val sendMessageU = new StaxResponseHandler(SendMessageResultStaxUnmarshaller.getInstance)

  implicit val setQueueAttributesM = new SetQueueAttributesRequestMarshaller()

  val sqsExceptionUnmarshallers = List[Unmarshaller[AmazonServiceException, Node]](
    new BatchEntryIdsNotDistinctExceptionUnmarshaller(),
    new BatchRequestTooLongExceptionUnmarshaller(),
    new EmptyBatchRequestExceptionUnmarshaller(),
    new InvalidAttributeNameExceptionUnmarshaller(),
    new InvalidBatchEntryIdExceptionUnmarshaller(),
    new InvalidIdFormatExceptionUnmarshaller(),
    new InvalidMessageContentsExceptionUnmarshaller(),
    new MessageNotInflightExceptionUnmarshaller(),
    new OverLimitExceptionUnmarshaller(),
    new QueueDeletedRecentlyExceptionUnmarshaller(),
    new QueueDoesNotExistExceptionUnmarshaller(),
    new QueueNameExistsExceptionUnmarshaller(),
    new ReceiptHandleIsInvalidExceptionUnmarshaller(),
    new TooManyEntriesInBatchRequestExceptionUnmarshaller(),
    new StandardErrorUnmarshaller()
  ).toBuffer.asJava

}

class SQSClient(val props: SQSClientProps) extends SprayAWSClient(props) {

  import MarshallersAndUnmarshallers._

  val log = props.system.log

  val errorResponseHandler = new DefaultErrorResponseHandler(sqsExceptionUnmarshallers)

  def sendAddPermission(aws: AddPermissionRequest): Future[Unit] =
    fold(addPermission(aws))

  def addPermission(aws: AddPermissionRequest): Future[Either[AmazonServiceException, Unit]] =
    pipeline(request(aws)).map(response[Unit])

  def sendChangeMessageVisibilityBatch(aws: ChangeMessageVisibilityBatchRequest): Future[ChangeMessageVisibilityBatchResult] =
    fold(changeMessageVisibilityBatch(aws))

  def changeMessageVisibilityBatch(aws: ChangeMessageVisibilityBatchRequest): Future[Either[AmazonServiceException, ChangeMessageVisibilityBatchResult]] =
    pipeline(request(aws)).map(response[ChangeMessageVisibilityBatchResult])

  def sendCreateQueue(aws: CreateQueueRequest): Future[CreateQueueResult] =
    fold(createQueue(aws))

  def createQueue(aws: CreateQueueRequest): Future[Either[AmazonServiceException, CreateQueueResult]] =
    pipeline(request(aws)).map(response[CreateQueueResult])

  def sendDeleteMessageBatch(aws: DeleteMessageBatchRequest): Future[DeleteMessageBatchResult] =
    fold(deleteMessageBatch(aws))

  def deleteMessageBatch(aws: DeleteMessageBatchRequest): Future[Either[AmazonServiceException, DeleteMessageBatchResult]] =
    pipeline(request(aws)).map(response[DeleteMessageBatchResult])

  def sendDeleteMessage(aws: DeleteMessageRequest): Future[Unit] =
    fold(deleteMessage(aws))

  def deleteMessage(aws: DeleteMessageRequest): Future[Either[AmazonServiceException, Unit]] =
    pipeline(request(aws)).map(response[Unit])

  def sendDeleteQueue(aws: DeleteQueueRequest): Future[Unit] =
    fold(deleteQueue(aws))

  def deleteQueue(aws: DeleteQueueRequest): Future[Either[AmazonServiceException, Unit]] =
    pipeline(request(aws)).map(response[Unit])

  def sendGetQueueAttributes(aws: GetQueueAttributesRequest): Future[GetQueueAttributesResult] =
    fold(getQueueAttributes(aws))

  def getQueueAttributes(aws: GetQueueAttributesRequest): Future[Either[AmazonServiceException, GetQueueAttributesResult]] =
    pipeline(request(aws)).map(response[GetQueueAttributesResult])

  def sendGetQueueUrl(aws: GetQueueUrlRequest): Future[GetQueueUrlResult] =
    fold(getQueueUrl(aws))

  def getQueueUrl(aws: GetQueueUrlRequest): Future[Either[AmazonServiceException, GetQueueUrlResult]] =
    pipeline(request(aws)).map(response[GetQueueUrlResult])

  def sendListDeadLetterSourceQueues(aws: ListDeadLetterSourceQueuesRequest): Future[ListDeadLetterSourceQueuesResult] =
    fold(listDeadLetterSourceQueues(aws))

  def listDeadLetterSourceQueues(aws: ListDeadLetterSourceQueuesRequest): Future[Either[AmazonServiceException, ListDeadLetterSourceQueuesResult]] =
    pipeline(request(aws)).map(response[ListDeadLetterSourceQueuesResult])

  def sendListQueues(aws: ListQueuesRequest): Future[ListQueuesResult] =
    fold(listQueues(aws))

  def listQueues(aws: ListQueuesRequest): Future[Either[AmazonServiceException, ListQueuesResult]] =
    pipeline(request(aws)).map(response[ListQueuesResult])

  def sendReceiveMessage(aws: ReceiveMessageRequest): Future[ReceiveMessageResult] =
    fold(receiveMessage(aws))

  def receiveMessage(aws: ReceiveMessageRequest): Future[Either[AmazonServiceException, ReceiveMessageResult]] =
    pipeline(request(aws)).map(response[ReceiveMessageResult])

  def sendSendMessageBatch(aws: SendMessageBatchRequest): Future[SendMessageBatchResult] =
    fold(sendMessageBatch(aws))

  def sendMessageBatch(aws: SendMessageBatchRequest): Future[Either[AmazonServiceException, SendMessageBatchResult]] =
    pipeline(request(aws)).map(response[SendMessageBatchResult])

  def sendSendMessage(aws: SendMessageRequest): Future[SendMessageResult] =
    fold(sendMessage(aws))

  def sendMessage(aws: SendMessageRequest): Future[Either[AmazonServiceException, SendMessageResult]] =
    pipeline(request(aws)).map(response[SendMessageResult])

  def sendSetQueueAttributes(aws: SetQueueAttributesRequest): Future[Unit] =
    fold(setQueueAttributes(aws))

  def setQueueAttributes(aws: SetQueueAttributesRequest): Future[Either[AmazonServiceException, Unit]] =
    pipeline(request(aws)).map(response[Unit])

}
