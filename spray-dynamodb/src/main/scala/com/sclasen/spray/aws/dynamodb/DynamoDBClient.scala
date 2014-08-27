package com.sclasen.spray.aws.dynamodb

import akka.actor.{ ActorRefFactory, ActorSystem }
import com.amazonaws.auth.{ BasicAWSCredentials, AWSCredentialsProvider }
import com.amazonaws.internal.StaticCredentialsProvider
import collection.JavaConverters._
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.model.transform._
import com.amazonaws.transform.{ JsonErrorUnmarshaller, Unmarshaller }
import com.amazonaws.http.{ JsonErrorResponseHandler, JsonResponseHandler }
import com.amazonaws.util.json.JSONObject
import concurrent.Future
import java.util.{ List => JList }
import akka.util.Timeout
import com.sclasen.spray.aws._
import com.amazonaws.AmazonServiceException

case class DynamoDBClientProps(credentialsProvider: AWSCredentialsProvider, operationTimeout: Timeout, system: ActorSystem, factory: ActorRefFactory, endpoint: String) extends SprayAWSClientProps {
  val service = "dynamodb"
}

object DynamoDBClientProps {
  def apply(key: String, secret: String, operationTimeout: Timeout, system: ActorSystem, factory: ActorRefFactory, endpoint: String = "https://dynamodb.us-east-1.amazonaws.com") =
    new DynamoDBClientProps(new StaticCredentialsProvider(new BasicAWSCredentials(key, secret)), operationTimeout, system, factory, endpoint)
}

object MarshallersAndUnmarshallers {
  implicit val batchWriteM = new BatchWriteItemRequestMarshaller()
  implicit val batchWriteU = new JsonResponseHandler(BatchWriteItemResultJsonUnmarshaller.getInstance())
  implicit val putItemM = new PutItemRequestMarshaller()
  implicit val putItemU = new JsonResponseHandler(PutItemResultJsonUnmarshaller.getInstance())
  implicit val delItemM = new DeleteItemRequestMarshaller()
  implicit val delItemU = new JsonResponseHandler(DeleteItemResultJsonUnmarshaller.getInstance())
  implicit val batchGetM = new BatchGetItemRequestMarshaller()
  implicit val batchGetU = new JsonResponseHandler(BatchGetItemResultJsonUnmarshaller.getInstance())
  implicit val listM = new ListTablesRequestMarshaller()
  implicit val listU = new JsonResponseHandler(ListTablesResultJsonUnmarshaller.getInstance())
  implicit val qM = new QueryRequestMarshaller()
  implicit val qU = new JsonResponseHandler(QueryResultJsonUnmarshaller.getInstance())
  implicit val uM = new UpdateItemRequestMarshaller()
  implicit val uU = new JsonResponseHandler(UpdateItemResultJsonUnmarshaller.getInstance())
  implicit val dM = new DescribeTableRequestMarshaller()
  implicit val dU = new JsonResponseHandler(DescribeTableResultJsonUnmarshaller.getInstance())
  implicit val sM = new ScanRequestMarshaller()
  implicit val sU = new JsonResponseHandler(ScanResultJsonUnmarshaller.getInstance())
  implicit val cM = new CreateTableRequestMarshaller()
  implicit val cU = new JsonResponseHandler(CreateTableResultJsonUnmarshaller.getInstance())
  implicit val upM = new UpdateTableRequestMarshaller()
  implicit val upU = new JsonResponseHandler(UpdateTableResultJsonUnmarshaller.getInstance())
  implicit val deM = new DeleteTableRequestMarshaller()
  implicit val deU = new JsonResponseHandler(DeleteTableResultJsonUnmarshaller.getInstance())
  implicit val getM = new GetItemRequestMarshaller()
  implicit val getU = new JsonResponseHandler(GetItemResultJsonUnmarshaller.getInstance())

  val dynamoExceptionUnmarshallers = List[JsonErrorUnmarshaller](
    new LimitExceededExceptionUnmarshaller(),
    new InternalServerErrorExceptionUnmarshaller(),
    new ProvisionedThroughputExceededExceptionUnmarshaller(),
    new ResourceInUseExceptionUnmarshaller(),
    new ConditionalCheckFailedExceptionUnmarshaller(),
    new ResourceNotFoundExceptionUnmarshaller(),
    new JsonErrorUnmarshaller()).toBuffer.asJava

}

class DynamoDBClient(val props: DynamoDBClientProps) extends SprayAWSClient(props) {

  import MarshallersAndUnmarshallers._

  val log = props.system.log

  val errorResponseHandler = new JsonErrorResponseHandler(dynamoExceptionUnmarshallers)

  def sendListTables(aws: ListTablesRequest): Future[ListTablesResult] = fold(listTables(aws))

  def listTables(aws: ListTablesRequest): Future[Either[AmazonServiceException, ListTablesResult]] =
    pipeline(request(aws)).map(response[ListTablesResult])

  def sendQuery(aws: QueryRequest): Future[QueryResult] = fold(query(aws))

  def query(aws: QueryRequest): Future[Either[AmazonServiceException, QueryResult]] =
    pipeline(request(aws)).map(response[QueryResult])

  def sendUpdateItem(aws: UpdateItemRequest): Future[UpdateItemResult] = fold(updateItem(aws))

  def updateItem(aws: UpdateItemRequest): Future[Either[AmazonServiceException, UpdateItemResult]] =
    pipeline(request(aws)).map(response[UpdateItemResult])

  def sendPutItem(aws: PutItemRequest): Future[PutItemResult] = fold(putItem(aws))

  def putItem(aws: PutItemRequest): Future[Either[AmazonServiceException, PutItemResult]] =
    pipeline(request(aws)).map(response[PutItemResult])

  def sendDescribeTable(aws: DescribeTableRequest): Future[DescribeTableResult] = fold(describeTable(aws))

  def describeTable(aws: DescribeTableRequest): Future[Either[AmazonServiceException, DescribeTableResult]] =
    pipeline(request(aws)).map(response[DescribeTableResult])

  def sendCreateTable(aws: CreateTableRequest): Future[CreateTableResult] = fold(createTable(aws))

  def createTable(aws: CreateTableRequest): Future[Either[AmazonServiceException, CreateTableResult]] =
    pipeline(request(aws)).map(response[CreateTableResult])

  def sendUpdateTable(aws: UpdateTableRequest): Future[UpdateTableResult] = fold(updateTable(aws))

  def updateTable(aws: UpdateTableRequest): Future[Either[AmazonServiceException, UpdateTableResult]] =
    pipeline(request(aws)).map(response[UpdateTableResult])

  def sendDeleteTable(aws: DeleteTableRequest): Future[DeleteTableResult] = fold(deleteTable(aws))

  def deleteTable(aws: DeleteTableRequest): Future[Either[AmazonServiceException, DeleteTableResult]] =
    pipeline(request(aws)).map(response[DeleteTableResult])

  def sendGetItem(aws: GetItemRequest): Future[GetItemResult] = fold(getItem(aws))

  def getItem(aws: GetItemRequest): Future[Either[AmazonServiceException, GetItemResult]] =
    pipeline(request(aws)).map(response[GetItemResult])

  def sendBatchWriteItem(awsWrite: BatchWriteItemRequest): Future[BatchWriteItemResult] =
    fold(batchWriteItem(awsWrite))

  def batchWriteItem(awsWrite: BatchWriteItemRequest): Future[Either[AmazonServiceException, BatchWriteItemResult]] =
    pipeline(request(awsWrite)).map(response[BatchWriteItemResult])

  def sendBatchGetItem(awsGet: BatchGetItemRequest): Future[BatchGetItemResult] = fold(batchGetItem(awsGet))

  def batchGetItem(awsGet: BatchGetItemRequest): Future[Either[AmazonServiceException, BatchGetItemResult]] =
    pipeline(request(awsGet)).map(response[BatchGetItemResult])

  def sendDeleteItem(awsDel: DeleteItemRequest): Future[DeleteItemResult] = fold(deleteItem(awsDel))

  def deleteItem(awsDel: DeleteItemRequest): Future[Either[AmazonServiceException, DeleteItemResult]] =
    pipeline(request(awsDel)).map(response[DeleteItemResult])

}
