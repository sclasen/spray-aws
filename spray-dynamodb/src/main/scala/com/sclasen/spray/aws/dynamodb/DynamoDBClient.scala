package com.sclasen.spray.aws.dynamodb

import akka.actor.{ ActorRefFactory, ActorSystem }
import collection.JavaConverters._
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.model.transform._
import com.amazonaws.transform.JsonErrorUnmarshaller
import concurrent.Future
import java.util.{ List => JList }
import akka.util.Timeout
import com.sclasen.spray.aws._
import com.amazonaws.AmazonServiceException

case class DynamoDBClientProps(key: String, secret: String, operationTimeout: Timeout, system: ActorSystem, factory: ActorRefFactory, endpoint: String = "https://dynamodb.us-east-1.amazonaws.com") extends SprayAWSClientProps {
  val service = "dynamodb"
}

object MarshallersAndUnmarshallers {
  implicit val batchWriteM = new BatchWriteItemRequestMarshaller()
  implicit val batchWriteU = BatchWriteItemResultJsonUnmarshaller.getInstance()
  implicit val putItemM = new PutItemRequestMarshaller()
  implicit val putItemU = PutItemResultJsonUnmarshaller.getInstance()
  implicit val delItemM = new DeleteItemRequestMarshaller()
  implicit val delItemU = DeleteItemResultJsonUnmarshaller.getInstance()
  implicit val batchGetM = new BatchGetItemRequestMarshaller()
  implicit val batchGetU = BatchGetItemResultJsonUnmarshaller.getInstance()
  implicit val listM = new ListTablesRequestMarshaller()
  implicit val listU = ListTablesResultJsonUnmarshaller.getInstance()
  implicit val qM = new QueryRequestMarshaller()
  implicit val qU = QueryResultJsonUnmarshaller.getInstance()
  implicit val uM = new UpdateItemRequestMarshaller()
  implicit val uU = UpdateItemResultJsonUnmarshaller.getInstance()
  implicit val dM = new DescribeTableRequestMarshaller()
  implicit val dU = DescribeTableResultJsonUnmarshaller.getInstance()
  implicit val sM = new ScanRequestMarshaller()
  implicit val sU = ScanResultJsonUnmarshaller.getInstance()
  implicit val cM = new CreateTableRequestMarshaller()
  implicit val cU = CreateTableResultJsonUnmarshaller.getInstance()
  implicit val upM = new UpdateTableRequestMarshaller()
  implicit val upU = UpdateTableResultJsonUnmarshaller.getInstance()
  implicit val deM = new DeleteTableRequestMarshaller()
  implicit val deU = DeleteTableResultJsonUnmarshaller.getInstance()
  implicit val getM = new GetItemRequestMarshaller()
  implicit val getU = GetItemResultJsonUnmarshaller.getInstance()

  val dynamoExceptionUnmarshallers: JList[JsonErrorUnmarshaller] = List(
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

  def exceptionUnmarshallers: JList[JsonErrorUnmarshaller] = dynamoExceptionUnmarshallers

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

