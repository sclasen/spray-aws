package com.sclasen.spray.aws.kinesis

import akka.actor.{ ActorRefFactory, ActorSystem }
import collection.JavaConverters._
import com.amazonaws.services.kinesis.model._
import com.amazonaws.services.kinesis.model.transform._
import com.amazonaws.transform.JsonErrorUnmarshaller
import concurrent.Future
import java.util.{ List => JList }
import akka.util.Timeout
import com.sclasen.spray.aws._
import com.amazonaws.services.kinesis.model.ListStreamsRequest
import com.amazonaws.services.kinesis.model.ListStreamsResult
import com.amazonaws.transform.Unmarshaller
import com.amazonaws.transform.JsonUnmarshallerContext
import com.amazonaws.AmazonServiceException
import com.amazonaws.util.json.JSONObject
import com.amazonaws.http.{ JsonResponseHandler, JsonErrorResponseHandler }

case class KinesisClientProps(key: String, secret: String, operationTimeout: Timeout, system: ActorSystem, factory: ActorRefFactory, endpoint: String = "https://kinesis.us-east-1.amazonaws.com") extends SprayAWSClientProps {
  val service = "kinesis"
}

private object MarshallersAndUnmarshallers {
  implicit val createStreamRequest = new CreateStreamRequestMarshaller()

  implicit val deleteStreamRequest = new DeleteStreamRequestMarshaller()

  implicit val describeStreamRequest = new DescribeStreamRequestMarshaller()
  implicit val describeStreamResult = new JsonResponseHandler(DescribeStreamResultJsonUnmarshaller.getInstance())

  implicit val getRecordsRequest = new GetRecordsRequestMarshaller()
  implicit val getRecordsResult = new JsonResponseHandler(GetRecordsResultJsonUnmarshaller.getInstance())

  implicit val getShardIteratorRequest = new GetShardIteratorRequestMarshaller()
  implicit val getShardIteratorResult = new JsonResponseHandler(GetShardIteratorResultJsonUnmarshaller.getInstance())

  implicit val listStreamsRequest = new ListStreamsRequestMarshaller()
  implicit val listStreamsResult = new JsonResponseHandler(ListStreamsResultJsonUnmarshaller.getInstance())

  implicit val mergeShardsRequest = new MergeShardsRequestMarshaller()

  implicit val putRecordsRequest = new PutRecordRequestMarshaller()
  implicit val putRecordsResult = new JsonResponseHandler(PutRecordResultJsonUnmarshaller.getInstance())

  implicit val splitShardRequest = new SplitShardRequestMarshaller()

  implicit val unitResult = new JsonResponseHandler(UnitUnmarshaller)

  val kinesisExceptionUnmarshallers = List[JsonErrorUnmarshaller](
    new InvalidArgumentExceptionUnmarshaller(),
    new LimitExceededExceptionUnmarshaller(),
    new ResourceInUseExceptionUnmarshaller(),
    new ResourceNotFoundExceptionUnmarshaller(),
    new ExpiredIteratorExceptionUnmarshaller(),
    new ProvisionedThroughputExceededExceptionUnmarshaller(),
    new JsonErrorUnmarshaller()).toBuffer.asJava

}

/**
 * Unmarshaller for empty results.
 *
 * Like VoidJsonUnmarshaller, but returns a Scala Unit rather than a Java Void/null.
 */
private object UnitUnmarshaller extends Unmarshaller[Unit, JsonUnmarshallerContext] {
  def unmarshall(context: JsonUnmarshallerContext): Unit = ()
}

class KinesisClient(val props: KinesisClientProps) extends SprayAWSClient(props) {

  import MarshallersAndUnmarshallers._

  val log = props.system.log

  val errorResponseHandler = new JsonErrorResponseHandler(kinesisExceptionUnmarshallers)

  /**
   * Creates a stream.
   *
   * Returns 200 with an empty body on success.
   */
  def createStream(aws: CreateStreamRequest): Future[Either[AmazonServiceException, Unit]] =
    pipeline(request(aws)).map(response[Unit])

  def sendCreateStream(aws: CreateStreamRequest): Future[Unit] = fold(createStream(aws))

  /**
   * Deletes a stream.
   *
   * Returns 200 with an empty body on success.
   */
  def deleteStream(aws: DeleteStreamRequest): Future[Either[AmazonServiceException, Unit]] =
    pipeline(request(aws)).map(response[Unit])

  def sendDeleteStream(aws: DeleteStreamRequest): Future[Unit] = fold(deleteStream(aws))

  /**
   * Get metadata about a stream.
   */
  def describeStream(aws: DescribeStreamRequest): Future[Either[AmazonServiceException, DescribeStreamResult]] =
    pipeline(request(aws)).map(response[DescribeStreamResult])

  def sendDescribeStream(aws: DescribeStreamRequest): Future[DescribeStreamResult] = fold(describeStream(aws))

  /**
   * Get records from a specific shard.  Open the iterator first with getShardIterator.
   */
  def getRecords(aws: GetRecordsRequest): Future[Either[AmazonServiceException, GetRecordsResult]] =
    pipeline(request(aws)).map(response[GetRecordsResult])

  def sendGetRecords(aws: GetRecordsRequest): Future[GetRecordsResult] = fold(getRecords(aws))

  /**
   * Opens an iterator onto a specific shard.
   */
  def getShardIterator(aws: GetShardIteratorRequest): Future[Either[AmazonServiceException, GetShardIteratorResult]] =
    pipeline(request(aws)).map(response[GetShardIteratorResult])

  def sendGetShardIterator(aws: GetShardIteratorRequest): Future[GetShardIteratorResult] = fold(getShardIterator(aws))

  /**
   * Get list of all streams.
   */
  def listStreams(aws: ListStreamsRequest): Future[Either[AmazonServiceException, ListStreamsResult]] =
    pipeline(request(aws)).map(response[ListStreamsResult])

  def sendListStreams(aws: ListStreamsRequest): Future[ListStreamsResult] = fold(listStreams(aws))

  /**
   * Merge two shards.
   *
   * Returns 200 with empty body on success.
   */
  def mergeShards(aws: MergeShardsRequest): Future[Either[AmazonServiceException, Unit]] =
    pipeline(request(aws)).map(response[Unit])

  def sendMergeShards(aws: MergeShardsRequest): Future[Unit] = fold(mergeShards(aws))

  /**
   * Add a record to a stream.
   */
  def putRecord(aws: PutRecordRequest): Future[Either[AmazonServiceException, PutRecordResult]] =
    pipeline(request(aws)).map(response[PutRecordResult])

  def sendPutRecord(aws: PutRecordRequest): Future[PutRecordResult] = fold(putRecord(aws))

  /**
   * Split a shard into two.
   *
   * Returns 200 with empty body on success.
   */
  def splitShard(aws: SplitShardRequest): Future[Either[AmazonServiceException, Unit]] =
    pipeline(request(aws)).map(response[Unit])

  def sendSplitShard(aws: SplitShardRequest): Future[Unit] = fold(splitShard(aws))

}
