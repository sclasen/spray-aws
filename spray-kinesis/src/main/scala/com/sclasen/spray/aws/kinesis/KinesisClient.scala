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

case class KinesisClientProps(key: String, secret: String, operationTimeout: Timeout, system: ActorSystem, factory: ActorRefFactory, endpoint: String = "kinesis.us-east-1.amazonaws.com") extends SprayAWSClientProps {
  val service = "kinesis"
}

private object MarshallersAndUnmarshallers {
  implicit val createStreamRequest = new CreateStreamRequestMarshaller()

  implicit val deleteStreamRequest = new DeleteStreamRequestMarshaller()

  implicit val describeStreamRequest = new DescribeStreamRequestMarshaller()
  implicit val describeStreamResult = DescribeStreamResultJsonUnmarshaller.getInstance()

  implicit val getRecordsRequest = new GetRecordsRequestMarshaller()
  implicit val getRecordsResult = GetRecordsResultJsonUnmarshaller.getInstance()

  implicit val getShardIteratorRequest = new GetShardIteratorRequestMarshaller()
  implicit val getShardIteratorResult = GetShardIteratorResultJsonUnmarshaller.getInstance()

  implicit val listStreamsRequest = new ListStreamsRequestMarshaller()
  implicit val listStreamsResult = ListStreamsResultJsonUnmarshaller.getInstance()

  implicit val mergeShardsRequest = new MergeShardsRequestMarshaller()

  implicit val putRecordsRequest = new PutRecordRequestMarshaller()
  implicit val putRecordsResult = PutRecordResultJsonUnmarshaller.getInstance()

  implicit val splitShardRequest = new SplitShardRequestMarshaller()

  implicit val unitResult = UnitUnmarshaller

  val kinesisExceptionUnmarshallers: JList[JsonErrorUnmarshaller] = List(
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
  def unmarshall(context: JsonUnmarshallerContext): Unit = {
    return ;
  }
}

class KinesisClient(val props: KinesisClientProps) extends SprayAWSClient(props) {

  import MarshallersAndUnmarshallers._

  val log = props.system.log

  def exceptionUnmarshallers: JList[JsonErrorUnmarshaller] = kinesisExceptionUnmarshallers

  /**
   * Creates a stream.
   *
   * Returns 200 with an empty body on success.
   */
  def sendCreateStream(aws: CreateStreamRequest): Future[Unit] =
    pipeline(request(aws)).map(response[Unit])

  /**
   * Deletes a stream.
   *
   * Returns 200 with an empty body on success.
   */
  def deleteCreateStream(aws: DeleteStreamRequest): Future[Unit] =
    pipeline(request(aws)).map(response[Unit])

  /**
   * Get metadata about a stream.
   */
  def describeStream(aws: DescribeStreamRequest): Future[DescribeStreamResult] =
    pipeline(request(aws)).map(response[DescribeStreamResult])

  /**
   * Get records from a specific shard.  Open the iterator first with getShardIterator.
   */
  def getRecords(aws: GetRecordsRequest): Future[GetRecordsResult] =
    pipeline(request(aws)).map(response[GetRecordsResult])

  /**
   * Opens an iterator onto a specific shard.
   */
  def getShardIterator(aws: GetShardIteratorRequest): Future[GetShardIteratorResult] =
    pipeline(request(aws)).map(response[GetShardIteratorResult])

  /**
   * Get list of all streams.
   */
  def listStreams(aws: ListStreamsRequest): Future[ListStreamsResult] =
    pipeline(request(aws)).map(response[ListStreamsResult])

  /**
   * Merge two shards.
   *
   * Returns 200 with empty body on success.
   */
  def mergeShards(aws: MergeShardsRequest): Future[Unit] =
    pipeline(request(aws)).map(response[Unit])

  /**
   * Add a record to a stream.
   */
  def putRecord(aws: PutRecordRequest): Future[PutRecordResult] =
    pipeline(request(aws)).map(response[PutRecordResult])

  /**
   * Split a shard into two.
   *
   * Returns 200 with empty body on success.
   */
  def splitShard(aws: SplitShardRequest): Future[Unit] =
    pipeline(request(aws)).map(response[Unit])

}
