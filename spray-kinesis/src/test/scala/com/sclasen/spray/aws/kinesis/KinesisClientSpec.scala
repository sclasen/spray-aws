package com.sclasen.spray.aws.kinesis

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import akka.actor.ActorSystem
import akka.util.Timeout
import concurrent.Await
import concurrent.duration._
import com.amazonaws.services.kinesis.model.ListStreamsRequest
import com.amazonaws.services.kinesis.model.PutRecordRequest
import java.nio.ByteBuffer
import com.amazonaws.services.kinesis.model.DescribeStreamRequest
import com.amazonaws.services.kinesis.model.GetRecordsRequest
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest
import com.amazonaws.services.kinesis.model.ShardIteratorType._
import scala.collection.JavaConversions._
import com.amazonaws.services.kinesis.model.ShardIteratorType

class KinesisClientSpec extends WordSpec with MustMatchers {

  val testStreamName = "unittest_spray_aws"

  val randomBytes = java.util.UUID.randomUUID.toString.getBytes

  val system = ActorSystem("test")
  val props = KinesisClientProps(sys.env("AWS_ACCESS_KEY_ID"), sys.env("AWS_SECRET_ACCESS_KEY"), Timeout(10 seconds), system, system)
  val client = new KinesisClient(props)

  "A KinesisClient" must {
    "List streams" in {
      val result = Await.result(client.listStreams(new ListStreamsRequest()), 10 seconds)
      println(result)
      result.getStreamNames.size must be >= 1
    }
    "Write to a stream" in {
      val request = new PutRecordRequest()
      println("Adding record " + new String(randomBytes))
      val buffer = ByteBuffer.wrap(randomBytes)
      request.setData(buffer)
      request.setPartitionKey("somepartitionkeyvalue")
      request.setStreamName(testStreamName)
      val result = Await.result(client.putRecord(request), 10 seconds)
      println(result)
      assert(!result.getSequenceNumber.isEmpty)
    }
    "Describe a stream" in {
      val request = new DescribeStreamRequest()
      request.setStreamName(testStreamName)
      val result = Await.result(client.describeStream(request), 10 seconds)
      println(result)
      result.getStreamDescription.getShards.size must be > 0
    }
    "Iterate over records in a stream" in {
      val description =
        {
          val request = new DescribeStreamRequest()
          request.setStreamName(testStreamName)
          val result = Await.result(client.describeStream(request), 10 seconds)
          println(result)
          result.getStreamDescription.getShards.size must be > 0
          result.getStreamDescription
        }

      def shardIterator(shardId: String, start: Option[String]) =
        {
          val request = new GetShardIteratorRequest()
          request.setStreamName(testStreamName)
          if (start.isEmpty) {
            request.setShardIteratorType(TRIM_HORIZON)
          } else {
            request.setShardIteratorType(AT_SEQUENCE_NUMBER)
            request.setStartingSequenceNumber(start.get)
          }
          request.setShardId(shardId)
          val result = Await.result(client.getShardIterator(request), 10 seconds)
          println(result)
          assert(result.getShardIterator != null)
          result.getShardIterator
        }

      def readRecords(n: Int, shardIterator: String): List[ByteBuffer] = {
        val request = new GetRecordsRequest()
        request.setShardIterator(shardIterator)
        val result = Await.result(client.getRecords(request), 10 seconds)
        println(result)
        result.getRecords.foreach(println(_))

        val records = result.getRecords.toList.map(_.getData)
        val nextIterator = result.getNextShardIterator
        // Stop when we don't find any more records
        // Kinesis will let us keep 'tailing' the stream, but we don't want to do this
        if (n == 0 && (records.isEmpty || nextIterator == null || nextIterator.isEmpty)) {
          return records
        } else {
          return records ++ readRecords(n - 1, nextIterator)
        }
      }

      // Kinesis has a delay between putting the record and reading it out
      Thread.sleep(10000)

      // Kinesis has a weird problem where it needs us to loop before it will return records
      val shards = description.getShards
      val records = shards.flatMap(shard => readRecords(128, shardIterator(shard.getShardId, Some(shard.getSequenceNumberRange.getStartingSequenceNumber))))
      records.size() must be > 0
      val added = ByteBuffer.wrap(randomBytes)
      assert(records.exists(added.equals(_)))
    }
  }

}
