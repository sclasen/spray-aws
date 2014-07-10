package com.sclasen.spray.aws.s3

import java.util.UUID
import java.io.ByteArrayInputStream
import java.nio.charset.Charset
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.JavaConverters._

import org.scalatest.Matchers
import org.scalatest.fixture.WordSpec
import akka.actor.ActorSystem
import akka.util.Timeout
import com.amazonaws.services.s3.model._

class S3ClientSpec extends WordSpec with Matchers {

  val system = ActorSystem("test")
  val props = S3ClientProps(sys.env("AWS_ACCESS_KEY_ID"), sys.env("AWS_SECRET_ACCESS_KEY"), Timeout(100 seconds), system, system)
  val client = new S3Client(props)
  val timeout = 10 seconds

  type FixtureParam = String

  def withFixture(test: OneArgTest) = {
    val bucketName = UUID.randomUUID.toString

    Await.result(client.createBucket(new CreateBucketRequest(bucketName)), timeout)
    try {
      test(bucketName)
    } finally {
      val listRequest = new ListObjectsRequest()
      listRequest.setBucketName(bucketName)
      val listResult = Await.result(client.listObjects(listRequest), timeout)
      val names = listResult.right.get.getObjectSummaries.asScala.map(_.getKey)

      for (name <- names) {
        val deleteRequest = new DeleteObjectRequest(bucketName, name)
        Await.result(client.deleteObject(deleteRequest), timeout)
      }

      Await.result(client.deleteBucket(new DeleteBucketRequest(bucketName)), timeout)
    }
  }

  "A S3Client" when {
    "no buckets exists" should {
      "create, list, and delete a bucket" in { () =>

        val bucketName = UUID.randomUUID.toString

        val createResult = Await.result(client.createBucket(new CreateBucketRequest(bucketName)), timeout)
        createResult shouldEqual Right(null)

        val listResult = Await.result(client.listBuckets(new ListBucketsRequest), timeout)
        listResult.right.get.map(_.getName) should contain(bucketName)

        val deleteResult = Await.result(client.deleteBucket(new DeleteBucketRequest(bucketName)), timeout)
        deleteResult shouldEqual Right(null)
      }
    }

    "a bucket exists" should {
      "add, get, and delete an object" in { bucketName =>

        val objectName = "testObject"
        val data = "Some test[ ] data"
        val inputStream = new ByteArrayInputStream(data.getBytes(Charset.defaultCharset))
        val metadata = new ObjectMetadata
        val putResult = Await.result(client.putObject(new PutObjectRequest(bucketName, objectName, inputStream, metadata)), timeout)

        assert(putResult.isRight)

        val getResult = Await.result(client.getObject(new GetObjectRequest(bucketName, objectName)), timeout)
        val is = getResult.right.get.getObjectContent
        new String(Stream.continually(is.read).takeWhile(-1 != _).map(_.toByte).toArray, Charset.defaultCharset) shouldEqual data

        val deleteResult = Await.result(client.deleteObject(new DeleteObjectRequest(bucketName, objectName)), timeout)
        assert(deleteResult.isRight)
      }

      "add, list, and get an object containing binary" in { bucketName =>

        val objectName = "testObjectBinary"
        val data: Array[Byte] = Array(1, 2, 3, 4, 5, 6, 7, 8)
        val inputStream = new ByteArrayInputStream(data)
        val metadata = new ObjectMetadata
        val putResult = Await.result(client.putObject(new PutObjectRequest(bucketName, objectName, inputStream, metadata)), timeout)

        assert(putResult.isRight)

        val listRequest = new ListObjectsRequest()
        listRequest.setBucketName(bucketName)
        val listResult = Await.result(client.listObjects(listRequest), timeout)

        listResult.right.get.getObjectSummaries.get(0).getKey shouldEqual objectName

        val getResult = Await.result(client.getObject(new GetObjectRequest(bucketName, objectName)), timeout)
        val is = getResult.right.get.getObjectContent
        Stream.continually(is.read).takeWhile(-1 != _).map(_.toByte).toArray shouldEqual data
      }
    }
  }
}
