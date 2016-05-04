package com.sclasen.spray.aws.s3

import java.util.UUID
import java.io.{ ByteArrayInputStream, InputStream }
import java.nio.charset.Charset
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.collection.JavaConverters._

import org.scalatest.Matchers
import org.scalatest.fixture.WordSpec
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.amazonaws.services.s3.model._

class S3ClientSpec extends WordSpec with Matchers {

  implicit val system = ActorSystem("test")
  val materializer = ActorMaterializer()
  val props = S3ClientProps(sys.env("AWS_ACCESS_KEY_ID"), sys.env("AWS_SECRET_ACCESS_KEY"), Timeout(100 seconds), system, system, materializer)
  val client = new S3Client(props)
  val timeout = 10 seconds

  type FixtureParam = String

  def sync[T](future: Future[T]): T = {
    Await.result(future, timeout)
  }

  def streamToBytes(is: InputStream): Array[Byte] = {
    Stream.continually(is.read).takeWhile(-1 != _).map(_.toByte).toArray
  }

  def withFixture(test: OneArgTest) = {
    val bucketName = UUID.randomUUID.toString

    sync(client.createBucket(new CreateBucketRequest(bucketName)))

    try {
      test(bucketName)
    } finally {
      val listRequest = new ListObjectsRequest()
      listRequest.setBucketName(bucketName)
      val listResult = sync(client.listObjects(listRequest))
      val names = listResult.right.get.getObjectSummaries.asScala.map(_.getKey)

      for (name <- names) {
        sync(client.deleteObject(new DeleteObjectRequest(bucketName, name)))
      }

      sync(client.deleteBucket(new DeleteBucketRequest(bucketName)))
    }
  }

  "A S3Client" when {
    "no buckets exists" should {
      "create, list, and delete a bucket" in { () =>
        val bucketName = UUID.randomUUID.toString
        val createResult = sync(client.createBucket(new CreateBucketRequest(bucketName)))
        createResult shouldEqual Right(null)

        val listResult = sync(client.listBuckets(new ListBucketsRequest))
        listResult should be('right)
        listResult.right.get.map(_.getName) should contain(bucketName)

        val deleteResult = sync(client.deleteBucket(new DeleteBucketRequest(bucketName)))
        deleteResult shouldEqual Right(null)
      }
    }

    "a bucket exists" should {
      "add, get, and delete an object" in { bucketName =>
        val objectName = "testObject"
        val data = "Some test[ ] data"
        val inputStream = new ByteArrayInputStream(data.getBytes(Charset.defaultCharset))
        val metadata = new ObjectMetadata
        val putResult = sync(client.putObject(new PutObjectRequest(bucketName, objectName, inputStream, metadata)))
        putResult shouldBe ('right)

        val getResult = sync(client.getObject(new GetObjectRequest(bucketName, objectName)))
        getResult shouldBe ('right)
        new String(streamToBytes(getResult.right.get.getObjectContent), Charset.defaultCharset) shouldEqual data

        val deleteResult = sync(client.deleteObject(new DeleteObjectRequest(bucketName, objectName)))
        deleteResult shouldBe ('right)
      }

      "add, list, and get an object containing binary" in { bucketName =>
        val objectName = "testObjectBinary"
        val data: Array[Byte] = Array(1, 2, 3, 4, 5, 6, 7, 8)
        val putResult = sync(client.putObject(new PutObjectRequest(bucketName, objectName, new ByteArrayInputStream(data), new ObjectMetadata)))
        putResult shouldBe ('right)

        val listRequest = new ListObjectsRequest()
        listRequest.setBucketName(bucketName)
        val listResult = sync(client.listObjects(listRequest))

        listResult shouldBe ('right)
        listResult.right.get.getObjectSummaries.get(0).getKey shouldEqual objectName

        val getResult = sync(client.getObject(new GetObjectRequest(bucketName, objectName)))
      }

      "put and get and object with umlauts and spaces in the name" in { bucketName =>
        val objectName = "öbjects näme"
        val data: Array[Byte] = Array(1, 2, 3, 4, 5, 6, 7, 8)
        val putResult = sync(client.putObject(new PutObjectRequest(bucketName, objectName, new ByteArrayInputStream(data), new ObjectMetadata)))
        putResult shouldBe ('right)

        val getResult = sync(client.getObject(new GetObjectRequest(bucketName, objectName)))
        getResult shouldBe ('right)
        streamToBytes(getResult.right.get.getObjectContent) shouldEqual data
      }

      "get a non-existing object" in { bucketName =>
        val getResult = sync(client.getObject(new GetObjectRequest(bucketName, "objectName")))
        getResult shouldBe ('left)
      }
    }
  }
}
