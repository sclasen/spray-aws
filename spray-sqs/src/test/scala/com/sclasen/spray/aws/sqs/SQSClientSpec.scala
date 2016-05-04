package com.sclasen.spray.aws.sqs

import java.net.URLEncoder
import java.util.UUID

import org.scalatest.WordSpec
import org.scalatest.Matchers
import akka.stream.ActorMaterializer
import akka.actor.ActorSystem
import akka.util.Timeout
import concurrent.Await
import concurrent.duration._
import com.amazonaws.services.sqs.model._

class SQSClientSpec extends WordSpec with Matchers {

  "A SQSClient" must {
    "Create queue, use it and delete it" in {
      implicit val system = ActorSystem("test")
      val materializer = ActorMaterializer()
      val props = SQSClientProps(sys.env("AWS_ACCESS_KEY_ID"), sys.env("AWS_SECRET_ACCESS_KEY"), Timeout(100 seconds), system, system, materializer)
      val client = new SQSClient(props)
      val result = Await.result(client.sendCreateQueue(new CreateQueueRequest(UUID.randomUUID.toString)), 100 seconds)
      val queueUrl = result.getQueueUrl
      val result2 = Await.result(client.sendListQueues(new ListQueuesRequest()), 100 seconds)
      result2.getQueueUrls.size should be >= 1
      val result3 = Await.result(client.sendSendMessage(new SendMessageRequest(queueUrl, "Test message")), 100 seconds)
      val result4 = Await.result(client.sendReceiveMessage(new ReceiveMessageRequest(queueUrl)), 100 seconds)
      result4.getMessages.size shouldBe 1
      val message = result4.getMessages.get(0)
      Await.result(client.sendDeleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle())), 100 seconds)
      Await.result(client.sendDeleteQueue(new DeleteQueueRequest(queueUrl)), 100 seconds)
    }
  }

}
