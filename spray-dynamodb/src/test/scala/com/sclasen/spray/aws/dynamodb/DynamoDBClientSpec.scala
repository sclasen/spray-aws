package com.sclasen.spray.aws.dynamodb

import org.scalatest.WordSpec
import org.scalatest.Matchers
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.Timeout
import concurrent.Await
import concurrent.duration._
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest

class DynamoDBClientSpec extends WordSpec with Matchers {

  "A DynamoDBClient" must {
    "List tables" in {
      implicit val system = ActorSystem("test")
      val materializer = ActorMaterializer()
      val props = DynamoDBClientProps(sys.env("AWS_ACCESS_KEY_ID"), sys.env("AWS_SECRET_ACCESS_KEY"), Timeout(100 seconds), system, system, materializer)
      val client = new DynamoDBClient(props)
      try {
        val result = Await.result(client.sendListTables(new ListTablesRequest()), 100 seconds)
        println(result)
        result.getTableNames.size() should be >= 1
      } catch {
        case e: Exception =>
          println(e)
          e.printStackTrace()
      }
    }
  }

}
