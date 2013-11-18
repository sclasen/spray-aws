package com.sclasen.spray.aws.dynamodb

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import akka.actor.ActorSystem
import akka.util.Timeout
import concurrent.Await
import concurrent.duration._
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest

class DynamoDBClientSpec extends WordSpec with MustMatchers {

  "A DynamoDBClient" must {
    "List tables" in {
      val system = ActorSystem("test")
      val props = DynamoDBClientProps(sys.env("AWS_ACCESS_KEY_ID"), sys.env("AWS_SECRET_ACCESS_KEY"), Timeout(100 seconds), system, system)
      val client = new DynamoDBClient(props)
      try {
        val result = Await.result(client.sendListTables(new ListTablesRequest()), 100 seconds)
        println(result)
        result.getTableNames.size() must be > 1
      } catch {
        case e: Exception =>
          println(e)
          e.printStackTrace()
      }
    }
  }

}
