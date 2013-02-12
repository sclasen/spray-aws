package com.sclasen.spray.dynamodb

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import akka.actor.ActorSystem
import akka.util.Timeout
import concurrent.Await
import concurrent.duration._
import com.amazonaws.services.dynamodb.model.ListTablesRequest


class DynamoDBClientSpec extends WordSpec with MustMatchers {

  "A DynamoDBClient" must {
    "List tables" in {
      val system = ActorSystem("test")
      val props = DynamoDBClientProps(sys.env("AWS_ACCESS_KEY_ID"), sys.env("AWS_SECRET_ACCESS_KEY"), Timeout(10 seconds), system, system)
      val client = new DynamoDBClient(props)
      val result = Await.result(client.sendListTables(new ListTablesRequest()), 10 seconds)
      println(result)
      result.getTableNames.size() must be > 1
    }
  }

}
