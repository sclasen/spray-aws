package com.sclasen.spray.aws.dynamodb

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.internal.StaticCredentialsProvider
import org.scalatest.WordSpec
import org.scalatest.Matchers
import akka.actor.ActorSystem
import akka.util.Timeout
import concurrent.Await
import concurrent.duration._
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest

class DynamoDBClientSpec extends WordSpec with Matchers {

  "A DynamoDBClient" must {
    "List tables" in {
      val system = ActorSystem("test")
      val props = DynamoDBClientProps(new StaticCredentialsProvider(new BasicAWSCredentials(sys.env("AWS_ACCESS_KEY_ID"), sys.env("AWS_SECRET_ACCESS_KEY"))), Timeout(100 seconds), system, system)
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
