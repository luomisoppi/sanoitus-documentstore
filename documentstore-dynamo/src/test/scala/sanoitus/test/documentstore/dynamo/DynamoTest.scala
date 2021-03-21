package sanoitus.test
package documentstore.dynamo

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner

import java.net.URI

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput

trait DynamoTest extends BeforeAndAfterAll { this: Suite =>

  System.setProperty("sqlite4java.library.path", "src/test/resources/libs/")

  val port = findFreePort()

  val args = Array[String]("-inMemory", "-port", port.toString)
  val server = ServerRunner.createServerFromCommandLineArgs(args)

  val client = DynamoDbAsyncClient
    .builder()
    .region(Region.EU_CENTRAL_1)
    .endpointOverride(new URI(s"http://localhost:$port"))
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("key", "secret")))
    .build()

  val tableName = "TestTable"
  val primaryKey = "SanoitusPrimary"

  val language: sanoitus.Interpreter

  override def beforeAll() = {
    server.start()
    client
      .createTable(
        CreateTableRequest
          .builder()
          .tableName(tableName)
          .attributeDefinitions(
            AttributeDefinition.builder().attributeName(primaryKey).attributeType(ScalarAttributeType.S).build()
          )
          .keySchema(KeySchemaElement.builder().attributeName(primaryKey).keyType(KeyType.HASH).build())
          .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(2000).writeCapacityUnits(2000).build)
          .build()
      )
      .get()
  }

  override def afterAll() = {
    language.close()
    server.stop()
  }
}
