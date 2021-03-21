package sanoitus
package test
package documentstore
package dynamo

import org.scalacheck.Gen

import sanoitus.documentstore.dynamo.DynamoDocumentStoreInterpreter
import sanoitus.util.LoggingExecutionService

class DynamoDocumentStoreConcurrencySpecification
    extends DocumentStoreConcurrencySpecification[String]
    with DynamoTest {

  implicit override val generatorDrivenConfig =
    PropertyCheckConfiguration(minSuccessful = 2000,
                               workers = 8,
                               minSize = 1,
                               sizeRange = 8,
                               maxDiscardedFactor = 0.00000001)

  override lazy val language = new DynamoDocumentStoreInterpreter(client, tableName, primaryKey)

  override val es = LoggingExecutionService(10, 1000)

  override val valueGen: Gen[String] = uuid
}
