package sanoitus
package test
package documentstore
package ram

import org.scalacheck.Gen

import sanoitus.documentstore.ram.RamDocumentStoreInterpreter
import sanoitus.util.LoggingExecutionService

class RamDocumentStoreLogicSpecification extends DocumentStoreLogicSpecification[String] {

  override val language = new RamDocumentStoreInterpreter[String]()

  implicit override val generatorDrivenConfig =
    PropertyCheckConfiguration(minSuccessful = 10000,
                               workers = 16,
                               minSize = 1,
                               sizeRange = 100,
                               maxDiscardedFactor = 0.00000001)

  override val es = LoggingExecutionService(10, 10000)

  override val valueGen: Gen[String] = uuid
}
