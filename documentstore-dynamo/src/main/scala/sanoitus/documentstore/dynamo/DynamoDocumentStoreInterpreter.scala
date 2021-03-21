package sanoitus
package documentstore
package dynamo

import java.util.concurrent.CompletionException

import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.jdk.CollectionConverters._

class DynamoDocumentStoreInterpreter(client: DynamoDbAsyncClient, tableName: String, primaryKey: String)
    extends Interpreter
    with DocumentStoreLanguage[String] { self =>

  import DynamoDocumentStoreInterpreter._

  override type Document = DynamoDocument[String]
  override type NewDocument = Document

  override def close() =
    client.close()

  override def apply[A](op: Op[A]): Program[A] = {
    op match {

      case CreateEmptyDocument(id) =>
        unit(DynamoDocument(id, Map(), 0, java.util.UUID.randomUUID.toString, Map()))

      case SetField(d, name, valueOpt) => {
        val document = d.asInstanceOf[Document] // 2.12 compatibility
        val prog = valueOpt match {
          case Some(value) =>
            unit(
              new DynamoDocument(document.id,
                                 document.values + ((name, value)),
                                 document.version,
                                 document.continuum,
                                 document.original)
            )
          case None =>
            unit(
              DynamoDocument(document.id,
                             document.values - name,
                             document.version,
                             document.continuum,
                             document.original)
            )
        }
        prog.asInstanceOf[Program[A]]
      }

      case GetId(document) => unit(document.id)

      case GetData(document) => unit(document.values)

      case ReadDocuments(ids) if ids.size == 0 => unit(Map())

      case ReadDocuments(ids) => {
        val pks = ids.foldLeft(List[Map[String, AttributeValue]]()) { (acc, a) =>
          Map(primaryKey -> AttributeValue.builder().s(a).build()) :: acc
        }
        val ka = KeysAndAttributes.builder().keys(pks.map { _.asJava }.asJava).build()
        val req = BatchGetItemRequest.builder().requestItems(Map(tableName -> ka).asJava).build()

        effect[Map[String, Document]] { exec =>
          client
            .batchGetItem(req)
            .whenCompleteAsync((res, err) =>
              if (res != null) {
                try {
                  val response = res.responses.asScala.get(tableName).get.asScala.toList.map(_.asScala)
                  val responseMap = response.foldLeft(Map[String, DynamoDocument[String]]()) { (result, dmap) =>
                    val docId = dmap.get(primaryKey).get.s()
                    val split = dmap.get(DocumentVersionKey).get.s().split("/")
                    val docVersion = split(0)
                    val docContinuum = split(1)
                    val values =
                      (Map() ++ dmap).map(x => (x._1, x._2.s())).toMap - primaryKey - DocumentVersionKey
                    result + ((docId, DynamoDocument(docId, values, docVersion.toLong, docContinuum, Map())))
                  }
                  exec.proceed(responseMap)
                } catch {
                  case t: Throwable => exec.fail(t)
                }
              } else {
                exec.fail(err)
              }
            )
          None
        }
      }

      case PersistDocument(document) => {
        {
          val initial = Map[String, AttributeValue](
            primaryKey -> AttributeValue.builder().s(document.id).build(),
            DocumentVersionKey -> AttributeValue.builder().s("1/" + document.continuum).build()
          )

          val data = document.values.foldLeft(initial) { (data, keyval) =>
            val (key, value) = keyval
            data + ((key, AttributeValue.builder().s(value).build()))
          }

          val req = PutItemRequest
            .builder()
            .item(data.asJava)
            .conditionExpression("attribute_not_exists(" + primaryKey + ")")
            .tableName(tableName)
            .build()

          effect[Either[DocumentAlreadyExists, Document]] { exec =>
            client
              .putItem(req)
              .whenCompleteAsync { (res, err) =>
                if (res != null) {
                  exec.proceed(
                    Right(DynamoDocument(document.id, document.values, 1, document.continuum, document.values))
                  )
                } else {
                  err match {
                    case ce: CompletionException =>
                      ce.getCause() match {
                        case _: ConditionalCheckFailedException => exec.proceed(Left(DocumentAlreadyExists()))
                        case t                                  => exec.fail(t)
                      }
                    case t => exec.fail(t)
                  }
                }
              }
            None
          }
        }
      }

      case UpdateDocument(document) if document.values == document.original => {

        val req =
          UpdateItemRequest
            .builder()
            .key(Map(primaryKey -> AttributeValue.builder().s(document.id).build()).asJava)
            .expressionAttributeValues(
              Map(
                ":VERSION" -> AttributeValue.builder().s(document.version.toString + "/" + document.continuum).build()
              ).asJava
            )
            .updateExpression("SET " + DocumentVersionKey + " = :VERSION")
            .conditionExpression(DocumentVersionKey + " = :VERSION")
            .tableName(tableName)
            .build()

        effect[Either[StaleDocument, Document]] { exec =>

          client
            .updateItem(req)
            .whenCompleteAsync { (res, err) =>
              if (res != null) {
                exec.proceed(
                  Right(
                    DynamoDocument(document.id, document.values, document.version, document.continuum, document.values)
                  )
                )
              } else {
                err match {
                  case ce: CompletionException =>
                    ce.getCause() match {
                      case _: ConditionalCheckFailedException => exec.proceed(Left(StaleDocument()))
                      case t                                  => exec.fail(t)
                    }
                  case t => exec.fail(t)
                }
              }
            }
          None
        }
      }

      case UpdateDocument(document) => {

        val requiredChanges = mapDiff(document.original, document.values)

        val update = createDynamoExpression(
          requiredChanges + ((DocumentVersionKey, Some((document.version + 1).toString + "/" + document.continuum)))
        )

        val req =
          UpdateItemRequest
            .builder()
            .key(Map(primaryKey -> AttributeValue.builder().s(document.id).build()).asJava)
            .updateExpression(update.expression)
            .expressionAttributeNames(update.names.asJava)
            .expressionAttributeValues(
              (update.values + ((":VERSION" -> AttributeValue
                .builder()
                .s(
                  document.version.toString + "/" + document.continuum
                )
                .build()))).asJava
            )
            .conditionExpression(DocumentVersionKey + " = :VERSION")
            .tableName(tableName)
            .build()

        effect[Either[StaleDocument, Document]] { exec =>
          client
            .updateItem(req)
            .whenCompleteAsync { (res, err) =>
              if (res != null) {
                exec.proceed(
                  Right(
                    DynamoDocument(document.id,
                                   document.values,
                                   document.version + 1,
                                   document.continuum,
                                   document.values)
                  )
                )
              } else {
                err match {
                  case ce: CompletionException =>
                    ce.getCause() match {
                      case _: ConditionalCheckFailedException => exec.proceed(Left(StaleDocument()))
                      case t                                  => exec.fail(t)
                    }
                  case t => exec.fail(t)
                }
              }
            }
          None
        }
      }

      case DeleteDocument(document) => {
        val id = document.id

        val req = DeleteItemRequest
          .builder()
          .key(Map(primaryKey -> AttributeValue.builder().s(id).build).asJava)
          .expressionAttributeValues(
            Map(":VERSION" -> AttributeValue.builder.s(document.version.toString + "/" + document.continuum).build).asJava
          )
          .conditionExpression(DocumentVersionKey + " = :VERSION")
          .tableName(tableName)
          .build()

        effect[Option[StaleDocument]] { exec =>
          client
            .deleteItem(req)
            .whenCompleteAsync((res, err) =>
              if (res != null) {
                exec.proceed(None)
              } else {
                err match {
                  case ce: CompletionException =>
                    ce.getCause() match {
                      case _: ConditionalCheckFailedException => exec.proceed(Some(StaleDocument()))
                      case t                                  => exec.fail(t)
                    }
                  case t => exec.fail(t)
                }
              }
            )
          None
        }
      }
    }
  }
}

object DynamoDocumentStoreInterpreter {

  val DocumentVersionKey = "SanoitusDocumentVersion"

  case class DynamoExpression(expression: String, names: Map[String, String], values: Map[String, AttributeValue])

  def createDynamoExpression(input: Map[String, Option[String]], separator: String = ", "): DynamoExpression = {
    val sets = input
      .filter(_._2.isDefined)
      .map(x => (x._1, x._2.get))
      .foldLeft((List[String](), Map[String, String](), Map[String, AttributeValue](), 1)) { (acc, a) =>
        val (expressionParts, nameMap, valueMap, currentId) = acc
        ("#" + currentId + " = :" + currentId :: expressionParts,
         nameMap + (("#" + currentId, a._1)),
         valueMap + ((":" + currentId, AttributeValue.builder().s(a._2).build())),
         currentId + 1)
      }

    val setPart = "SET " + sets._1.reverse.mkString(separator)
    val tmpNameMap = sets._2
    val valueMap = sets._3
    val id = sets._4

    val removes = input.filter(_._2.isEmpty).keys.foldLeft((List[String](), tmpNameMap, id)) { (acc, a) =>

      val (expressionParts, nameMap, currentId) = acc
      ("#" + currentId :: expressionParts, nameMap + (("#" + currentId, a)), currentId + 1)
    }

    val removePart = if (removes._1.isEmpty) "" else " REMOVE " + removes._1.reverse.mkString(separator)
    val nameMap = removes._2

    DynamoExpression(setPart + removePart, nameMap, valueMap)
  }
}
