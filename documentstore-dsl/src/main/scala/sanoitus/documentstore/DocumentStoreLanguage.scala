package sanoitus
package documentstore

trait DocumentStoreLanguage[ValueType] extends Language { self: Interpreter =>

  sealed trait Op[+A] extends Operation[A]

  type Document
  type NewDocument <: Document

  private[documentstore] case class CreateEmptyDocument(id: String) extends Op[NewDocument]

  private[documentstore] case class PersistDocument(document: NewDocument)
      extends Op[Either[DocumentAlreadyExists, Document]]

  case class SetField[D <: Document](document: D, field: String, value: Option[ValueType]) extends Op[D]

  case class GetId(document: Document) extends Op[String]

  case class GetData(document: Document) extends Op[Map[String, ValueType]]

  case class ReadDocuments(ids: Iterable[String]) extends Op[Map[String, Document]]

  case class UpdateDocument(document: Document) extends Op[Either[StaleDocument, Document]]

  case class DeleteDocument(document: Document) extends Op[Option[StaleDocument]]

  case class DocumentDoesNotExist()
  case class DocumentAlreadyExists()
  case class StaleDocument()

  def ReadDocument(id: String): Program[Option[Document]] =
    ReadDocuments(List(id)).map { _.get(id) }

  def Delete(id: String): Program[Option[DocumentDoesNotExist]] =
    for {
      doc <- ReadDocument(id)
      res <- doc match {
        case Some(doc) =>
          DeleteDocument(doc).flatMap {
            case None                  => unit(None: Option[DocumentDoesNotExist])
            case Some(StaleDocument()) => Delete(id)
          }
        case None => unit(Some(DocumentDoesNotExist()))
      }
    } yield res

  def SetFields[D <: Document](document: D, fields: Map[String, Option[ValueType]]): Program[D] =
    fields.foldLeft(unit(document)) { (prog, field) =>
      for {
        current <- prog
        next <- SetField(current, field._1, field._2)
      } yield next
    }

  def CreateDocument(id: String, values: Map[String, ValueType]): Program[Either[DocumentAlreadyExists, Document]] =
    for {
      initial <- CreateEmptyDocument(id)
      document <- SetFields(initial, values.transform((_, v) => Some(v)))
      persisted <- PersistDocument(document)
    } yield persisted
}
