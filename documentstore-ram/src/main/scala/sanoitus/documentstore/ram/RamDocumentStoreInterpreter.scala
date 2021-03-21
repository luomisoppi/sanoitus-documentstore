package sanoitus
package documentstore
package ram

import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.stm._
import scala.util.Random

class RamDocumentStoreInterpreter[ValueType] extends Interpreter with DocumentStoreLanguage[ValueType] { self =>

  override type Document = RamDocument[ValueType]
  override type NewDocument = Document

  val store: Ref[Map[String, RamDocument[ValueType]]] = Ref(Map())

  private val uidGen = new AtomicLong(0)
  def uid() = uidGen.incrementAndGet().toString

  def clear(): Unit = atomic { implicit tx => store() = Map() }

  override def close() = ()

  override def apply[A](op: Op[A]): Program[A] =
    op match {
      case CreateEmptyDocument(id) => unit(RamDocument(id, Map(), uid()))

      case SetField(d, name, valueOpt) => {
        val document = d.asInstanceOf[Document] // 2.12 compatibility
        val updated = valueOpt match {
          case Some(value) => unit(RamDocument(document.id, document.values + ((name, value)), document.uuid))
          case None        => unit(RamDocument(document.id, document.values - name, document.uuid))
        }
        updated.asInstanceOf[Program[A]]
      }

      case GetId(document) => unit(document.id)

      case GetData(document) => unit(document.values)

      case ReadDocuments(ids) =>
        effect[Map[String, Document]] { _ =>
          val res = Random.shuffle(ids).toList.foldLeft(Map[String, Document]()) { (acc, a) =>
            store.single.get.get(a) match {
              case Some(x) => acc + ((a, x))
              case None    => acc
            }
          }
          Some(res)
        }

      case PersistDocument(document) =>
        effect[Either[DocumentAlreadyExists, Document]] { _ =>
          atomic { implicit tx =>
            if (store().contains(document.id)) {
              Some(Left(DocumentAlreadyExists()))
            } else {
              store.transform(_ + ((document.id, document)))
              Some(Right(document))
            }
          }
        }

      case UpdateDocument(document) =>
        effect[Either[StaleDocument, Document]] { _ =>
          atomic { implicit tx =>
            store().get(document.id) match {
              case None =>
                Some(Left(StaleDocument()))
              case Some(existing) if existing.uuid == document.uuid && existing.values == document.values =>
                Some(Right(existing))
              case Some(existing) if existing.uuid == document.uuid => {
                val doc = RamDocument(document.id, document.values, uid())
                store.transform(_ + ((doc.id, doc)))
                Some(Right(doc))
              }
              case _ =>
                Some(Left(StaleDocument()))
            }
          }
        }

      case DeleteDocument(document) =>
        effect[Option[StaleDocument]] { _ =>
          atomic { implicit tx =>
            store().get(document.id) match {
              case Some(existing) if existing.uuid == document.uuid => {
                store.transform(_ - document.id)
                Some(None)
              }
              case _ => Some(Some(StaleDocument()))
            }
          }
        }

    }
}
