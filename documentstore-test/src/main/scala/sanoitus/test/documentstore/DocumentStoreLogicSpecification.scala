package sanoitus
package test
package documentstore

import org.scalacheck.Arbitrary._
import org.scalacheck.Gen

import scala.language.postfixOps
import scala.util.Random

import sanoitus.documentstore._

trait DocumentStoreLogicSpecification[ValueType] extends LanguageSpecification {

  override def name = "Logic specification"

  val valueGen: Gen[ValueType]

  type L = DocumentStoreLanguage[ValueType]

  override type State = Storage

  val language: L
  import language._

  type DocumentId = String

  case class Node(document: Document, content: Map[String, ValueType])

  case class Versions(currentValue: Option[Node], saveable: Set[Node], unsaveable: Set[Node])

  object Storage {
    def apply(_data: Map[DocumentId, Versions]) =
      new Storage {
        override val data = _data
      }
  }

  trait Storage { self =>

    val data: Map[DocumentId, Versions]

    def createDocument(id: DocumentId, document: Document, content: Map[String, ValueType]): State = {
      val rootNode = Node(document, content)
      data.get(id) match {
        case None => Storage(data + ((id, Versions(Some(rootNode), Set(), Set()))))
        case Some(Versions(None, saveable, unsaveable)) =>
          Storage(data + ((id, Versions(Some(rootNode), Set(), saveable ++ unsaveable))))
        case _ => throw new IllegalStateException("creating existing document!")
      }
    }

    def createNewVersion(id: DocumentId,
                         parent: Node,
                         document: Document,
                         modification: Map[String, Option[ValueType]]): State =
      data.get(id) match {
        case None => throw new IllegalArgumentException("nothing found to update")
        case Some(Versions(current, saveable, unsaveable))
            if saveable.contains(parent) && !unsaveable.contains(parent) =>
          Storage(
            data + ((id,
                     Versions(current, saveable + Node(document, modify(parent.content, modification)), unsaveable)))
          )
        case Some(Versions(current, saveable, unsaveable))
            if unsaveable.contains(parent) && !saveable.contains(parent) =>
          Storage(
            data + ((id,
                     Versions(current, saveable, unsaveable + Node(document, modify(parent.content, modification)))))
          )
        case Some(Versions(Some(current), saveable, unsaveable))
            if !unsaveable.contains(parent) && !saveable.contains(parent) && parent == current =>
          Storage(
            data + ((id,
                     Versions(Some(current),
                              saveable + Node(document, modify(parent.content, modification)),
                              unsaveable)))
          )
        case fails @ _ => throw new IllegalStateException(s"test bug: $parent, $fails")
      }

    def existingDocuments: Set[DocumentId] = data.filter { _._2.currentValue.isDefined } keySet
    def deletedDocuments: Set[DocumentId] = data.filter { _._2.currentValue.isEmpty } keySet

    def allNodes(id: DocumentId): Set[Node] = {
      val versions = data.get(id).get
      versions.saveable ++ versions.unsaveable ++ versions.currentValue.map(Set(_)).getOrElse(Set())
    }

    def allNodes: Set[(DocumentId, Node)] =
      data.keys.foldLeft(Set[(DocumentId, Node)]()) { (set, key) => set ++ allNodes(key).map((key, _)) }
  }

  def modify(values: Map[String, ValueType], modification: Map[String, Option[ValueType]]): Map[String, ValueType] =
    modification.foldLeft(values) { (acc, kv) =>
      kv match {
        case (key, None)        => acc - key
        case (key, Some(value)) => acc + ((key, value))
      }
    }

  override val genInitialState: Gen[Program[State]] = {
    Gen.const(unit(new Storage {
      override val data = Map()
    }))
  }

  override val programGenerators =
    List(CreateDocumentGenerator,
         ReadDocumentsGenerator,
         SetFieldsGenerator,
         GetDataGenerator,
         UpdateDocumentGenerator,
         DeleteDocumentGenerator)

  case object CreateDocumentGenerator extends ProgramGenerator {

    case class Arguments(id: String, values: Map[String, ValueType])

    override type Result = Either[DocumentAlreadyExists, Document]

    override def precondition(state: State): Boolean = true

    def genFields(size: Int): Gen[Map[String, ValueType]] =
      for {
        amountValues <- Gen.choose(0, size)
        values <- Gen.listOfN(amountValues, pair(uuid, valueGen))
      } yield values.toMap

    override def apply(state: State): Gen[Program[(Arguments, Result)]] =
      for {
        success <- arbitrary[Boolean].map { _ || state.existingDocuments.size == 0 }
        key <- {
          if (success) {
            if (state.deletedDocuments.size > 0)
              Gen.oneOf(uuid, Gen.oneOf(state.deletedDocuments.toList))
            else
              uuid
          } else {
            Gen.oneOf(state.existingDocuments.toList)
          }
        }
        numFields <- Gen.choose(0, 3)
        values <- genFields(numFields)
      } yield CreateDocument(key, values).map((Arguments(key, values), _))

    override def analyze(state: State, arguments: Arguments, result: Result) = {
      val (id, values) = (arguments.id, arguments.values)
      val res = (state.data.get(id), result) match {
        case (None, Right(doc)) => Right(state.createDocument(id, doc, values))
        case (None, Left(err)) =>
          Left(s"CreateDocument($id, $values) => $err - but store has no association with key $id")

        case (Some(Versions(Some(_), _, _)), Left(DocumentAlreadyExists())) => Right(state)
        case (Some(Versions(Some(_), _, _)), Right(_)) =>
          Left(s"CreateDocument($id, $values) successful - but store already has document $id)")

        case (Some(Versions(None, _, _)), Right(doc)) => Right(state.createDocument(id, doc, values))
        case (Some(Versions(None, _, _)), Left(err)) =>
          Left(s"CreateDocument($id, $values) => $err - should have worked, as document $id was deleted earlier")
      }
      unit(res)
    }
  }

  case object ReadDocumentsGenerator extends ProgramGenerator {

    override type Arguments = Iterable[String]

    override type Result = Map[String, Map[String, ValueType]]

    override def precondition(state: State): Boolean = true

    override def apply(state: State): Gen[Program[(Arguments, Result)]] =
      for {
        numExisting <- Gen.choose(0, 10.min(state.existingDocuments.size))
        numDeleted <- Gen.choose(0, (10 - numExisting).min(state.deletedDocuments.size))
        numNonExisting <- Gen.choose(0, 10)
        existing <- Gen.pick(numExisting, state.existingDocuments.toList)
        deleted <- Gen.pick(numDeleted, state.deletedDocuments.toList)
        nonExisting <- Gen.listOfN(numNonExisting, uuid)
        all = existing.toList ::: deleted.toList ::: nonExisting
        shuffled = Random.shuffle(all)
      } yield for {
        values <- ReadDocuments(shuffled)
        converted <- values.foldLeft(unit(Map[String, Map[String, ValueType]]())) { (acc, a) =>
          for {
            current <- acc
            data <- GetData(a._2)
          } yield current + ((a._1, data))
        }
      } yield (shuffled, converted)

    override def analyze(state: State, ids: Iterable[String], result: Map[String, Map[String, ValueType]]) = {
      val res = ids.foldLeft(Right(state): Either[String, State]) { (acc, id) =>
        for {
          _ <- acc
          result <- (state.data.get(id).flatMap { _.currentValue.map { _.content } }, result.get(id)) match {
            case (None, None)                                         => acc
            case (Some(expected), Some(actual)) if expected == actual => acc
            case (Some(expected), Some(actual))                       => Left(s"Value mismatch for $id, expected $expected, got $actual")
            case (Some(_), None)                                      => Left(s"Read did not return document with key $id, it should be there")
            case (None, Some(actual))                                 => Left(s"Got value for non-existing key $id: $actual")
          }
        } yield result
      }
      unit(res)
    }
  }

  case object SetFieldsGenerator extends ProgramGenerator {

    case class Arguments(id: DocumentId, node: Node, modification: Map[String, Option[ValueType]])

    override type Result = Document

    override def precondition(state: State): Boolean = state.data.size > 0

    def modGen(current: Map[String, ValueType]): Gen[Map[String, Option[ValueType]]] =
      for {
        n <- Gen.choose(0, 3)
        amountChanges <- Gen.choose(0, n.min(current.size))
        amountDeletes <- Gen.choose(0, (n - amountChanges).min(current.size - amountChanges))
        amountAdds = n - amountChanges - amountDeletes
        changedKeys <- Gen.pick(amountChanges, current.keys.toList)
        deletedKeys <- Gen.pick(amountDeletes, (current.keySet -- changedKeys.toSet).toList)

        changes <- genSeq(changedKeys.toList.map { key =>
          valueGen.map { value => (key, Some(value): Option[ValueType]) }
        })
        deletes = deletedKeys.map((_, None: Option[ValueType])).toList
        adds <- Gen.listOfN(amountAdds, pair(uuid, valueGen.map { Some(_) }))
        all = changes ++ deletes ++ adds
      } yield all.toMap

    override def apply(state: State): Gen[Program[(Arguments, Result)]] =
      for {
        docId <- Gen.oneOf(state.data.keys.toList)
        doc = state.data.get(docId).get
        node <- Gen.oneOf(state.allNodes(docId).toList)
        modifyBackToOriginal <- Gen.choose(0, 10).map(_ == 7 && doc.currentValue.map(_ != node).getOrElse(false))
        modification <- if (modifyBackToOriginal) Gen.const(mapDiff(node.content, doc.currentValue.get.content))
        else modGen(node.content)
      } yield SetFields(node.document, modification).map((Arguments(docId, node, modification), _))

    override def analyze(state: State, arguments: Arguments, result: Document) = {
      val (id, node, modification) = (arguments.id, arguments.node, arguments.modification)
      unit(Right(state.createNewVersion(id, node, result, modification)))
    }
  }

  case object GetDataGenerator extends ProgramGenerator {

    override type Arguments = Node
    override type Result = Map[String, ValueType]

    override def precondition(state: State): Boolean = state.allNodes.size > 0

    override def apply(state: State): Gen[Program[(Arguments, Result)]] =
      for {
        node <- Gen.oneOf(state.allNodes.toList)
      } yield GetData(node._2.document).map((node._2, _))

    override def analyze(state: State, node: Node, result: Map[String, ValueType]) =
      if (node.content == result)
        unit(Right(state))
      else
        unit(Left(node.content.toString + " vs " + result))
  }

  case object UpdateDocumentGenerator extends ProgramGenerator {

    case class Arguments(id: DocumentId, node: Node)
    override type Result = Either[StaleDocument, Document]

    override def precondition(state: State): Boolean = state.allNodes.size > 0

    override def apply(state: State): Gen[Program[(Arguments, Result)]] =
      for {
        node <- Gen.oneOf(state.allNodes.toList)
      } yield UpdateDocument(node._2.document).map((Arguments(node._1, node._2), _))

    override def analyze(state: State, arguments: Arguments, result: Either[StaleDocument, Document]) = {
      val (id, node) = (arguments.id, arguments.node)
      val res = (state.data.get(id).get, result) match {
        case (Versions(Some(_), _, unsaveable), Left(StaleDocument())) if unsaveable.contains(node) => Right(state)
        case (Versions(Some(_), _, unsaveable), Right(_)) if unsaveable.contains(node) =>
          Left("update succeeded on stale value")
        case (Versions(Some(current), saveable, _), Left(StaleDocument()))
            if saveable.contains(node) || current == node =>
          Left("update failed on up-to-date document")
        case (Versions(Some(current), _, _), Right(_)) if current == node => Right(state)
        case (Versions(Some(current), saveable, unsaveable), Right(doc)) if saveable.contains(node) =>
          Right(
            Storage(
              state.data + ((id, Versions(Some(Node(doc, node.content)), Set(), saveable ++ unsaveable + current)))
            )
          )
        case (Versions(Some(_), _, _), _) => Left("This should not happen")

        case (Versions(None, _, _), Right(_))              => Left("update succeeded on deleted document")
        case (Versions(None, _, _), Left(StaleDocument())) => Right(state)
      }
      unit(res)
    }
  }

  case object DeleteDocumentGenerator extends ProgramGenerator {

    case class Arguments(id: DocumentId, node: Node)

    override type Result = Option[StaleDocument]

    override def precondition(state: State): Boolean = state.allNodes.size > 0

    override def apply(state: State): Gen[Program[(Arguments, Result)]] =
      for {
        node <- Gen.oneOf(state.allNodes.toList)
      } yield DeleteDocument(node._2.document).map((Arguments(node._1, node._2), _))

    override def analyze(state: State, arguments: Arguments, result: Option[StaleDocument]) = {
      val (id, node) = (arguments.id, arguments.node)
      val res = (state.data.get(id).get, result) match {
        case (Versions(Some(_), _, unsaveable), Some(StaleDocument())) if unsaveable.contains(node) => Right(state)
        case (Versions(Some(_), _, unsaveable), None) if unsaveable.contains(node) =>
          Left("delete succeeded on stale value")
        case (Versions(Some(current), saveable, _), Some(StaleDocument()))
            if current == node || saveable.contains(node) =>
          Left("delete failed on up-to-date document")
        case (Versions(Some(current), saveable, unsaveable), None) if current == node || saveable.contains(node) =>
          Right(Storage(state.data + ((id, Versions(None, Set(), saveable ++ unsaveable + current)))))
        case (Versions(Some(_), _, _), _) => Left("This should not happen")

        case (Versions(None, _, _), Some(StaleDocument())) => Right(state)
        case (Versions(None, _, _), None)                  => Left("delete succeeded on already deleted document")
      }
      unit(res)
    }
  }
}
