package sanoitus
package test
package documentstore

import org.scalacheck.Arbitrary._
import org.scalacheck.Gen

import sanoitus.documentstore._
import sanoitus.parallel.core.ParallelInterpreter
import sanoitus.util._

/**
 * Rules for single step analysis:
 *
 * Create
 * - only one successful create possible (as delete can succeed only for already existing documents)
 * Read
 * - max 3 different values read (missing value is a value in this context) (read, delete, read, create, read)
 * Update
 * - fails for all stale versions
 * - if there's a successful delete, fails for all up-to-date versions
 * - if there's no successful deletes, succeeds for exactly one up-to-date version with real updates
 * Delete
 * - fails for all stale versions
 * - if there's a successful update, fails for all up-to-date versions
 * - if there's no successful updates, succeeds for exactly one up-to-date version (of any kind).
 *
 * ==> number of of updates + number of deletes <= 1
 *
 * Rules for cumulative analysis:
 *
 * C = number of successful creations
 * R = number of different values read (missing value is a value)
 * U = number of successful updates to different values
 * D = number of successful deleted
 *
 * D <= C <= D + 1
 * 0 <= R <= C + U + 1
 */
trait DocumentStoreConcurrencySpecification[ValueType] extends LanguageSpecification {

  override def name = "Concurrency specification"

  type L = DocumentStoreLanguage[ValueType]

  override type State = Data

  val valueGen: Gen[ValueType]

  val parallel = ParallelInterpreter
  import parallel._

  val language: L
  import language._

  case class Doc(representation: Document, values: Map[String, ValueType], root: Map[String, ValueType])

  case class Data(key: String,
                  unchanged: Set[Doc],
                  changed: Set[Doc],
                  readValues: Set[Option[Map[String, ValueType]]],
                  c: Int,
                  u: Int,
                  d: Int) {

    val all = unchanged ++ changed

    def remove(doc: Doc) = Data(key, unchanged - doc, changed - doc, readValues, c, u, d)

    def modChanged(f: Set[Doc] => Set[Doc]) = Data(key, unchanged, f(changed), readValues, c, u, d)

    def modUnchanged(f: Set[Doc] => Set[Doc]) = Data(key, f(unchanged), changed, readValues, c, u, d)

    def modReadValues(f: Set[Option[Map[String, ValueType]]] => Set[Option[Map[String, ValueType]]]) =
      Data(key, unchanged, changed, f(readValues), c, u, d)

    def incc = Data(key, unchanged, changed, readValues, c + 1, u, d)
    def incu = Data(key, unchanged, changed, readValues, c, u + 1, d)
    def incd = Data(key, unchanged, changed, readValues, c, u, d + 1)
  }

  case class SinglePassSummary(read: Set[Option[Map[String, ValueType]]],
                               created: List[Map[String, ValueType]],
                               numUpdates: Int,
                               numDeletes: Int) {

    def modRead(f: Set[Option[Map[String, ValueType]]] => Set[Option[Map[String, ValueType]]]) =
      SinglePassSummary(f(read), created, numUpdates, numDeletes)

    def modCreated(f: List[Map[String, ValueType]] => List[Map[String, ValueType]]) =
      SinglePassSummary(read, f(created), numUpdates, numDeletes)

    def modDeleted(f: Int => Int) = SinglePassSummary(read, created, numUpdates, f(numDeletes))

    def modUpdated(f: Int => Int) = SinglePassSummary(read, created, f(numUpdates), numDeletes)

  }

  override val genInitialState: Gen[Program[State]] =
    for {
      key <- uuid
    } yield unit(Data(key, Set(), Set(), Set(), 0, 0, 0))

  override val programGenerators =
    List(ParallelProgramGenerator)

  sealed trait ProgramDescription[D <: ProgramDescription[D]] { self =>
    type Arguments
    type Result

    val arguments: Arguments
    val result: Option[Result]
    val prog: Program[Result]

    val apply: (Arguments, Option[Result]) => D

    final lazy val program: Program[D] = prog.map { res => apply(arguments, Some(res)) }
  }

  case class ReadDocumentDescription(override val arguments: String,
                                     override val result: Option[Option[Document]] = None)
      extends ProgramDescription[ReadDocumentDescription] {
    override type Arguments = String
    override type Result = Option[Document]
    override val prog = ReadDocument(arguments)
    override val apply = ReadDocumentDescription.apply _
  }

  case class DeleteDocumentDescription(override val arguments: Document,
                                       override val result: Option[Option[StaleDocument]] = None)
      extends ProgramDescription[DeleteDocumentDescription] {
    override type Arguments = Document
    override type Result = Option[StaleDocument]
    override val prog = DeleteDocument(arguments)
    override val apply = DeleteDocumentDescription.apply _
  }

  case class CreateDocumentDescription(override val arguments: (String, Map[String, ValueType]),
                                       override val result: Option[Either[DocumentAlreadyExists, Document]] = None)
      extends ProgramDescription[CreateDocumentDescription] {

    override type Arguments = (String, Map[String, ValueType])
    override type Result = Either[DocumentAlreadyExists, Document]
    override val prog = CreateDocument(arguments._1, arguments._2)
    override val apply = CreateDocumentDescription.apply _
  }

  case class SetFieldDescription(override val arguments: (Doc, String, Option[ValueType]),
                                 override val result: Option[Document] = None)
      extends ProgramDescription[SetFieldDescription] {

    override type Arguments = (Doc, String, Option[ValueType])
    override type Result = Document
    override val prog = SetField(arguments._1.representation, arguments._2, arguments._3)
    override val apply = SetFieldDescription.apply _
  }

  case class SetFieldsDescription(override val arguments: (Doc, Map[String, Option[ValueType]]),
                                  override val result: Option[Document] = None)
      extends ProgramDescription[SetFieldsDescription] {

    override type Arguments = (Doc, Map[String, Option[ValueType]])
    override type Result = Document
    override val prog = SetFields(arguments._1.representation, arguments._2)
    override val apply = SetFieldsDescription.apply _
  }

  case class UpdateDocumentDescription(override val arguments: Doc,
                                       override val result: Option[Either[StaleDocument, Document]] = None)
      extends ProgramDescription[UpdateDocumentDescription] {

    override type Arguments = Doc
    override type Result = Either[StaleDocument, Document]
    override val prog = UpdateDocument(arguments.representation)
    override val apply = UpdateDocumentDescription.apply _
  }

  case object ParallelProgramGenerator extends ProgramGenerator {

    override type Arguments = Unit
    type FreeDescription = ProgramDescription[D] forSome { type D <: ProgramDescription[D] }
    override type Result = List[FreeDescription]

    def precondition(state: State): Boolean = true

    def read(state: State): Gen[FreeDescription] =
      ReadDocumentDescription(state.key)

    def delete(state: State): Gen[FreeDescription] =
      for {
        doc <- Gen.oneOf(state.all.toList)
      } yield DeleteDocumentDescription(doc.representation)

    def create(state: State): Gen[FreeDescription] =
      for {
        size <- Gen.choose(1, 5)
        values <- Gen.listOfN(size, pair(uuid, valueGen)).map { _.toMap }
      } yield CreateDocumentDescription((state.key, values))

    def localModify(state: State): Gen[FreeDescription] =
      for {
        doc <- Gen.oneOf(state.all.toList)
        backToRoot <- arbitrary[Boolean]
        field <- uuid
        value <- valueGen
      } yield {
        if (backToRoot)
          SetFieldsDescription((doc, mapDiff(doc.values, doc.root))): FreeDescription
        else
          SetFieldDescription((doc, field, Some(value))): FreeDescription
      }

    def update(state: State): Gen[FreeDescription] =
      for {
        doc <- Gen.oneOf(state.all.toList)
      } yield UpdateDocumentDescription(doc)

    def genOperation(state: State): Gen[FreeDescription] =
      if (state.all.size == 0)
        Gen.oneOf(read(state), create(state))
      else
        Gen.oneOf(read(state), create(state), delete(state), localModify(state), update(state))

    override def apply(state: State): Gen[Program[(Arguments, Result)]] = Gen.sized { _ =>
      for {
        // always 8 concurrent operations
        ops <- Gen.listOfN(8, genOperation(state))
        frees = ops.map(_.program): List[Program[FreeDescription]]
      } yield for {
        x <- Parallelize(frees).map(x => ((), x))
        y <- Await(x._2)
      } yield ((), y)
    }

    override def analyze(state: State, ops: Arguments, r: Result): Program[Either[String, State]] = {

      val stepAnalysis: (SinglePassSummary, FreeDescription) => Program[SinglePassSummary] =
        (summary, res) => {
          res match {
            case ReadDocumentDescription(_, Some(Some(doc))) =>
              for {
                values <- GetData(doc)
              } yield summary.modRead(_ + Some(values))
            case ReadDocumentDescription(_, Some(None))                 => unit(summary.modRead(_ + None))
            case CreateDocumentDescription((_, values), Some(Right(_))) => unit(summary.modCreated(values :: _))
            case DeleteDocumentDescription(_, Some(None))               => unit(summary.modDeleted(_ + 1))
            case UpdateDocumentDescription(doc, Some(Right(_))) => {
              if (doc.root == doc.values)
                unit(summary)
              else
                unit(summary.modUpdated(_ + 1))
            }
            case _ => unit(summary)
          }
        }

      val stepProp = foldLeft(r)(SinglePassSummary(Set(), List(), 0, 0))(stepAnalysis).map { summary =>
        if (summary.created.size > 1)
          Left(summary.created.size.toString + " creations successful")
        else if (summary.read.size > 3)
          Left(summary.read.size.toString + " different reads: " + summary.read)
        else if (summary.numUpdates + summary.numDeletes > 1)
          Left("multiple updates / deletes: " + summary)
        else
          Right(state)
      }

      val cumulativeAnalysis: (State, FreeDescription) => Program[State] = (state, res) =>
        res match {
          case ReadDocumentDescription(_, Some(Some(doc))) =>
            for {
              values <- GetData(doc)
            } yield state.modUnchanged(_ + Doc(doc, values, values)).modReadValues(_ + Some(values))

          case ReadDocumentDescription(_, None) => unit(state.modReadValues(_ + None))

          case CreateDocumentDescription((_, values), Some(Right(doc))) =>
            unit(state.modUnchanged(_ + Doc(doc, values, values)).incc)

          case SetFieldDescription((old, _, _), Some(doc)) =>
            for {
              values <- GetData(old.representation)
            } yield state.modChanged(_ + Doc(doc, values, old.root))

          case UpdateDocumentDescription(unsaved, Some(Right(doc))) =>
            unit(state.modUnchanged(_ + Doc(doc, unsaved.values, unsaved.values)).incu)

          case DeleteDocumentDescription(_, Some(None)) => unit(state.incd)

          case _ => unit(state)
        }

      for {
        s <- stepProp
        y <- foldLeft(r)(state)(cumulativeAnalysis).map { state =>
          val c = state.c
          val r = state.readValues.size
          val u = state.u
          val d = state.d
          if (d <= c && c <= (d + 1) && 0 <= r && r <= (c + u + 1))
            Right(state)
          else
            Left("cumulative analysis fails: " + (c, r, u, d, state.readValues).toString)
        }
      } yield s.flatMap { _ => y }
    }
  }
}
