package sanoitus
package test

import org.scalacheck.Arbitrary._
import org.scalacheck.Gen

import scala.jdk.CollectionConverters._

package object documentstore {

  def uuid: Gen[String] = Gen.uuid.map(_.toString)

  def pair[A, B](genA: Gen[A], genB: Gen[B]): Gen[(A, B)] =
    for {
      a <- genA
      b <- genB
    } yield (a, b)

  val nonEmptyString: Gen[String] =
    for {
      a <- arbitrary[Char]
      b <- arbitrary[String]
    } yield a.toString + b

  def foldLeft[Z, A](as: List[A])(zero: Z)(f: (Z, A) => Program[Z]): Program[Z] =
    as.foldLeft(unit(zero)) { (acc, a) =>
      for {
        z <- acc
        newz <- f(z, a)
      } yield newz
    }

  def genSeq[A](genList: List[Gen[A]]): Gen[List[A]] = Gen.sequence(genList).map {
    _.asScala.toList
  }

}
