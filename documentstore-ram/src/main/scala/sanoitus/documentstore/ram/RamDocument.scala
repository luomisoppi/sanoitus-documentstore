package sanoitus.documentstore.ram

case class RamDocument[ValueType](id: String, values: Map[String, ValueType], uuid: String)
