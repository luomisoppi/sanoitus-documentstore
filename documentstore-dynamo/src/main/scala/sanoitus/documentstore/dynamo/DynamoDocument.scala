package sanoitus.documentstore.dynamo

case class DynamoDocument[ValueType](id: String,
                                     values: Map[String, ValueType],
                                     version: Long,
                                     continuum: String,
                                     original: Map[String, ValueType])
