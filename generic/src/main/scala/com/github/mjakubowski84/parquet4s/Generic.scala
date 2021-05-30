package com.github.mjakubowski84.parquet4s


import scala.deriving.Mirror

object ParquetRecordDecoder:

  private[ParquetRecordDecoder] class Fields[Labels <: Tuple, Values <: Tuple](val values: Values)

  given ParquetRecordDecoder[Fields[EmptyTuple, EmptyTuple]] with
    def decode(record: RowParquetRecord): Fields[EmptyTuple, EmptyTuple] = new Fields(EmptyTuple)

  given [L <: String & Singleton: ValueOf,
         LT <: Tuple,
         V : ValueDecoder,
         VT <: Tuple
        ](using tailDecoder: ParquetRecordDecoder[Fields[LT, VT]])
        : ParquetRecordDecoder[Fields[L *: LT, V *: VT]] with
    def decode(record: RowParquetRecord): Fields[L *: LT, V *: VT] =
      val label = summon[ValueOf[L]].value
      val value = record.get(summon[ValueOf[L]].value)
      Fields(summon[ValueDecoder[V]].decode(value) *: tailDecoder.decode(record).values)

  given derived[P <: Product] (using
                               mirror: Mirror.ProductOf[P],
                               decoder: ParquetRecordDecoder[Fields[mirror.MirroredElemLabels, mirror.MirroredElemTypes]]
                             ): ParquetRecordDecoder[P] with
    def decode(record: RowParquetRecord): P =
      mirror.fromProduct(decoder.decode(record).values)

  def decode[P <: Product: ParquetRecordDecoder](record: RowParquetRecord): P =
    summon[ParquetRecordDecoder[P]].decode(record)

end ParquetRecordDecoder


trait ParquetRecordDecoder[T]:
  def decode(record: RowParquetRecord): T

object ParquetRecordEncoder:

  private[ParquetRecordEncoder] class Fields[Labels <: Tuple, Values <: Tuple](val values: Values)

  given ParquetRecordEncoder[Fields[EmptyTuple, EmptyTuple]] with
    def encode(empty: Fields[EmptyTuple, EmptyTuple]): RowParquetRecord = RowParquetRecord.Empty

  given [L <: String & Singleton: ValueOf,
         LT <: Tuple,
         V : ValueEncoder,
         VT <: Tuple
        ](using tailEncoder: ParquetRecordEncoder[Fields[LT, VT]])
        : ParquetRecordEncoder[Fields[L *: LT, V *: VT]] with
    def encode(fields: Fields[L *: LT, V *: VT]): RowParquetRecord = {
      val label = summon[ValueOf[L]].value
      val value = summon[ValueEncoder[V]].encode(fields.values.head)
      val record = tailEncoder.encode(Fields(fields.values.tail))
      record.add(label, value)
    }

  given derived[P <: Product](using
                              mirror: Mirror.ProductOf[P],
                              encoder: ParquetRecordEncoder[Fields[mirror.MirroredElemLabels, mirror.MirroredElemTypes]]
                             ): ParquetRecordEncoder[P] with
    def encode(product: P): RowParquetRecord =
      encoder.encode(Fields(Tuple.fromProductTyped(product)))


  def encode[P <: Product: ParquetRecordEncoder](product: P): RowParquetRecord =
    summon[ParquetRecordEncoder[P]].encode(product)

end ParquetRecordEncoder

trait ParquetRecordEncoder[T]:
  def encode(obj: T): RowParquetRecord


trait Value extends Any

case class IntValue(value: Int) extends AnyVal with Value
case class BooleanValue(value: Boolean) extends AnyVal with Value

object RowParquetRecord {
  val Empty = RowParquetRecord(Map.empty)
}

class RowParquetRecord(fields: Map[String, Value]) extends Value:

  def get(name: String): Value = fields(name)

  def add(name: String, value: Value): RowParquetRecord = RowParquetRecord(fields.updated(name, value))

end RowParquetRecord


trait ValueDecoder[T]:
  def decode(value: Value): T

object ValueDecoder:

  given ValueDecoder[Boolean] = value =>
    value match
      case BooleanValue(b) => b

  given ValueDecoder[Int] = value =>
    value match
      case IntValue(int) => int

  given [P <: Product: ParquetRecordDecoder]: ValueDecoder[P] = value =>
    value match
      case record: RowParquetRecord =>
        summon[ParquetRecordDecoder[P]].decode(record)

end ValueDecoder

trait ValueEncoder[T]:
  def encode(obj: T): Value

object ValueEncoder:

  given ValueEncoder[Boolean] = BooleanValue.apply _

  given ValueEncoder[Int] = IntValue.apply _

  given [P <: Product: ParquetRecordEncoder]: ValueEncoder[P] = summon[ParquetRecordEncoder[P]].encode _

end ValueEncoder

case class Nested(x: Int)
case class MyClass (int: Int, bool: Boolean, nested: Nested)

@main def run: Unit =
  val product = MyClass(666, true, Nested(12))

  val record = ParquetRecordEncoder.encode(product)
  println(record)

  val decoded = ParquetRecordDecoder.decode[MyClass](record)
  println(decoded)

  assert(product == decoded)