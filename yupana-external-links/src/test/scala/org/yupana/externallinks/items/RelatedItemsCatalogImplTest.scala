package org.yupana.externallinks.items

import org.scalamock.scalatest.MockFactory
import org.yupana.api.Time
import org.yupana.api.query.Query
import org.yupana.core.{ MapReducible, QueryContext, SimpleTsdbConfig, TSDB, TsdbServerResult }
import org.yupana.externallinks.TestSchema
import org.yupana.schema.externallinks.{ ItemsInvertedIndex, RelatedItemsCatalog }
import org.yupana.schema.{ Dimensions, Tables }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RelatedItemsCatalogImplTest extends AnyFlatSpec with Matchers with MockFactory {
  import org.yupana.api.query.syntax.All._

  class MockedTsdb extends TSDB(TestSchema.schema, null, null, null, identity, SimpleTsdbConfig())

  "RelatedItemsCatalogImpl" should "handle phrase field in conditions" in {
    val tsdb = mock[MockedTsdb]
    val catalog = new RelatedItemsCatalogImpl(tsdb, RelatedItemsCatalog)

    val expQuery1 = Query(
      Tables.itemsKkmTable,
      const(Time(100L)),
      const(Time(500L)),
      Seq(dimension(Dimensions.KKM_ID).toField, time.toField),
      in(lower(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD)), Set("хлеб ржаной"))
    )

    val qc1 = QueryContext(expQuery1, None)

    (tsdb.mapReduceEngine _).expects(*).returning(MapReducible.iteratorMR).anyNumberOfTimes()

    (tsdb.query _)
      .expects(expQuery1)
      .returning(
        new TsdbServerResult(
          qc1,
          Seq(
            Array[Any](Time(120), 123456),
            Array[Any](Time(150), 123456),
            Array[Any](Time(120), 345112)
          ).toIterator
        )
      )

    val expQuery2 = Query(
      Tables.itemsKkmTable,
      const(Time(100L)),
      const(Time(500L)),
      Seq(dimension(Dimensions.KKM_ID).toField, time.toField),
      in(lower(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD)), Set("бородинский"))
    )

    val qc2 = QueryContext(expQuery2, None)

    (tsdb.query _)
      .expects(expQuery2)
      .returning(
        new TsdbServerResult(
          qc2,
          Seq(
            Array[Any](Time(125), 123456),
            Array[Any](Time(120), 123456)
          ).toIterator
        )
      )

    val condition = catalog.condition(
      and(
        ge(time, const(Time(100L))),
        lt(time, const(Time(500L))),
        in(lower(link(RelatedItemsCatalog, RelatedItemsCatalog.PHRASE_FIELD)), Set("хлеб ржаной")),
        notIn(lower(link(RelatedItemsCatalog, RelatedItemsCatalog.PHRASE_FIELD)), Set("бородинский"))
      )
    )

    condition shouldEqual and(
      ge(time, const(Time(100L))),
      lt(time, const(Time(500L))),
      in(
        tuple(time, dimension(Dimensions.KKM_ID)),
        Set((Time(120L), 123456), (Time(150L), 123456), (Time(120L), 345112))
      ),
      notIn(
        tuple(time, dimension(Dimensions.KKM_ID)),
        Set((Time(125L), 123456), (Time(120L), 123456))
      )
    )
  }

  it should "handle item field in conditions" in {
    val tsdb = mock[MockedTsdb]
    val catalog = new RelatedItemsCatalogImpl(tsdb, RelatedItemsCatalog)

    val expQuery = Query(
      Tables.itemsKkmTable,
      const(Time(100L)),
      const(Time(500L)),
      Seq(dimension(Dimensions.KKM_ID).toField, time.toField),
      in(lower(dimension(Dimensions.ITEM)), Set("яйцо молодильное 1к"))
    )

    val qc = QueryContext(expQuery, None)

    (tsdb.mapReduceEngine _).expects(*).returning(MapReducible.iteratorMR).anyNumberOfTimes()

    (tsdb.query _)
      .expects(expQuery)
      .returning(
        new TsdbServerResult(
          qc,
          Seq(
            Array[Any](Time(220), 123456),
            Array[Any](Time(330), 654321)
          ).toIterator
        )
      )

    val condition = catalog.condition(
      and(
        ge(time, const(Time(100L))),
        lt(time, const(Time(500L))),
        in(lower(link(RelatedItemsCatalog, RelatedItemsCatalog.ITEM_FIELD)), Set("яйцо молодильное 1к"))
      )
    )

    condition shouldEqual and(
      ge(time, const(Time(100L))),
      lt(time, const(Time(500L))),
      in(
        tuple(time, dimension(Dimensions.KKM_ID)),
        Set((Time(220L), 123456), (Time(330L), 654321))
      )
    )
  }

}
