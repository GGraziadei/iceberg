/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles.Result;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Zorder;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TestSparkCurveTermRunners extends TestBase {

  private static final TableIdentifier TABLE_IDENT = TableIdentifier.of("default", "tbl");
  private static final Schema SCHEMA =
      new Schema(
          NestedField.required(1, "id", IntegerType.get()),
          NestedField.required(2, "s", StringType.get()),
          NestedField.required(3, "ts", TimestampType.withoutZone()));
  private static final PartitionSpec PARTITION_SPEC_IDENTITY_S =
      PartitionSpec.builderFor(SCHEMA).identity("s").build();

  @AfterEach
  public void removeTable() {
    catalog.dropTable(TABLE_IDENT);
  }

  @Test
  public void zOrderAcceptsTransformTerms() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkZOrderFileRewriteRunner runner =
        SparkZOrderFileRewriteRunner.withTerms(
            spark, table, Expressions.truncate("s", 2), Expressions.day("ts"));
    assertThat(runner.description()).isEqualTo("Z-ORDER");
  }

  @Test
  public void rejectsNonRefNonTransformTerms() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    assertThatThrownBy(
            () ->
                SparkZOrderFileRewriteRunner.withTerms(
                    spark, table, new Zorder(ImmutableList.of(Expressions.ref("s")))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot use term");
  }

  @Test
  public void stringPathStillWorks() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    SparkActions.get(spark).rewriteDataFiles(table).zOrder("s");
  }

  @Test
  public void transformOfIdentityPartitionColumnIsSkippedWithRemainingColumns() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA, PARTITION_SPEC_IDENTITY_S);
    SparkHilbertFileRewriteRunner runner =
        SparkHilbertFileRewriteRunner.withTerms(
            spark, table, Expressions.truncate("s", 2), Expressions.ref("id"));
    // s is identity-partitioned: its transform term is dropped, id remains
    assertThat(runner.curveTermNames()).containsExactly("id");
  }

  @Test
  public void zOrderWithTransformTermsRewritesData() {
    Table table = createTableWithData();
    List<Object[]> before = rowsSorted(table);
    Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .zOrder(Expressions.truncate("s", 2), Expressions.day("ts"))
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .execute();
    assertThat(result.rewrittenDataFilesCount()).isGreaterThan(0);
    assertEquals("We shouldn't have changed the data", before, rowsSorted(table));
  }

  /**
   * The E2E rewrite tests above only prove that {@code day(ts)} yields *some* value of the right
   * type without throwing; a wrong conversion (e.g. a plain cast instead of {@code unix_date}, or
   * skipping the NTZ-to-micros step) would still produce an Integer and still leave the rows
   * unchanged. This probes the exact bytes {@code orderedColumn} produces for a known, pre-epoch
   * NTZ timestamp and compares them against an independently computed oracle (java.time, not {@code
   * DateTimeUtil}) for the epoch-day the {@code day} transform must produce.
   */
  @Test
  public void dayTransformOnNtzTimestampMatchesIndependentOracle() {
    TableIdentifier probeIdent = TableIdentifier.of("default", "probe");
    Schema probeSchema = new Schema(NestedField.required(1, "ts", TimestampType.withoutZone()));
    Table table = catalog.createTable(probeIdent, probeSchema);
    try {
      SparkZOrderFileRewriteRunner runner =
          SparkZOrderFileRewriteRunner.withTerms(spark, table, Expressions.day("ts"));
      runner.init(ImmutableMap.of());

      // 1969-12-31 is one day before the epoch: a wrong/omitted conversion (e.g. treating the
      // NTZ value as already-epoch micros, or casting instead of computing unix_date) would not
      // reproduce epoch-day -1.
      Dataset<Row> df =
          spark.range(1).selectExpr("CAST('1969-12-31 00:00:00' AS TIMESTAMP_NTZ) AS ts");

      SparkZOrderUDF byteUDF =
          new SparkZOrderUDF(1, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE, Integer.MAX_VALUE);
      Column[] ordered = runner.orderedColumns(df, byteUDF);
      byte[] actual = (byte[]) df.select(ordered[0].as("v")).collectAsList().get(0).get(0);

      int expectedEpochDay = (int) LocalDate.of(1969, 12, 31).toEpochDay();
      assertThat(expectedEpochDay).isEqualTo(-1);
      byte[] expected =
          ZOrderByteUtils.intToOrderedBytes(
                  expectedEpochDay, ByteBuffer.allocate(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE))
              .array();

      assertThat(actual).isEqualTo(expected);
    } finally {
      catalog.dropTable(probeIdent);
    }
  }

  /**
   * {@code truncate("n.s", 2)} must resolve the NESTED field {@code n.s}, not a top-level column
   * that happens to share the leaf name {@code s}. A lookup keyed on {@link
   * org.apache.iceberg.expressions.BoundReference#field()}{@code .name()} (the leaf name) would
   * silently resolve the decoy top-level column instead of the nested one; {@link
   * org.apache.iceberg.expressions.BoundReference#name()} (the full dotted path) is required. This
   * probes the exact ordered bytes {@code orderedColumn} produces, using the same idiom as {@link
   * #dayTransformOnNtzTimestampMatchesIndependentOracle()}.
   */
  @Test
  public void truncateOnNestedColumnResolvesNestedFieldNotTopLevelDecoy() {
    TableIdentifier probeIdent = TableIdentifier.of("default", "nestedProbe");
    Schema probeSchema =
        new Schema(
            NestedField.required(1, "s", StringType.get()),
            NestedField.required(
                2, "n", Types.StructType.of(NestedField.required(3, "s", StringType.get()))));
    Table table = catalog.createTable(probeIdent, probeSchema);
    try {
      SparkZOrderFileRewriteRunner runner =
          SparkZOrderFileRewriteRunner.withTerms(spark, table, Expressions.truncate("n.s", 2));
      runner.init(ImmutableMap.of());

      // Decoy top-level "s" and the actual nested "n.s" hold DIFFERENT values.
      Dataset<Row> df = spark.range(1).selectExpr("'top' AS s", "named_struct('s', 'nested') AS n");

      SparkZOrderUDF byteUDF =
          new SparkZOrderUDF(1, ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE, Integer.MAX_VALUE);
      Column[] ordered = runner.orderedColumns(df, byteUDF);
      byte[] actual = (byte[]) df.select(ordered[0].as("v")).collectAsList().get(0).get(0);

      // truncate(2) of the nested value "nested" is "ne"; truncate(2) of the decoy "top" is "to".
      byte[] expectedFromNested =
          ZOrderByteUtils.stringToOrderedBytes(
                  "ne",
                  ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE,
                  ByteBuffer.allocate(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE),
                  StandardCharsets.UTF_8.newEncoder())
              .array();
      byte[] expectedFromTopLevelDecoy =
          ZOrderByteUtils.stringToOrderedBytes(
                  "to",
                  ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE,
                  ByteBuffer.allocate(ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE),
                  StandardCharsets.UTF_8.newEncoder())
              .array();

      assertThat(actual).isEqualTo(expectedFromNested);
      assertThat(actual).isNotEqualTo(expectedFromTopLevelDecoy);
    } finally {
      catalog.dropTable(probeIdent);
    }
  }

  @Test
  public void hilbertWithBucketTerm() {
    Table table = createTableWithData();
    List<Object[]> before = rowsSorted(table);
    Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .hilbert(Expressions.bucket("id", 8), Expressions.ref("s"))
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .execute();
    assertThat(result.rewrittenDataFilesCount()).isGreaterThan(0);
    assertEquals("We shouldn't have changed the data", before, rowsSorted(table));
  }

  @Test
  public void transformOnUnsupportedSourceFails() {
    Table table = createTableWithData();
    assertThatThrownBy(
            () ->
                SparkActions.get(spark)
                    .rewriteDataFiles(table)
                    .zOrder(Expressions.day("s")) // day() cannot transform string
                    .execute())
        .isInstanceOf(org.apache.iceberg.exceptions.ValidationException.class)
        .hasMessageContaining("cannot transform");
  }

  /** Creates a table with schema {@link #SCHEMA} and several data files. */
  private Table createTableWithData() {
    Table table = catalog.createTable(TABLE_IDENT, SCHEMA);
    for (int batch = 0; batch < 5; batch += 1) {
      writeBatch(batch * 8, 8);
    }
    return table;
  }

  private void writeBatch(int startId, int count) {
    Dataset<Row> df =
        spark
            .range(startId, startId + count)
            .selectExpr(
                "CAST(id AS INT) AS id",
                "concat('row', CAST(id AS STRING)) AS s",
                "CAST(date_add(DATE'2023-01-01', CAST(id % 27 AS INT)) AS TIMESTAMP_NTZ) AS ts")
            .coalesce(1);
    df.write()
        .format("iceberg")
        .mode("append")
        .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
        .save(TABLE_IDENT.toString());
  }

  private List<Object[]> rowsSorted(Table table) {
    return rowsToJava(
        spark.read().format("iceberg").load(TABLE_IDENT.toString()).orderBy("id").collectAsList());
  }
}
