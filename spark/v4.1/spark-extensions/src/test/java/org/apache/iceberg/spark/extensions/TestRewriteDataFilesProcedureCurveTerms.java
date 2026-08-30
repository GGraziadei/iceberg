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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.expressions.UnboundTransform;
import org.apache.iceberg.expressions.Zorder;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.ExtendedParser;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRewriteDataFilesProcedureCurveTerms extends ExtensionsTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void parseSortOrderWithNestedTransforms() {
    List<ExtendedParser.RawOrderField> fields =
        ExtendedParser.parseSortOrder(spark, "zorder(truncate(4, c2), truncate(2, c3))");
    assertThat(fields).hasSize(1);
    Zorder zorder = (Zorder) fields.get(0).term();
    assertThat(zorder.terms()).hasSize(2);
    assertThat(zorder.terms().get(0)).isInstanceOf(UnboundTransform.class);
    assertThat(zorder.terms().get(1)).isInstanceOf(UnboundTransform.class);
  }

  @TestTemplate
  public void rewriteWithZorderTransformExpression() {
    createTable();
    insertData(10);

    List<Object[]> expectedRecords = currentData();
    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_data_files(table => '%s', "
                + "strategy => 'sort', sort_order => 'zorder(truncate(2, c2), c1)')",
            catalogName, tableIdent);

    assertThat(Arrays.copyOf((Object[]) output.get(0), 2)).containsExactly(10, 1);
    assertEquals("Data after compaction should not change", expectedRecords, currentData());
  }

  @TestTemplate
  public void nestedCurveTermIsRejected() {
    createTable();
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_data_files(table => '%s', "
                        + "strategy => 'sort', sort_order => 'zorder(zorder(c1, c2))')",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot nest multi-column terms");
  }

  @TestTemplate
  public void addPartitionFieldWithNestedTransformIsRejected() {
    createTable();
    assertThatThrownBy(
            () -> sql("ALTER TABLE %s ADD PARTITION FIELD bucket(16, truncate(4, c2))", tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot convert transform with nested transform arguments");
  }

  private void createTable() {
    sql("CREATE TABLE %s (c1 int, c2 string, c3 string) USING iceberg", tableName);
  }

  private void insertData(int filesCount) {
    ThreeColumnRecord record1 = new ThreeColumnRecord(1, "foo", "aaa");
    ThreeColumnRecord record2 = new ThreeColumnRecord(2, "bar", "bbb");

    List<ThreeColumnRecord> records = Lists.newArrayList();
    IntStream.range(0, filesCount / 2)
        .forEach(
            i -> {
              records.add(record1);
              records.add(record2);
            });

    Dataset<Row> df =
        spark.createDataFrame(records, ThreeColumnRecord.class).repartition(filesCount);
    try {
      df.writeTo(tableName).append();
    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
      throw new RuntimeException(e);
    }
  }

  private List<Object[]> currentData() {
    return rowsToJava(
        spark.sql("SELECT * FROM " + tableName + " order by c1, c2, c3").collectAsList());
  }
}
