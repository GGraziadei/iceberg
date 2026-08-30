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

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.BoundTransform;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.expressions.UnboundTransform;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SerializableFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for rewrite runners that sort rows by a space-filling-curve value computed from a list of
 * columns.
 *
 * <p>Subclasses parameterise the internal value column name, the exact user-facing error messages
 * (kept per-curve for compatibility), and the combine step that turns per-column ordered bytes into
 * a single curve value.
 */
abstract class SparkCurveFileRewriteRunner extends SparkShufflingFileRewriteRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SparkCurveFileRewriteRunner.class);

  private final String curveColumnName;
  private final Schema curveSchema;
  private final SortOrder curveSortOrder;
  private final List<UnboundTerm<?>> curveTerms;

  SparkCurveFileRewriteRunner(
      SparkSession spark,
      Table table,
      List<? extends Term> terms,
      String curveColumnName,
      String noColumnsError,
      String columnConflictError,
      String allIdentityColumnsError) {
    super(spark, table);
    this.curveColumnName = curveColumnName;
    this.curveSchema =
        new Schema(Types.NestedField.required(0, curveColumnName, Types.BinaryType.get()));
    this.curveSortOrder =
        SortOrder.builderFor(curveSchema)
            .sortBy(curveColumnName, SortDirection.ASC, NullOrder.NULLS_LAST)
            .build();
    this.curveTerms =
        validCurveTerms(
            spark, table, terms, noColumnsError, columnConflictError, allIdentityColumnsError);
  }

  @Override
  protected SortOrder sortOrder() {
    return curveSortOrder;
  }

  /**
   * Returns the schema used while sorting: the table's columns plus the internal curve value
   * column.
   */
  @Override
  protected Schema sortSchema() {
    return new Schema(
        new ImmutableList.Builder<Types.NestedField>()
            .addAll(table().schema().columns())
            .addAll(curveSchema.columns())
            .build());
  }

  @Override
  protected Dataset<Row> sortedDF(Dataset<Row> df, Function<Dataset<Row>, Dataset<Row>> sortFunc) {
    Dataset<Row> valueDF = df.withColumn(curveColumnName, curveValue(df));
    Dataset<Row> sortedDF = sortFunc.apply(valueDF);
    return sortedDF.drop(curveColumnName);
  }

  /** Combines the curve input columns of {@code df} into a single binary curve value. */
  protected abstract Column curveValue(Dataset<Row> df);

  protected List<UnboundTerm<?>> curveTerms() {
    return curveTerms;
  }

  /** Package-visible accessor for tests: the source column name of each curve term. */
  @VisibleForTesting
  List<String> curveTermNames() {
    return curveTerms.stream().map(term -> term.ref().name()).collect(Collectors.toList());
  }

  /** Converts the curve input columns to their ordered-bytes representation. */
  protected Column[] orderedColumns(Dataset<Row> df, SparkZOrderUDF byteUDF) {
    return curveTerms.stream().map(term -> orderedColumn(df, byteUDF, term)).toArray(Column[]::new);
  }

  private Column orderedColumn(Dataset<Row> df, SparkZOrderUDF byteUDF, UnboundTerm<?> term) {
    if (term instanceof NamedReference) {
      StructField col = df.schema().apply(term.ref().name());
      return byteUDF.sortedLexicographically(df.col(col.name()), col.dataType());
    }

    UnboundTransform<?, ?> unbound = (UnboundTransform<?, ?>) term;
    BoundTransform<?, ?> bound =
        unbound.bind(table().schema().asStruct(), SparkUtil.caseSensitive(spark()));
    Column source = toIcebergInternal(df.col(bound.ref().name()), bound.ref().type());
    Column transformed = applyTransform(source, bound);
    return byteUDF.sortedLexicographically(transformed, resultDataType(bound.type()));
  }

  /** Rewrites the column so its JVM values match Iceberg's internal representation. */
  private static Column toIcebergInternal(Column column, Type sourceType) {
    switch (sourceType.typeId()) {
      case DATE:
        return functions.unix_date(column); // int days since epoch
      case TIMESTAMP:
        Types.TimestampType ts = (Types.TimestampType) sourceType;
        // long micros; NTZ values are converted inside the UDF instead (LocalDateTime input)
        return ts.shouldAdjustToUTC() ? functions.unix_micros(column) : column;
      default:
        return column;
    }
  }

  @SuppressWarnings("unchecked")
  private static Column applyTransform(Column column, BoundTransform<?, ?> bound) {
    Type sourceType = bound.ref().type();
    SerializableFunction<Object, Object> func =
        (SerializableFunction<Object, Object>) bound.transform().bind(sourceType);
    boolean ntzInput =
        sourceType.typeId() == Type.TypeID.TIMESTAMP
            && !((Types.TimestampType) sourceType).shouldAdjustToUTC();
    UserDefinedFunction udf =
        functions
            .udf(
                (Object value) -> {
                  if (value == null) {
                    return null;
                  }
                  Object input =
                      ntzInput
                          ? DateTimeUtil.microsFromTimestamp((LocalDateTime) value)
                          : (value instanceof byte[] ? ByteBuffer.wrap((byte[]) value) : value);
                  Object out = func.apply(input);
                  if (out instanceof ByteBuffer) {
                    return ByteBuffers.toByteArray((ByteBuffer) out);
                  } else if (out instanceof CharSequence) {
                    return out.toString();
                  }
                  return out;
                },
                resultDataType(bound.type()))
            .withName(bound.transform().toString());
    return udf.apply(column);
  }

  /**
   * Spark type fed to the ordered-bytes step; date/timestamp results stay in internal int/long
   * form.
   */
  private static DataType resultDataType(Type resultType) {
    switch (resultType.typeId()) {
      case DATE:
        return DataTypes.IntegerType; // int days — same ordering as the date it represents
      case TIMESTAMP:
        return DataTypes.LongType;
      default:
        return SparkSchemaUtil.convert(resultType);
    }
  }

  private List<UnboundTerm<?>> validCurveTerms(
      SparkSession spark,
      Table table,
      List<? extends Term> inputTerms,
      String noColumnsError,
      String columnConflictError,
      String allIdentityColumnsError) {

    Preconditions.checkArgument(inputTerms != null && !inputTerms.isEmpty(), noColumnsError);

    Schema schema = table.schema();
    Set<Integer> identityPartitionFieldIds = table.spec().identitySourceIds();
    boolean caseSensitive = SparkUtil.caseSensitive(spark);

    Preconditions.checkArgument(
        caseSensitive
            ? schema.findField(curveColumnName) == null
            : schema.caseInsensitiveFindField(curveColumnName) == null,
        columnConflictError,
        curveColumnName);

    List<UnboundTerm<?>> validTerms = Lists.newArrayList();

    for (Term term : inputTerms) {
      Preconditions.checkArgument(
          term instanceof NamedReference || term instanceof UnboundTransform,
          "Cannot use term %s: only column references and transforms are supported",
          term);

      String colName = ((UnboundTerm<?>) term).ref().name();
      Types.NestedField field =
          caseSensitive ? schema.findField(colName) : schema.caseInsensitiveFindField(colName);
      Preconditions.checkArgument(
          field != null,
          "Cannot find column '%s' in table schema (case sensitive = %s): %s",
          colName,
          caseSensitive,
          schema.asStruct());

      if (identityPartitionFieldIds.contains(field.fieldId())) {
        LOG.warn("Ignoring '{}' as such values are constant within a partition", colName);
      } else {
        validTerms.add((UnboundTerm<?>) term);
      }
    }

    Preconditions.checkArgument(!validTerms.isEmpty(), allIdentityColumnsError);

    return validTerms;
  }
}
