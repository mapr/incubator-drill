/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.index;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;
import org.apache.drill.exec.planner.sql.TypeInferenceUtils;

import java.math.BigDecimal;
import java.util.List;

/**
 * Convert a logicalExpression to RexNode, notice the inputRel could be in an old plan, but newRowType is the newly built rowType
 * that the new RexNode will be applied upon, so when reference fields, use newRowType, when need cluster, plannerSetting, etc, use old inputRel
 */
public class ExprToRex extends AbstractExprVisitor<RexNode, Void, RuntimeException> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExprToRex.class);

  private final RexBuilder builder;
  private final RelDataType newRowType;
  private final RelNode inputRel;

  public ExprToRex(RelNode inputRel, RelDataType newRowType, RexBuilder builder) {
    this.inputRel = inputRel;
    this.newRowType = newRowType;
    this.builder = builder;
  }

  public static RelDataTypeField findField(String fieldName, RelDataType rowType) {
    final String rootPart = SchemaPath.parseFromString(fieldName).getRootSegmentPath();

    for (RelDataTypeField f : rowType.getFieldList()) {
      if (rootPart.equalsIgnoreCase(f.getName())) {
        return f;
      }
    }
    return null;
  }

  private RexNode makeItemOperator(Object[] paths, int index, RelDataType rowType) {
    if (index == 0) { //last one, return ITEM([0]-inputRef, [1] Literal)
      final RelDataTypeField field = findField((String)paths[0], rowType);
      return field == null ? null : builder.makeInputRef(field.getType(), field.getIndex());
    }
    RexLiteral literal;
    if (paths[index] instanceof Integer) {
      int value = (Integer)paths[index];
      literal = builder.makeBigintLiteral(BigDecimal.valueOf(value));
    } else {
      literal = builder.makeLiteral((String)paths[index]);
    }
    return builder.makeCall(SqlStdOperatorTable.ITEM,
                            makeItemOperator(paths, index - 1, rowType),
                            literal);
  }

  @Override
  public RexNode visitSchemaPath(SchemaPath path, Void value) throws RuntimeException {
    PathSegment rootSegment = path.getRootSegment();
    if (rootSegment.isLastPath()) {
      if (rootSegment.isNamed()) {  // named segment
        String segmentPath = ((PathSegment.NameSegment)rootSegment).getPath();
        final RelDataTypeField field = findField(segmentPath, newRowType);
        return field == null ? null : builder.makeInputRef(field.getType(), field.getIndex());
      } else {  // array segment
        // TODO: for array segment such as a[-1],  build a corresponding ITEM($n, -1) expression
        throw new IllegalArgumentException("Unexpected array segment encountered");
      }
    }
    // a path may be a string or an integer (e.g for array indexes)
    List<Object> paths = Lists.newArrayList();

    while (rootSegment != null) {
      if (rootSegment.isNamed()) {
        paths.add(((PathSegment.NameSegment)rootSegment).getPath());
        rootSegment = rootSegment.getChild();
      } else {
        // this is an array segment, so use an index of '-1' which will be
        // used to create the appropriate ITEM expr
        int index = ((PathSegment.ArraySegment)rootSegment).getIndex();
        paths.add(Integer.valueOf(index));
        rootSegment = rootSegment.getChild();
        // throw new IllegalArgumentException("Unexpected array segment encountered");
      }
    }
    return makeItemOperator(paths.toArray(new Object[0]), paths.size() - 1, newRowType);
  }

  @Override
  public RexNode visitCastExpression(CastExpression e, Void value) throws RuntimeException {
    RexNode convertedInput = e.getInput().accept(this, null);
    String typeStr = e.getMajorType().getMinorType().toString();

    if (SqlTypeName.get(typeStr) == null) {
      logger.debug("ExprToRex.visitCastExpression:SqlTypeName could not find {}", typeStr);
    }

    SqlTypeName typeName = TypeInferenceUtils.getCalciteTypeFromDrillType(e.getMajorType().getMinorType());

    RelDataType targetType = TypeInferenceUtils.createCalciteTypeWithNullability(
        inputRel.getCluster().getTypeFactory(), typeName, true);
    return builder.makeCast(targetType, convertedInput);
  }

  @Override
  public RexNode visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    List<RexNode> operands = Lists.newArrayList();
    for (LogicalExpression arg : call.args) {
      operands.add(arg.accept(this, null));
    }
    final DrillSqlOperator operator =
        new DrillSqlOperator(call.getName(), operands.size(), true, false);
    // Function call is not materialized - hence we cannot determine the return type. Create one without.
    return builder.makeCall(operator, operands);
  }

  @Override
  public RexNode visitQuotedStringConstant(QuotedString e, Void value) throws RuntimeException {
    return builder.makeLiteral(e.getString());
  }
}
