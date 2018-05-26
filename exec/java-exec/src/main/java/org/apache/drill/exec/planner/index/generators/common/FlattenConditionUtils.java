/**
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
package org.apache.drill.exec.planner.index.generators.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.drill.exec.planner.index.FlattenIndexPlanCallContext;
import org.apache.drill.exec.planner.logical.DrillProjectRel;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class FlattenConditionUtils {

  /**
   * The composeCondition combines all the flatten and non-flatten conditions into one condition.
   * It also changes the flatten column referenced in the flatten condition to ITEM($<fieldNo>, -1)
   * @param indexContext
   * @param builder
   * @return
   */
  public static RexNode composeCondition(FlattenIndexPlanCallContext indexContext, RexBuilder builder) {
    if (indexContext.getFilterAboveFlatten() != null) {
      FilterVisitor filterVisitor =
              new FilterVisitor(indexContext.getFlattenMap(), indexContext.lowerProject, builder);
      RexNode conditionFilterAboveFlatten = indexContext.getFilterAboveFlatten().getCondition().accept(filterVisitor);

      // keep track of the exprs that were created representing filter exprs
      // referencing flatten
      indexContext.setFilterExprsReferencingFlatten(filterVisitor.getExprsReferencingFlatten());

      if (indexContext.getFilterBelowFlatten() != null) {
        RexNode conditionFilterBelowFlatten = indexContext.getFilterBelowFlatten().getCondition();
        if (indexContext.getLeafProjectAboveScan() != null) {
          FilterVisitor filterVisitor2 =
                  new FilterVisitor(indexContext.getFlattenMap(), indexContext.getLeafProjectAboveScan(), builder);
          conditionFilterBelowFlatten = indexContext.getFilterBelowFlatten().getCondition().accept(filterVisitor2);

          // keep track of the relevant exprs in the filter that are referencing the
          // child Project
          indexContext.setRelevantExprsInLeafProject(filterVisitor2.getOtherExprs());
        }

        // compose a new condition using conjunction (this is valid since the above and below are 2 independent filters)
        RexNode combinedCondition = RexUtil.composeConjunction(builder,
                ImmutableList.of(conditionFilterAboveFlatten, conditionFilterBelowFlatten), false);
        return combinedCondition;
      } else {
        return conditionFilterAboveFlatten;
      }
    } else {
      // return null because filter below flatten is supposed to be handled by a separate
      // index planning rule
      return null;
    }
  }

  /**
   * <p>
   * The FilterVisitor converts an ITEM expression that is referencing the output of a FLATTEN(array) to
   * a corresponding nested ITEM expression with -1 ordinal. The reason for this conversion is that
   * for index planning purposes, we want to keep track of fields that occur within an array.
   * </p>
   * <p>
   * Query:
   * select d from (select flatten(t1.`first`.`second`.`a`) as f from t1) as t
   *    where t.f.b < 10 AND t.f.c > 20;
   * </p>
   * <p>
   * The logical plan has the following:
   *   <li>DrillFilterRel: condition=[AND(<(ITEM($0, 'b'), 10), >(ITEM($0, 'c'), 20))]) </li>
   *   <li>DrillProjectRel: FLATTEN(ITEM(ITEM($1, 'second'), 'a') </li>
   *   <li>DrillScanRel RowType: ['first']  </li>
   * </p>
   * <p>
   * Conversion is as follows:
   *   <li>original expr: ITEM($0, 'b') </li>
   *   <li>new expr: ITEM(ITEM($0, -1), 'b') </li>
   * </p>
   */
  public static class FilterVisitor extends RexVisitorImpl<RexNode> {

    private final Map<String, RexCall> flattenMap;
    private final DrillProjectRel project;
    private final RexBuilder builder;

    // list of expressions in the filter that are referencing a
    // FLATTEN expr from the child Project
    private final List<RexNode> exprsReferencingFlatten = Lists.newArrayList();

    // list of expressions in the filter that are _NOT_ referencing
    // FLATTEN expr from the child Project
    private final List<RexNode> otherExprs = Lists.newArrayList();

    public FilterVisitor(Map<String, RexCall> flattenMap, DrillProjectRel project,
                         RexBuilder builder) {
      super(true);
      this.project = project;
      this.flattenMap = flattenMap;
      this.builder = builder;
    }

    private RexNode makeArrayItem(RexCall item) {
      RexInputRef inputRef = (RexInputRef) item.getOperands().get(0);
      RexLiteral literal = (RexLiteral) item.getOperands().get(1);

      // check if input is referencing a FLATTEN
      String projectFieldName = project.getRowType().getFieldNames().get(inputRef.getIndex());
      RexCall c;
      RexNode left = null;
      if ((c = flattenMap.get(projectFieldName)) != null) {
        left = c.getOperands().get(0);
        Preconditions.checkArgument(left != null, "Found null input reference for Flatten") ;

        // take the Flatten's input and build a new RexExpr with ITEM($n, -1)
        RexLiteral right = builder.makeBigintLiteral(BigDecimal.valueOf(-1));
        RexNode result1 = builder.makeCall(item.getType(), SqlStdOperatorTable.ITEM, ImmutableList.of(left, right));

        left = result1;
        right = literal;
        RexNode result2 = builder.makeCall(item.getType(), SqlStdOperatorTable.ITEM, ImmutableList.of(left, right));

        return result2;
      }
      return null;
    }

    private RexNode buildArrayItemRef(RexCall item) {
      RexVisitor<RexNode> visitor =
              new RexVisitorImpl<RexNode>(true) {
                @Override public RexNode visitCall(RexCall ref) {
                  if (SqlStdOperatorTable.ITEM.equals(ref.getOperator()) &&
                          ref.getOperands().get(0) instanceof RexInputRef) {
                    RexNode arrayRef = makeArrayItem(ref);
                    if (arrayRef != null) {
                      return arrayRef;
                    }
                  }
                  return builder.makeCall(ref.getType(), ref.getOperator(), visitChildren(ref, this));
                }

                @Override
                public RexNode visitLiteral(RexLiteral literal) {
                  return literal;
                }
              };
      RexNode arrayItem = item.accept(visitor);
      exprsReferencingFlatten.add(arrayItem);
      return arrayItem;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      SqlOperator op = call.getOperator();
      RelDataType type = call.getType();

      if (SqlStdOperatorTable.ITEM.equals(op)) {
        RexNode node = buildArrayItemRef(call);
        if (node != null) {
          return node;
        }
      }
      return builder.makeCall(type, op, visitChildren(call, this));
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      // check if input is referencing a FLATTEN
      String projectFieldName = project.getRowType().getFieldNames().get(inputRef.getIndex());
      RexCall c;
      if ((c = flattenMap.get(projectFieldName)) != null) {
        RexNode left = c.getOperands().get(0);
        Preconditions.checkArgument(left != null, "Found null input reference for Flatten") ;

        // take the Flatten's input and build a new RexExpr with ITEM($n, -1)
        RexLiteral right = builder.makeBigintLiteral(BigDecimal.valueOf(-1));
        RexNode result = builder.makeCall(inputRef.getType(), SqlStdOperatorTable.ITEM, ImmutableList.of(left, right));

        // save the individual ITEM exprs for future use
        exprsReferencingFlatten.add(result);
        return result;
      } else {
        // since the filter will ultimately be pushed into the Scan, we are interested in
        // the input of the project exprs
        RexNode n = project.getProjects().get(inputRef.getIndex());

        // save all such exprs (i.e those not referencing Flatten) for future use
        otherExprs.add(n);
        return n;
      }
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
      return literal;
    }

    private List<RexNode> visitChildren(RexCall call, RexVisitor<RexNode> visitor) {
      List<RexNode> children = Lists.newArrayList();
      for (RexNode child : call.getOperands()) {
        children.add(child.accept(visitor));
      }
      return ImmutableList.copyOf(children);
    }

    public List<RexNode> getExprsReferencingFlatten() {
      return exprsReferencingFlatten;
    }

    public List<RexNode> getOtherExprs() {
      return otherExprs;
    }
  }
}
