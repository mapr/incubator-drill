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
package org.apache.drill.exec.planner.index.generators.common;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
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
import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.index.FlattenCallContext;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class FlattenConditionUtils {

  /**
   * A holder class to maintain artifacts related to filter conditions on flatten expressions
   * and other conditions. It provides utility methods to combine multiple conditions as needed.
   */
  public static class ComposedConditionInfo {
    private RexBuilder builder;
    private Map<String, RexNode> nameComposedCondMap = Maps.newHashMap();
    private RexNode conditionBelowFlatten = null;
    private List<RexNode> otherRemainderConjuncts = Lists.newArrayList();

    public RexNode getConditionBelowFlatten() {
      return conditionBelowFlatten;
    }

    public List<RexNode> getflattenConditions() {
      List<RexNode> flattenConditions = new ArrayList<>(nameComposedCondMap.values());
      return flattenConditions;
    }

    public List<RexNode> getOtherRemainderConjuncts() {
      return otherRemainderConjuncts;
    }

    public ComposedConditionInfo(RexBuilder builder) {
      this.builder = builder;
    }

    public void addCondition(String name, RexNode c) {
      nameComposedCondMap.put(name,  c);
    }

    public void addOtherRemainderConjunct(RexNode c) {
      otherRemainderConjuncts.add(c);
    }

    public void setConditionBelowFlatten(RexNode c) {
      conditionBelowFlatten = c;
    }

    public Set<String> getFieldNamesWithFlatten() {
      return nameComposedCondMap.keySet();
    }

    public int numEntries() {
      return nameComposedCondMap.size();
    }

    public RexNode getMainFlattenCondition(String fieldNameWithFlatten) {
      RexNode cond = nameComposedCondMap.get(fieldNameWithFlatten);
      // compose a new condition by combining with the condition below flatten
      if (conditionBelowFlatten != null) {
        RexNode combinedCondition = RexUtil.composeConjunction(builder,
            ImmutableList.of(cond, conditionBelowFlatten), false);
        return combinedCondition;
      }
      return cond;
    }

    public RexNode getRemainderFlattenCondition(String fieldNameWithFlatten) {
      RexNode remainderCond = null;
      List<RexNode> remainderList = Lists.newArrayList();
      for (Map.Entry<String, RexNode> e : nameComposedCondMap.entrySet()) {
        if ( !e.getKey().equals(fieldNameWithFlatten) ) {
          remainderList.add(e.getValue());
        }
      }

      remainderList.addAll(otherRemainderConjuncts);

      if (remainderList.size() > 0) {
        remainderCond = RexUtil.composeConjunction(builder, ImmutableList.copyOf(remainderList), false);
      }

      return remainderCond;

    }

    public RexNode getTotalCondition() {
      List<RexNode> conjuncts = Lists.newArrayList();
      for (Map.Entry<String, RexNode> e : nameComposedCondMap.entrySet()) {
        conjuncts.add(e.getValue());
      }

      conjuncts.addAll(otherRemainderConjuncts);

      if (conditionBelowFlatten != null) {
        conjuncts.add(conditionBelowFlatten);
      }
      RexNode totalCondition = RexUtil.composeConjunction(builder, ImmutableList.copyOf(conjuncts), false);

      return totalCondition;
    }

  }

  /** Compose filter conditions by combining the filter above flatten and those below.  Suppose the query is:
   *  ...(SELECT flatten(a) as f1, flatten(a) as f2 ..WHERE e > 5) WHERE f1.b > 20 AND f2.c < 50
   * In this case we will create 2 RexNodes corresponding to following 2 filter conditions:
   *    f1.b > 20 AND e > 5
   *    f2.c < 50 AND e > 5
   * These are kept in a map maintained by ComposedConditionInfo where the key of the map is the name of the
   * project expression: 'f1' or 'f2' etc.
   * Conditions that do not reference Flatten exprs are also saved in separate lists
   *
   * The RexNodes created for the Flatten filter conditions are of type ITEM($n, -1) where
   * 'n' = field ordinal from the child
   */
  public static void composeConditions(FlattenCallContext flattenContext, RexBuilder builder,
      ComposedConditionInfo cInfo) {
    if (flattenContext.getFilterAboveRootFlatten() == null) {
      // return null because filter below flatten is supposed to be handled by a separate
      // index planning rule
      return;
    }
    RexNode origConditionAboveFlatten = flattenContext.getFilterAboveRootFlatten().getCondition();

    // break up the condition into conjuncts
    List<RexNode> conjuncts = RelOptUtil.conjunctions(origConditionAboveFlatten);

    Map<String, List<RexNode>> newConjunctsMap = Maps.newHashMap();
    Map<String, List<RexNode>> exprsReferencingFlattenMap = Maps.newHashMap();

    // process each conjunct separately
    for (RexNode c : conjuncts) {
      FilterVisitor  filterVisitor =
          new FilterVisitor(flattenContext.getProjectToFlattenMapForAllProjects(), builder);

      RexNode conjunctAboveFlatten = c.accept(filterVisitor);

      // if the number of flatten exprs referenced by a single conjunct is > 1 then currently we
      // are not supporting this
      Preconditions.checkArgument(filterVisitor.getExprsReferencingFlattenMap().keySet().size() <= 1,
          "This type of predicate is not currently supported");

      if (filterVisitor.getExprsReferencingFlattenMap().size() == 0) {
        // since this conjunct is not referencing Flatten, save this in the 'other remainder' list
        cInfo.addOtherRemainderConjunct(conjunctAboveFlatten);
        continue;
      }

      // save the results from each iteration of the filter visitor such that
      // consolidated information can be used further
      for (Map.Entry<String, List<RexNode>> e :
        filterVisitor.getExprsReferencingFlattenMap().entrySet()) {

        if (newConjunctsMap.containsKey(e.getKey())) {
          List<RexNode> newConjuncts = newConjunctsMap.get(e.getKey());
          newConjuncts.add(conjunctAboveFlatten);
        } else {
          newConjunctsMap.put(e.getKey(), Lists.newArrayList(conjunctAboveFlatten));
        }

        if (exprsReferencingFlattenMap.containsKey(e.getKey())) {
          List<RexNode> exprs = exprsReferencingFlattenMap.get(e.getKey());
          exprs.addAll(e.getValue());
        } else {
          exprsReferencingFlattenMap.put(e.getKey(), e.getValue());
        }
      }
    }

    // Since there could be multiple conjuncts involving the same flatten expression
    // (e.g f1.b > 20, f1.c < 30 etc.), consolidate the conjuncts into a single RexNode
    // for each of the flatten expression
    for (Map.Entry<String, List<RexNode>> e : newConjunctsMap.entrySet()) {
      List<RexNode> newConjuncts = e.getValue();
      RexNode cond = RexUtil.composeConjunction(builder,
          ImmutableList.copyOf(newConjuncts), false);
      cInfo.addCondition(e.getKey(), cond);
    }

    // keep track of the exprs that were created representing filter exprs referencing flatten
    flattenContext.setFilterExprsReferencingFlatten(exprsReferencingFlattenMap);

    // if only filters above Flatten are present, return the information gathered so far
    if (flattenContext.getFilterBelowLeafFlatten() == null) {
      return;
    }

    // handle the filters below the flatten
    RexNode conditionFilterBelowFlatten = flattenContext.getFilterBelowLeafFlatten().getCondition();
    if (flattenContext.getLeafProjectAboveScan() != null) {
      FilterVisitor  filterVisitor2 =
          new FilterVisitor(flattenContext.getLeafProjectAboveScan(), builder);
      conditionFilterBelowFlatten = flattenContext.getFilterBelowLeafFlatten().getCondition().accept(filterVisitor2);

      // keep track of the relevant exprs in the filter that are referencing the
      // child Project
      flattenContext.setRelevantExprsInLeafProject(filterVisitor2.getOtherExprs());
    } else if (conditionFilterBelowFlatten != null) {
      InputRefVisitor refVisitor = new InputRefVisitor();
      conditionFilterBelowFlatten.accept(refVisitor);
      List<RexInputRef> inputRefList = refVisitor.getInputRefs();

      flattenContext.setExprsForLeafFilter(inputRefList);
    }

    cInfo.setConditionBelowFlatten(conditionFilterBelowFlatten);
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

    private Iterator<Entry<RelNode, Map<String, RexCall>>> projectFlattenIterator;
    private Entry<RelNode, Map<String, RexCall>> currentEntry  = null;
    private final RexBuilder builder;
    private DrillProjectRelBase leafProjectAboveScan = null;

    // The key of the map is the name of the Project expression for Flatten.
    // Value is the list of expressions in the filter that are referencing that particular
    // FLATTEN expr from the child Project.
    private Map<String, List<RexNode>> exprsReferencingFlattenMap = Maps.newHashMap();

    // list of expressions in the filter that are _NOT_ referencing
    // FLATTEN expr from the child Project
    private final List<RexNode> otherExprs = Lists.newArrayList();

    private final Map<String, RexCall> emptyFlattenMap = new HashMap<String, RexCall>();

    private class LocalItemVisitor extends RexVisitorImpl<RexNode> {

      private String projectFieldName = null;

      public LocalItemVisitor() {
        super(true);
      }

      private RexNode makeArrayItem(RexCall item) {
        RexInputRef inputRef = (RexInputRef) item.getOperands().get(0);
        RexLiteral literal = (RexLiteral) item.getOperands().get(1);

        // check if input is referencing a FLATTEN
        String projectFieldName = getCurrentProject().getRowType().getFieldNames().get(inputRef.getIndex());
        RexCall c;
        RexNode left = null;
        if ((c = getCurrentFlattenMap().get(projectFieldName)) != null) {
          left = c.getOperands().get(0);
          Preconditions.checkArgument(left != null, "Found null input reference for Flatten");

          // take the Flatten's input and build a new RexExpr with ITEM($n, -1)
          RexLiteral right = builder.makeBigintLiteral(BigDecimal.valueOf(-1));

          if (projectFlattenIterator.hasNext()) {
            currentEntry = projectFlattenIterator.next();
            left = left.accept(this);
          }

          RexNode result1 = builder.makeCall(item.getType(), SqlStdOperatorTable.ITEM, ImmutableList.of(left, right));

          left = result1;
          right = literal;
          RexNode result2 = builder.makeCall(item.getType(), SqlStdOperatorTable.ITEM, ImmutableList.of(left, right));

          return result2;
        }
        RexNode result1 = null;
        if (projectFlattenIterator.hasNext()) {
          RexNode inputFlattenNode = builder.makeCall(item.getType(), SqlStdOperatorTable.ITEM, ImmutableList.of(inputRef, literal));
          currentEntry = projectFlattenIterator.next();
          result1 = inputFlattenNode.accept(this);
        }
        return result1;
      }

      public String getProjectFieldName() {
        return projectFieldName;
      }

      @Override public RexNode visitCall(RexCall ref) {
        if (SqlStdOperatorTable.ITEM.equals(ref.getOperator()) &&
            ref.getOperands().get(0) instanceof RexInputRef) {

          // keep track of the project field name from the child project referenced by this ITEM expr
          RexInputRef iRef = (RexInputRef) ref.getOperands().get(0);
          projectFieldName = getCurrentProject().getRowType().getFieldNames().get(iRef.getIndex());

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

      @Override
      public RexNode visitInputRef(RexInputRef inputRef) { return inputRef; }
    }

    public FilterVisitor(LinkedHashMap<RelNode, Map<String, RexCall>> projectToFlattenExprsMap,
        RexBuilder builder) {
      super(true);
      this.builder = builder;
      this.projectFlattenIterator = projectToFlattenExprsMap.entrySet().iterator();
      this.currentEntry = projectFlattenIterator.hasNext() ? projectFlattenIterator.next() : null;
    }

    public FilterVisitor(RexBuilder builder, Entry<RelNode, Map<String, RexCall>> currentEntry, Iterator<Entry<RelNode, Map<String, RexCall>>> projectFlattenIterator) {
      super(true);
      this.builder = builder;
      this.projectFlattenIterator = projectFlattenIterator;
      this.currentEntry = currentEntry;
      this.exprsReferencingFlattenMap = null;
    }

    public FilterVisitor(DrillProjectRelBase leafProjectAboveScan,
        RexBuilder builder) {
      super(true);
      this.builder = builder;
      this.leafProjectAboveScan = leafProjectAboveScan;
    }

    private RexNode buildArrayItemRef(RexCall item) {
      LocalItemVisitor visitor = new LocalItemVisitor();
      RexNode arrayItem = item.accept(visitor);

      // save the mapping of the project field name to the arrayItem expr created
      String projectFieldName = visitor.getProjectFieldName();
      Preconditions.checkArgument(projectFieldName != null);

      List<RexNode> exprsReferencingFlatten = null;

      if (exprsReferencingFlattenMap != null) {
        if ((exprsReferencingFlatten = exprsReferencingFlattenMap.get(projectFieldName)) == null) {
          exprsReferencingFlatten = Lists.newArrayList();
          exprsReferencingFlattenMap.put(projectFieldName, exprsReferencingFlatten);
        }
        exprsReferencingFlatten.add(arrayItem);
      }
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
      String projectFieldName = getCurrentProject().getRowType().getFieldNames().get(inputRef.getIndex());
      RexCall c;
      if ((c = getCurrentFlattenMap().get(projectFieldName)) != null) {
        RexNode left = c.getOperands().get(0);
        Preconditions.checkArgument(left != null, "Found null input reference for Flatten");

        // take the Flatten's input and build a new RexExpr with ITEM($n, -1)
        RexLiteral right = builder.makeBigintLiteral(BigDecimal.valueOf(-1));
        if (projectFlattenIterator.hasNext()) {
          currentEntry = projectFlattenIterator.next();
          left = left.accept(new FilterVisitor(this.builder, this.currentEntry, this.projectFlattenIterator));
        }
        RexNode result = builder.makeCall(inputRef.getType(), SqlStdOperatorTable.ITEM,
            ImmutableList.of(left, right));

        List<RexNode> exprsReferencingFlatten = null;
        if ((exprsReferencingFlatten = exprsReferencingFlattenMap.get(projectFieldName)) == null) {
          exprsReferencingFlatten = Lists.newArrayList();
          exprsReferencingFlattenMap.put(projectFieldName, exprsReferencingFlatten);
        }
        // save the individual ITEM exprs for future use
        exprsReferencingFlatten.add(result);
        return result;
      } else {
        // since the filter will ultimately be pushed into the Scan, we are interested in
        // the input of the project exprs
        RexNode n = getCurrentProject().getProjects().get(inputRef.getIndex());

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

    public Map<String, List<RexNode>> getExprsReferencingFlattenMap() {
      return exprsReferencingFlattenMap;
    }

    public List<RexNode> getOtherExprs() {
      return otherExprs;
    }

    private DrillProjectRelBase getCurrentProject() {
      if (currentEntry != null) {
        return (DrillProjectRelBase) currentEntry.getKey();
      } else {
        return leafProjectAboveScan;
      }
    }

    private Map<String, RexCall> getCurrentFlattenMap() {
      if (currentEntry != null) {
        return currentEntry.getValue();
      } else {
        return emptyFlattenMap;
      }
    }
  }

  /**
   * Simple visitor to gather all input refs in a filter
   */
  private static class InputRefVisitor extends RexVisitorImpl<Void> {
    private final List<RexInputRef> inputRefList;

    public InputRefVisitor() {
      super(true);
      inputRefList = new ArrayList<>();
    }

    public Void visitInputRef(RexInputRef ref) {
      inputRefList.add(ref);
      return null;
    }

    public Void visitCall(RexCall call) {
      for (RexNode operand : call.operands) {
        operand.accept(this);
      }
      return null;
    }

    public List<RexInputRef> getInputRefs() {
      return inputRefList;
    }
  }

}
