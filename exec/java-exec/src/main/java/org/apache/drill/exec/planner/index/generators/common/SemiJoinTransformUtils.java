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
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.ListUtils;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.drill.exec.planner.index.FlattenIndexPlanCallContext;
import org.apache.drill.exec.planner.index.IndexPlanUtils;
import org.apache.drill.exec.planner.index.SemiJoinIndexPlanCallContext;
import org.apache.drill.exec.planner.index.rules.AbstractMatchFunction;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.drill.exec.planner.logical.DrillRel.DRILL_LOGICAL;

public class SemiJoinTransformUtils {

  public static FlattenIndexPlanCallContext transformJoinToSingleTableScan(SemiJoinIndexPlanCallContext context,
                                                                           FunctionImplementationRegistry functionRegistry,
                                                                           org.slf4j.Logger logger) {
    DrillScanRel newScan = null;
    DrillFilterRel filterBelowFlatten = null;
    DrillProjectRel projectAboveScan = null;
    if (!context.isCoveringIndexPlanApplicable()) {
      logger.info("Covering index scan is not applicable for this query as it has complex operations");
      return null;
    }

    DrillRel left = context.leftSide.scan;
    DrillRel right = context.rightSide.scan;
    Pair<DrillRel, Map<Integer, Integer>> newScanInfo = merge((DrillScanRel)left, (DrillScanRel)right);
    newScan = (DrillScanRel)newScanInfo.left;

    if (context.rightSide.getLeafProjectAboveScan() != null) {
      left = SemiJoinIndexPlanUtils.getProject(left, (DrillProjectRel)null);
      right = SemiJoinIndexPlanUtils.getProject(right, (DrillProjectRel) context.rightSide.getLeafProjectAboveScan());
      newScanInfo = constructProject(context.join.getCluster(), newScanInfo.left, newScanInfo.right,
              (DrillProjectRel)left, (DrillProjectRel)right, functionRegistry, true);
      projectAboveScan = (DrillProjectRel)newScanInfo.left;
    }

    if (context.rightSide.getFilterBelowFlatten() != null) {
      left = SemiJoinIndexPlanUtils.getFilter(left, (DrillFilterRel)null);
      right = SemiJoinIndexPlanUtils.getFilter(right, (DrillFilterRel) context.rightSide.getFilterBelowFlatten());
      newScanInfo = constructFilter(context, newScanInfo.right, newScanInfo.left,
              (DrillFilterRel)left, (DrillFilterRel)right);
      filterBelowFlatten = (DrillFilterRel)newScanInfo.left;
    }

    DrillRel leftLowerProject = SemiJoinIndexPlanUtils.getProject(left, context.leftSide.lowerProject);
    DrillRel rightLowerProject = SemiJoinIndexPlanUtils.getProject(right, context.rightSide.lowerProject);
    Pair<DrillRel, Map<Integer, Integer>> lowerProjInfo = constructProject(context.join.getCluster(), newScanInfo.left, newScanInfo.right,
            (DrillProjectRel)leftLowerProject, (DrillProjectRel)rightLowerProject, functionRegistry, true);

    DrillRel leftFilter = SemiJoinIndexPlanUtils.getFilter(leftLowerProject, context.leftSide.filter);
    DrillRel rightFilter = SemiJoinIndexPlanUtils.getFilter(rightLowerProject, context.rightSide.filter);
    Pair<DrillRel, Map<Integer, Integer>> filterInfo = constructFilter(context, lowerProjInfo.right, lowerProjInfo.left,
            (DrillFilterRel)leftFilter, (DrillFilterRel)rightFilter);

    DrillProjectRel leftUpperProject = SemiJoinIndexPlanUtils.getProject(leftFilter, context.leftSide.upperProject);
    DrillProjectRel rightUpperProject = SemiJoinIndexPlanUtils.getProject(rightFilter, context.rightSide.upperProject);
    Pair<DrillRel, Map<Integer, Integer>> upperProjectInfo = constructProject(context.join.getCluster(), filterInfo.left, filterInfo.right,
            leftUpperProject, rightUpperProject, functionRegistry,false);
    Map<String, RexCall> flattenMap = Maps.newHashMap();
    List<RexNode> nonFlattenExprs = Lists.newArrayList();
    AbstractMatchFunction.projectHasFlatten((DrillProjectRel) lowerProjInfo.left, false, flattenMap, nonFlattenExprs);

    FlattenIndexPlanCallContext logicalPlanCallContext = new FlattenIndexPlanCallContext(context.call, (DrillProjectRel)upperProjectInfo.left,
            (DrillFilterRel) filterInfo.left, (DrillProjectRel) lowerProjInfo.left, filterBelowFlatten, projectAboveScan,  newScan,
            flattenMap, nonFlattenExprs);
    return logicalPlanCallContext;
  }

  public static Pair<DrillRel, Map<Integer,Integer>> constructFilter(SemiJoinIndexPlanCallContext context, Map<Integer, Integer> inputColMap,
                                                               DrillRel input, DrillFilterRel leftFilter, DrillFilterRel rightFilter) {
    RexBuilder builder = input.getCluster().getRexBuilder();
    RexNode leftFilterCondition = leftFilter.getCondition();
    RexNode rightFilterCondition = IndexPlanUtils.transform(leftFilter.getRowType().getFieldList().size(),
            builder, rightFilter.getCondition(),
            rightFilter.getInput().getRowType());
    RexNode finalCondition = IndexPlanUtils.transform(RexUtil.composeConjunction(builder,
            Lists.newArrayList(leftFilterCondition, rightFilterCondition),
            false), inputColMap, builder);
    return Pair.of(new DrillFilterRel(context.rightSide.filter.getCluster(),
            context.rightSide.filter.getTraitSet().plus(DRILL_LOGICAL),
            input, finalCondition), inputColMap);
  }

  public static Pair<DrillRel, Map<Integer,Integer>> constructProject(RelOptCluster cluster, DrillRel newScan, Map<Integer, Integer> inputColMap,
                                                                      DrillProjectRel leftProject, DrillProjectRel rightProject,
                                                                      FunctionImplementationRegistry functionRegistry, boolean uniquify) {
    List<RexNode> leftProjects = leftProject.getProjects();
    List<RexNode> rightProjects = IndexPlanUtils.projectsTransformer(leftProjects.size(), cluster.getRexBuilder(),
            rightProject.getProjects(), rightProject.getInput().getRowType());
    List<String> leftProjectsNames = leftProject.getRowType().getFieldNames();
    List<String> rightProjectsNames = rightProject.getRowType().getFieldNames();
    Pair<Pair<List<RexNode>, List<String>>, Map<Integer, Integer>> normalizedInfo = normalize(cluster.getRexBuilder(),
            ListUtils.union(leftProjects, rightProjects), functionRegistry, uniquify,
            ListUtils.union(leftProjectsNames, rightProjectsNames), inputColMap);

    Project proj = (Project) RelOptUtil.createProject(newScan, normalizedInfo.left.left, normalizedInfo.left.right);
    return Pair.of(DrillProjectRel.create(cluster, rightProject.getTraitSet().plus(DRILL_LOGICAL),
            proj.getInput(), proj.getProjects(),
            proj.getRowType()), normalizedInfo.right);
  }

  public static Pair<DrillRel, Map<Integer,Integer>> merge(DrillScanRel leftScan, DrillScanRel rightScan) {

    List<String> rightSideColumns = rightScan.getRowType().getFieldNames();
    List<String> leftSideColumns = leftScan.getRowType().getFieldNames();
    List<RelDataType> rightSideTypes = relDataTypeFromRelFieldType(rightScan.getRowType().getFieldList());
    List<RelDataType> leftSideTypes = relDataTypeFromRelFieldType(leftScan.getRowType().getFieldList());
    Pair<Pair<Pair<List<RelDataType>,
            List<String>>, List<SchemaPath>>,
            Map<Integer, Integer>> normalizedInfo = normalize(ListUtils.union(leftSideTypes, rightSideTypes),
            ListUtils.union(leftSideColumns, rightSideColumns), ListUtils.union(leftScan.getColumns(), rightScan.getColumns()));

    return Pair.of(new DrillScanRel(leftScan.getCluster(),
            rightScan.getTraitSet().plus(DRILL_LOGICAL),
            rightScan.getTable(),
            leftScan.getCluster().getTypeFactory().createStructType(normalizedInfo.left.left.left, normalizedInfo.left.left.right),
            normalizedInfo.left.right, false), normalizedInfo.right);
  }

  /**
   * Normalizes the datatype , columns by removing redundant columns and their types.
   * @param types column type info for this scan.
   * @param columns column information for this scan.
   * @param grpScanCols group scan columns this scan.
   * @return uniquified datatype, fieldnames, schemapaths and columnmap between input output fields
   */
  public static Pair<Pair<Pair<List<RelDataType>, List<String>>,
          List<SchemaPath>>,Map<Integer, Integer>> normalize(List<RelDataType> types,
                                                             List<String> columns, List<SchemaPath> grpScanCols) {
    Preconditions.checkArgument(types.size() == columns.size());

    Map<Integer, Integer> colIndexMap = new HashMap<>();
    Map<String, Integer> uniqueColMap = new HashMap<>();
    List<RelDataType> uniqueTypes = Lists.newArrayList();
    List<String> uniqueCols = Lists.newArrayList();

    int index = 0;
    for (int i=0;i<columns.size(); i++) {
      String columnName = columns.get(i);
      Integer colIndex = uniqueColMap.putIfAbsent(columnName, index);
      if (colIndex == null) {
        uniqueCols.add(columns.get(i));
        uniqueTypes.add(types.get(i));
        colIndexMap.put(i, index);
        index++;
      } else {
        colIndexMap.put(i, colIndex);
      }
    }

    return Pair.of(Pair.of(Pair.of(uniqueTypes, uniqueCols), grpScanCols), colIndexMap);
  }

  /**
   * Normalizes the expressions and columnnames. This function is used for Projects.
   * @param builder expression builder.
   * @param exprs list of expressions.
   * @param uniquify to uniquify the types and columns.
   * @param fieldNames names of the columns.
   * @param fieldRefMap columnmap.
   * @return transformed expressions with respect to fieldRefMap and column names.
   */
  public static Pair<Pair<List<RexNode>, List<String>>, Map<Integer, Integer>> normalize(RexBuilder builder, List<RexNode> exprs,
                                                                                   FunctionImplementationRegistry functionRegistry,
                                                                                   boolean uniquify, List<String> fieldNames,
                                                                                   Map<Integer, Integer> fieldRefMap) {
    Preconditions.checkArgument(exprs.size() == fieldNames.size());

    Map<Integer, Integer> colIndexMap = new HashMap<>();
    Map<String, Integer> uniqueColMap = new HashMap<>();
    List<RexNode> uniqueExprs = Lists.newArrayList();
    List<String> uniqueFieldNames = Lists.newArrayList();
    DrillRelOptUtil.RexFieldsTransformer transformer = new DrillRelOptUtil.RexFieldsTransformer(builder, fieldRefMap);

    int index = 0;
    for (int i=0;i<exprs.size();i++) {
      RexNode transFormedExpr = transformer.go(exprs.get(i));
      //any complex condition like Flatten should not be simplified, doing so will change the semantics of the query.
      boolean isComplexFunction = isComplexFunction(transFormedExpr, functionRegistry);
      Integer colIndex = uniqueColMap.putIfAbsent(transFormedExpr.toString(), index);
      if (!uniquify || colIndex == null || isComplexFunction) {
        uniqueExprs.add(transFormedExpr);
        uniqueFieldNames.add(fieldNames.get(i));
        colIndexMap.put(i, index);
        index++;
      } else {
        colIndexMap.put(i, colIndex);
      }
    }

    return Pair.of(Pair.of(uniqueExprs, uniqueFieldNames), colIndexMap);
  }

  public static boolean isComplexFunction(RexNode expr, FunctionImplementationRegistry functionRegistry) {
    if (expr instanceof RexCall) {
      if (functionRegistry.isFunctionComplexOutput(((RexCall) expr).getOperator().getName())) {
        return true;
      }
    }
    return false;
  }


  public static List<RelDataType> relDataTypeFromRelFieldType(List<RelDataTypeField> fieldTypes) {
    List<RelDataType> result = Lists.newArrayList();

    for (RelDataTypeField type : fieldTypes) {
      result.add(type.getType());
    }
    return result;
  }
}
