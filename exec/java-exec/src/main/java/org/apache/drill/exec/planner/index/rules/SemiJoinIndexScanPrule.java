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
package org.apache.drill.exec.planner.index.rules;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Pair;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.collections.ListUtils;
import org.apache.drill.exec.planner.index.FlattenIndexPlanCallContext;
import org.apache.drill.exec.planner.index.SemiJoinIndexPlanCallContext;
import org.apache.drill.exec.planner.index.IndexDescriptor;
import org.apache.drill.exec.planner.index.IndexPlanUtils;
import org.apache.drill.exec.planner.index.FunctionalIndexInfo;
import org.apache.drill.exec.planner.index.IndexLogicalPlanCallContext;
import org.apache.drill.exec.planner.index.generators.SemiJoinMergeRowKeyJoinGenerator;
import org.apache.drill.exec.planner.index.generators.SemiJoinToRowKeyJoinGenerator;
import org.apache.drill.exec.planner.index.generators.SemiJoinToCoveringIndexScanGenerator;
import org.apache.drill.exec.planner.index.generators.IndexPlanGenerator;
import org.apache.drill.exec.planner.index.generators.AbstractIndexPlanGenerator;
import org.apache.drill.exec.planner.index.generators.common.SemiJoinIndexPlanUtils;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.physical.PlannerSettings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.drill.exec.planner.logical.DrillRel.DRILL_LOGICAL;

public class SemiJoinIndexScanPrule extends AbstractIndexPrule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SemiJoinIndexScanPrule.class);

  public static final RelOptRule JOIN_FILTER_PROJECT_SCAN = new SemiJoinIndexScanPrule(
          RelOptHelper.some(DrillJoinRel.class,
                  RelOptHelper.any(AbstractRelNode.class),
                  RelOptHelper.some(DrillAggregateRel.class,
                          RelOptHelper.some(DrillProjectRel.class,
                              RelOptHelper.some(DrillFilterRel.class,
                                  RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class)))))),
          "SemiJoinIndexScanPrule:Join_Project_Filter_Project_Scan", new MatchJSPFPS());

  public static final RelOptRule JOIN_FILTER_PROJECT_FILTER_SCAN = new SemiJoinIndexScanPrule(
          RelOptHelper.some(DrillJoinRel.class,
                  RelOptHelper.any(AbstractRelNode.class),
                  RelOptHelper.some(DrillAggregateRel.class,
                          RelOptHelper.some(DrillProjectRel.class,
                                  RelOptHelper.some(DrillFilterRel.class,
                                          RelOptHelper.some(DrillProjectRel.class,
                                                  RelOptHelper.some(DrillFilterRel.class ,RelOptHelper.any(DrillScanRel.class))))))),
          "SemiJoinIndexScanPrule:Join_Project_Filter_Project_Filter_Scan", new MatchJSPFPFS());

  public static final RelOptRule JOIN_FILTER_PROJECT_FILTER_PROJECT_SCAN = new SemiJoinIndexScanPrule(
          RelOptHelper.some(DrillJoinRel.class,
                  RelOptHelper.any(AbstractRelNode.class),
                  RelOptHelper.some(DrillAggregateRel.class,
                          RelOptHelper.some(DrillProjectRel.class,
                                  RelOptHelper.some(DrillFilterRel.class,
                                          RelOptHelper.some(DrillProjectRel.class,
                                                  RelOptHelper.some(DrillFilterRel.class,
                                                          RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class)))))))),
          "SemiJoinIndexScanPrule:Join_Project_Filter_Project_Filter_Project_Scan", new MatchJSPFPFPS());

  final private MatchFunction<SemiJoinIndexPlanCallContext> match;

  private SemiJoinIndexScanPrule(RelOptRuleOperand operand,
                                 String description,
                                 MatchFunction<SemiJoinIndexPlanCallContext> match) {
    super(operand, description);
    this.match = match;
  }

  private static class SemiJoinCoveringIndexPlanGenerator implements IndexPlanGenerator {
    private final SemiJoinIndexPlanCallContext context;

    private SemiJoinCoveringIndexPlanGenerator(SemiJoinIndexPlanCallContext context) {
      this.context = context;
    }

    @Override
    public AbstractIndexPlanGenerator getCoveringIndexGen(FunctionalIndexInfo functionInfo,
                                                          IndexGroupScan indexGroupScan,
                                                          RexNode indexCondition,
                                                          RexNode remainderCondition,
                                                          RexBuilder builder, PlannerSettings settings) {
      return new SemiJoinToCoveringIndexScanGenerator(context, functionInfo, indexGroupScan,
              indexCondition, remainderCondition, builder, settings);
    }

    @Override
    public AbstractIndexPlanGenerator getNonCoveringIndexGen(IndexDescriptor indexDesc,
                                                             IndexGroupScan indexGroupScan,
                                                             RexNode indexCondition,
                                                             RexNode remainderCondition,
                                                             RexNode totalCondition,
                                                             RexBuilder builder, PlannerSettings settings) {
      throw new RuntimeException();
    }
  }

  private static class SemiJoinNonCoveringIndexPlanGenerator implements IndexPlanGenerator {
    private final SemiJoinIndexPlanCallContext context;

    private SemiJoinNonCoveringIndexPlanGenerator(SemiJoinIndexPlanCallContext context) {
      this.context = context;
    }

    @Override
    public AbstractIndexPlanGenerator getCoveringIndexGen(FunctionalIndexInfo functionInfo,
                                                          IndexGroupScan indexGroupScan,
                                                          RexNode indexCondition,
                                                          RexNode remainderCondition,
                                                          RexBuilder builder, PlannerSettings settings) {
      return new SemiJoinToRowKeyJoinGenerator(context, functionInfo, indexGroupScan,
              indexCondition, remainderCondition, builder, settings);
    }

    @Override
    public AbstractIndexPlanGenerator getNonCoveringIndexGen(IndexDescriptor indexDesc,
                                                             IndexGroupScan indexGroupScan,
                                                             RexNode indexCondition,
                                                             RexNode remainderCondition,
                                                             RexNode totalCondition,
                                                             RexBuilder builder, PlannerSettings settings) {
      return new SemiJoinMergeRowKeyJoinGenerator(context, indexDesc, indexGroupScan, indexCondition,
              remainderCondition, totalCondition, builder, settings);
    }
  }

  private static class MatchJSPFPFPS extends AbstractMatchFunction<SemiJoinIndexPlanCallContext> {
    IndexLogicalPlanCallContext context = null;

    @Override
    public boolean match(RelOptRuleCall call) {
      DrillJoinRel joinRel = call.rel(0);
      DrillAggregateRel aggRel = call.rel(2);
      DrillProjectRel upperProject = call.rel(3);
      DrillFilterRel upperFilter = call.rel(4);
      DrillProjectRel lowerProject = call.rel(5);
      DrillFilterRel lowerFilter= call.rel(6);
      DrillProjectRel lowerProjectAboveScan = call.rel(7);
      DrillScanRel rightRel = call.rel(8);

      IndexLogicalPlanCallContext context = IndexPlanUtils.generateContext(call, call.rel(1), logger);

      if (context.scan == null) {
        return false;
      }

      this.context = context;
      if (!SemiJoinIndexPlanUtils.checkSameTableScan(context.scan, rightRel)) {
        return false;
      }

      if (aggRel.getAggCallList().size() > 0) {
        return false;
      }

      if (checkScan(rightRel)) {
        //check if the lower project contains the flatten.
        return projectHasFlatten(lowerProject, true, null, null);
      } else {
        return false;
      }
    }

    @Override
    public SemiJoinIndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillProjectRel upperProject = call.rel(3);
      final DrillFilterRel upperFilter = call.rel(4);
      final DrillProjectRel lowerProject = call.rel(5);
      final DrillFilterRel lowerFilter = call.rel(6);
      final DrillProjectRel lowerProjectAboveScan = call.rel(7);
      final DrillScanRel rightScan = call.rel(8);

      final DrillJoinRel join = call.rel(0);
      final DrillAggregateRel distinct = call.rel(2);

      Map<String, RexCall> flattenMap = Maps.newHashMap();
      List<RexNode> nonFlattenExprs = Lists.newArrayList();

      // populate the flatten and non-flatten collections
      projectHasFlatten(lowerProject, false, flattenMap, nonFlattenExprs);

      FlattenIndexPlanCallContext rightSideContext = new FlattenIndexPlanCallContext(call, upperProject,
              upperFilter, lowerProject,lowerFilter, lowerProjectAboveScan, rightScan, flattenMap, nonFlattenExprs);
      SemiJoinIndexPlanCallContext idxContext = new SemiJoinIndexPlanCallContext(call, join, distinct, this.context, rightSideContext);
      idxContext.setCoveringIndexPlanApplicable(!projectHasFlatten(context.lowerProject, true, null,null) &&
              !projectHasFlatten(context.upperProject, true, null, null));
      return idxContext;
    }
  }

  private static class MatchJSPFPFS extends AbstractMatchFunction<SemiJoinIndexPlanCallContext> {
    IndexLogicalPlanCallContext context = null;

    @Override
    public boolean match(RelOptRuleCall call) {
      DrillJoinRel joinRel = call.rel(0);
      DrillAggregateRel aggRel = call.rel(2);
      DrillProjectRel upperProject = call.rel(3);
      DrillFilterRel upperFilter = call.rel(4);
      DrillProjectRel lowerProject = call.rel(5);
      DrillFilterRel lowerFilter= call.rel(6);
      DrillScanRel rightRel = call.rel(7);

      IndexLogicalPlanCallContext context = IndexPlanUtils.generateContext(call, call.rel(1), logger);

      if (context.scan == null) {
        return false;
      }

      this.context = context;
      if (!SemiJoinIndexPlanUtils.checkSameTableScan(context.scan, rightRel)) {
        return false;
      }

      if (aggRel.getAggCallList().size() > 0) {
        return false;
      }

      if (checkScan(rightRel)) {
        //check if the lower project contains the flatten.
        return projectHasFlatten(lowerProject, true, null, null);
      } else {
        return false;
      }
    }

    @Override
    public SemiJoinIndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillProjectRel upperProject = call.rel(3);
      final DrillFilterRel upperFilter = call.rel(4);
      final DrillProjectRel lowerProject = call.rel(5);
      final DrillFilterRel lowerFilter = call.rel(6);
      final DrillScanRel rightScan = call.rel(7);

      final DrillJoinRel join = call.rel(0);
      final DrillAggregateRel distinct = call.rel(2);

      Map<String, RexCall> flattenMap = Maps.newHashMap();
      List<RexNode> nonFlattenExprs = Lists.newArrayList();

      // populate the flatten and non-flatten collections
      projectHasFlatten(lowerProject, false, flattenMap, nonFlattenExprs);

      FlattenIndexPlanCallContext rightSideContext = new FlattenIndexPlanCallContext(call, upperProject,
              upperFilter, lowerProject,lowerFilter, rightScan, flattenMap, nonFlattenExprs);
      SemiJoinIndexPlanCallContext idxContext = new SemiJoinIndexPlanCallContext(call, join, distinct, this.context, rightSideContext);
      idxContext.setCoveringIndexPlanApplicable(!projectHasFlatten(context.lowerProject, true, null,null) &&
              !projectHasFlatten(context.upperProject, true, null, null));
      return idxContext;
    }
  }

  private static class MatchJSPFPS extends AbstractMatchFunction<SemiJoinIndexPlanCallContext> {

    IndexLogicalPlanCallContext context = null;

    @Override
    public boolean match(RelOptRuleCall call) {
      DrillJoinRel joinRel = call.rel(0);
      DrillAggregateRel aggRel = call.rel(2);
      DrillProjectRel upperProject = call.rel(3);
      DrillFilterRel filter = call.rel(4);
      DrillProjectRel lowerProject = call.rel(5);
      DrillScanRel rightRel = call.rel(6);

      IndexLogicalPlanCallContext context = IndexPlanUtils.generateContext(call, call.rel(1), logger);

      if (context.scan == null) {
        return false;
      }

      this.context = context;
      if (!SemiJoinIndexPlanUtils.checkSameTableScan(context.scan, rightRel)) {
        return false;
      }

      if (aggRel.getAggCallList().size() > 0) {
        return false;
      }

      if (checkScan(rightRel)) {
        //check if the lower project contains the flatten.
        return projectHasFlatten(lowerProject, true, null, null);
      } else {
        return false;
      }
    }

    @Override
    public SemiJoinIndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillProjectRel upperProject = call.rel(3);
      final DrillFilterRel filter = call.rel(4);
      final DrillProjectRel lowerProject = call.rel(5);
      final DrillScanRel rightScan = call.rel(6);

      final DrillJoinRel join = call.rel(0);
      final DrillAggregateRel distinct = call.rel(2);

      Map<String, RexCall> flattenMap = Maps.newHashMap();
      List<RexNode> nonFlattenExprs = Lists.newArrayList();

      // populate the flatten and non-flatten collections
      projectHasFlatten(lowerProject, false, flattenMap, nonFlattenExprs);

      FlattenIndexPlanCallContext rightSideContext = new FlattenIndexPlanCallContext(call, upperProject, filter, lowerProject,
              null, rightScan, flattenMap, nonFlattenExprs);

      SemiJoinIndexPlanCallContext idxContext = new SemiJoinIndexPlanCallContext(call, join, distinct,
              this.context, rightSideContext);
      idxContext.setCoveringIndexPlanApplicable(!projectHasFlatten(context.lowerProject, true, null,null) &&
              !projectHasFlatten(context.upperProject, true, null, null));
      return idxContext;
    }
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return match.match(call);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    doOnMatch(match.onMatch(call));
  }

  private void doOnMatch(SemiJoinIndexPlanCallContext indexContext) {
    if (indexContext.join != null) {
      FlattenIndexPlanCallContext context = transformJoinToSingleTableScan(indexContext);
      indexContext.set(context);
      if (context != null && FlattenToIndexScanPrule.FILTER_PROJECT_SCAN.doOnMatch(context, new SemiJoinCoveringIndexPlanGenerator(indexContext))){
        return;
      }
    }

    //As we couldn't generate a single table scan for the join,
    //try index planning by only considering the PFPS.
    FlattenToIndexScanPrule.FILTER_PROJECT_SCAN.doOnMatch(indexContext.rightSide, new SemiJoinNonCoveringIndexPlanGenerator(indexContext));
  }

  private FlattenIndexPlanCallContext transformJoinToSingleTableScan(SemiJoinIndexPlanCallContext context) {
    DrillScanRel newScan = null;
    DrillFilterRel filterBelowFlatten = null;
    DrillProjectRel projectAboveScan = null;
    if (!context.isCoveringIndexPlanApplicable()) {
      return null;
    }

    DrillRel left = context.leftSide.scan;
    DrillRel right = context.rightSide.scan;
    Pair<DrillRel, Map<Integer, Integer>> newScanInfo = merge((DrillScanRel)left, (DrillScanRel)right);
    newScan = (DrillScanRel)newScanInfo.left;

    if (context.rightSide.getLeafProjectAboveScan() != null) {
      left = SemiJoinIndexPlanUtils.getProject(left, (DrillProjectRel)null);
      right = SemiJoinIndexPlanUtils.getProject(right, context.rightSide.getLeafProjectAboveScan());
      newScanInfo = constructProject(context.join.getCluster(), newScanInfo.left, newScanInfo.right,
              (DrillProjectRel)left, (DrillProjectRel)right, true);
      projectAboveScan = (DrillProjectRel)newScanInfo.left;
    }

    if (context.rightSide.getFilterBelowFlatten() != null) {
      left = SemiJoinIndexPlanUtils.getFilter(left, (DrillFilterRel)null);
      right = SemiJoinIndexPlanUtils.getFilter(right, context.rightSide.getFilterBelowFlatten());
      newScanInfo = constructFilter(context, newScanInfo.right, newScanInfo.left,
              (DrillFilterRel)left, (DrillFilterRel)right);
      filterBelowFlatten = (DrillFilterRel)newScanInfo.left;
    }

    DrillRel leftLowerProject = SemiJoinIndexPlanUtils.getProject(left, context.leftSide.lowerProject);
    DrillRel rightLowerProject = SemiJoinIndexPlanUtils.getProject(right, context.rightSide.lowerProject);
    Pair<DrillRel, Map<Integer, Integer>> lowerProjInfo = constructProject(context.join.getCluster(), newScanInfo.left, newScanInfo.right,
            (DrillProjectRel)leftLowerProject, (DrillProjectRel)rightLowerProject, true);

    DrillRel leftFilter = SemiJoinIndexPlanUtils.getFilter(leftLowerProject, context.leftSide.filter);
    DrillRel rightFilter = SemiJoinIndexPlanUtils.getFilter(rightLowerProject, context.rightSide.filter);
    Pair<DrillRel, Map<Integer, Integer>> filterInfo = constructFilter(context, lowerProjInfo.right, lowerProjInfo.left,
            (DrillFilterRel)leftFilter, (DrillFilterRel)rightFilter);

    DrillProjectRel leftUpperProject = SemiJoinIndexPlanUtils.getProject(leftFilter, context.leftSide.upperProject);
    DrillProjectRel rightUpperProject = SemiJoinIndexPlanUtils.getProject(rightFilter, context.rightSide.upperProject);
    Pair<DrillRel, Map<Integer, Integer>> upperProjectInfo = constructProject(context.join.getCluster(), filterInfo.left, filterInfo.right,
                                                leftUpperProject, rightUpperProject, false);
    Map<String, RexCall> flattenMap = Maps.newHashMap();
    List<RexNode> nonFlattenExprs = Lists.newArrayList();
    AbstractMatchFunction.projectHasFlatten((DrillProjectRel) lowerProjInfo.left, false, flattenMap, nonFlattenExprs);

    FlattenIndexPlanCallContext logicalPlanCallContext = new FlattenIndexPlanCallContext(context.call, (DrillProjectRel)upperProjectInfo.left,
            (DrillFilterRel) filterInfo.left, (DrillProjectRel) lowerProjInfo.left, filterBelowFlatten, projectAboveScan,  newScan,
            flattenMap, nonFlattenExprs);
    return logicalPlanCallContext;
  }

  private Pair<DrillRel, Map<Integer,Integer>> constructFilter(SemiJoinIndexPlanCallContext context, Map<Integer, Integer> inputColMap,
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

  private Pair<DrillRel, Map<Integer,Integer>> constructProject(RelOptCluster cluster, DrillRel newScan, Map<Integer, Integer> inputColMap,
                                           DrillProjectRel leftProject, DrillProjectRel rightProject, boolean uniquify) {
    List<RexNode> leftProjects = leftProject.getProjects();
    List<RexNode> rightProjects = IndexPlanUtils.projectsTransformer(leftProjects.size(), cluster.getRexBuilder(),
                                                  rightProject.getProjects(), rightProject.getRowType());
    List<String> leftProjectsNames = leftProject.getRowType().getFieldNames();
    List<String> rightProjectsNames = rightProject.getRowType().getFieldNames();
    Pair<Pair<List<RexNode>, List<String>>, Map<Integer, Integer>> normalizedInfo = normalize(cluster.getRexBuilder(),
                                                            ListUtils.union(leftProjects, rightProjects), uniquify,
                                                            ListUtils.union(leftProjectsNames, rightProjectsNames), inputColMap);

    Project proj = (Project)RelOptUtil.createProject(newScan, normalizedInfo.left.left, normalizedInfo.left.right);
    return Pair.of(DrillProjectRel.create(cluster, rightProject.getTraitSet().plus(DRILL_LOGICAL),
            proj.getInput(), proj.getProjects(),
            proj.getRowType()), normalizedInfo.right);
  }

  private Pair<DrillRel, Map<Integer,Integer>> merge(DrillScanRel leftScan, DrillScanRel rightScan) {

    List<String> rightSideColumns = rightScan.getRowType().getFieldNames();
    List<String> leftSideColumns = leftScan.getRowType().getFieldNames();
    List<RelDataType> rightSideTypes = relDataTypeFromRelFieldType(rightScan.getRowType().getFieldList());
    List<RelDataType> leftSideTypes = relDataTypeFromRelFieldType(leftScan.getRowType().getFieldList());
    Pair<Pair<Pair<List<RelDataType>,
            List<String>>, List<SchemaPath>>,
         Map<Integer, Integer>> normalizedInfo = normalize(ListUtils.union(leftSideTypes, rightSideTypes),
            ListUtils.union(leftSideColumns, rightSideColumns), ListUtils.union(leftScan.getColumns(), rightScan.getColumns()));

    return Pair.of(new DrillScanRel(leftScan.getCluster(),
            rightScan.getTraitSet(),
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
  private Pair<Pair<Pair<List<RelDataType>, List<String>>,
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
  private Pair<Pair<List<RexNode>, List<String>>, Map<Integer, Integer>> normalize(RexBuilder builder, List<RexNode> exprs,
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
      Integer colIndex = uniqueColMap.putIfAbsent(transFormedExpr.toString(), index);
      if (!uniquify || colIndex == null) {
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

  private List<RelDataType> relDataTypeFromRelFieldType(List<RelDataTypeField> fieldTypes) {
    List<RelDataType> result = Lists.newArrayList();

    for (RelDataTypeField type : fieldTypes) {
      result.add(type.getType());
    }
    return result;
  }
}
