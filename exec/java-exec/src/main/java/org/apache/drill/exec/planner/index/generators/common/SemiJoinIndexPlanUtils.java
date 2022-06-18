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

import org.apache.drill.exec.planner.logical.DrillRelFactories;
import org.apache.drill.exec.planner.logical.DrillSemiJoinRel;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.ListUtils;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.drill.exec.planner.index.IndexPlanUtils;
import org.apache.drill.exec.planner.index.SemiJoinIndexPlanCallContext;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.physical.AggPruleBase;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.physical.RowKeyJoinPrel;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.HashAggPrule;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.StreamAggPrule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.drill.exec.planner.logical.DrillRel.DRILL_LOGICAL;
import static org.apache.drill.exec.planner.physical.Prel.DRILL_PHYSICAL;


public class SemiJoinIndexPlanUtils {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SemiJoinIndexPlanUtils.class);


  public static RelNode applyProjects(Pair<RelNode, Map<Integer, Integer>> input, List<ProjectPrel> projectRels,
                                      RelNode leftSideJoinNode, RelBuilder builder, PlannerSettings plannerSettings) {
    RelNode inputNode = leftSideJoinNode;
    for (ProjectPrel proj : Lists.reverse(projectRels)) {
      inputNode = buildProject(inputNode, inputNode);
      input = mergeProject((ProjectPrel) inputNode, proj, input.left, builder, plannerSettings.functionImplementationRegistry,
              input.right, true);
    }
    return input.left;
  }

  /**
   * Merge the rel nodes if both are project and generate a single merged project.
   * When one of the rel node is not project, then this function is a no-op.
   * @param topProj top project in the pipeline.
   * @param bottomProj bottom project in the pipeline.
   * @param context SemiJoin index plan context.
   * @return Merged project if both rel nodes are projects otherwise no change.
   */
  public static RelNode mergeIfPossible(RelNode topProj, RelNode bottomProj, SemiJoinIndexPlanCallContext context) {
    if (topProj instanceof Project && bottomProj instanceof Project) {
      return DrillRelOptUtil.mergeProjects(
              (ProjectPrel)topProj, (ProjectPrel) bottomProj, false, context.call.builder());
    } else {
      return topProj;
    }
  }

  public static List<RelNode> buildRowKeyJoin(SemiJoinIndexPlanCallContext joinContext,
                                        RelNode leftInput, List<RelNode> distinct) throws InvalidRelException {

    List<RelNode> rkjNodes = new ArrayList<>();
    for (RelNode agg : distinct) {
      RowKeyJoinPrel rowKeyJoinPrel = new RowKeyJoinPrel(leftInput.getCluster(), distinct.get(0).getTraitSet().plus(DRILL_PHYSICAL), leftInput, agg,
                                                        joinContext.join.getCondition(), JoinRelType.INNER);
      if (joinContext.join instanceof DrillSemiJoinRel) {
        List<RexNode> projects = IndexPlanUtils.projects(rowKeyJoinPrel.getInput(0), rowKeyJoinPrel.getInput(0).getRowType().getFieldNames());
        rkjNodes.add(new ProjectPrel(rowKeyJoinPrel.getCluster(), rowKeyJoinPrel.getTraitSet().plus(DRILL_PHYSICAL), rowKeyJoinPrel,
                projects, joinContext.join.getRowType()));
      } else {
        rkjNodes.add(rowKeyJoinPrel);
      }
    }
    return rkjNodes;
  }

  public static ProjectPrel getProject(RelNode input, ProjectPrel project) {
    if (project != null) {
      return project;
    }

    return buildProject(input, input);
  }

  public static DrillProjectRel getProject(DrillRel input, DrillProjectRel project) {
    if (project != null) {
      return project;
    }

    return buildProject(input, input);
  }

  public static FilterPrel getFilter(RelNode input, FilterPrel filter) {
    if (filter != null) {
      return filter;
    }

    return buildFilter(input);
  }

  public static DrillFilterRel getFilter(DrillRel input, DrillFilterRel filter) {
    if (filter != null) {
      return filter;
    }

    return buildFilter(input);
  }

  public static FilterPrel buildFilter(RelNode input, DrillFilterRel filterRel) {
    return new FilterPrel(input.getCluster(), input.getTraitSet().plus(DRILL_PHYSICAL), input,
            filterRel.getCondition());
  }

  public static FilterPrel buildFilter(RelNode input) {
    return new FilterPrel(input.getCluster(), input.getTraitSet().plus(DRILL_PHYSICAL), input,
            input.getCluster().getRexBuilder().makeLiteral(true));
  }

  public static DrillFilterRel buildFilter(DrillRel input) {
    return new DrillFilterRel(input.getCluster(), input.getTraitSet().plus(DRILL_LOGICAL), input,
            input.getCluster().getRexBuilder().makeLiteral(true));
  }

  public static ProjectPrel buildProject(RelNode input, DrillProjectRel projectRel) {
    return new ProjectPrel(input.getCluster(), input.getTraitSet().plus(DRILL_PHYSICAL), input,
            projectRel.getProjects(), projectRel.getRowType());
  }

  public static ProjectPrel buildProject(RelNode input, RelNode projectsRelNode) {
    return new ProjectPrel(input.getCluster(), input.getTraitSet().plus(DRILL_PHYSICAL), input,
            IndexPlanUtils.projects(input, projectsRelNode.getRowType().getFieldNames()),
            projectsRelNode.getRowType());
  }

  public static DrillProjectRel buildProject(DrillRel input, DrillRel projectsRelNode) {
    return DrillProjectRel.create(input.getCluster(), input.getTraitSet().plus(DRILL_LOGICAL), input,
            IndexPlanUtils.projects(input, projectsRelNode.getRowType().getFieldNames()),
            projectsRelNode.getRowType());
  }

  public static Pair<RelNode, Map<Integer,Integer>> mergeScan(ScanPrel leftScan, ScanPrel rightScan) {
    Preconditions.checkArgument(SemiJoinIndexPlanUtils.checkSameTableScan(leftScan, rightScan));
    Preconditions.checkArgument(leftScan.getGroupScan() instanceof DbGroupScan &&
                                rightScan.getGroupScan() instanceof DbGroupScan);

    List<String> rightSideColumns = rightScan.getRowType().getFieldNames();
    List<String> leftSideColumns = leftScan.getRowType().getFieldNames();
    List<RelDataType> rightSideTypes = relDataTypeFromRelFieldType(rightScan.getRowType().getFieldList());
    List<RelDataType> leftSideTypes = relDataTypeFromRelFieldType(leftScan.getRowType().getFieldList());

    DbGroupScan leftGroupScan = (DbGroupScan) leftScan.getGroupScan();
    DbGroupScan rightGroupScan = (DbGroupScan) rightScan.getGroupScan();
    List grpScanColumns = ListUtils.union(leftGroupScan.getColumns(), rightGroupScan.getColumns());
    Pair<Pair<Pair<List<RelDataType>,
            List<String>>, List<SchemaPath>>,
            Map<Integer, Integer>> normalizedInfo = SemiJoinTransformUtils.normalize(ListUtils.union(leftSideTypes, rightSideTypes),
            ListUtils.union(leftSideColumns, rightSideColumns), grpScanColumns);
    DbGroupScan restrictedGroupScan  = ((DbGroupScan) IndexPlanUtils.getGroupScan(leftScan))
            .getRestrictedScan(grpScanColumns);
    restrictedGroupScan.setComplexFilterPushDown(true);
    return Pair.of(new ScanPrel(leftScan.getCluster(),
            rightScan.getTraitSet(),
            restrictedGroupScan,
            leftScan.getCluster().getTypeFactory().createStructType(normalizedInfo.left.left.left, normalizedInfo.left.left.right),
            leftScan.getTable()), normalizedInfo.right);
  }

  public static Pair<RelNode, Map<Integer, Integer>> mergeFilter(FilterPrel filterL,
                                    FilterPrel filterR,
                                    RelNode input, Map<Integer, Integer> inputColMap) {
    List<RexNode> combineConditions = Lists.newArrayList();
    RexNode leftTransformedConditions = IndexPlanUtils.transform(0,input.getCluster().getRexBuilder(),
                                                                  filterL.getCondition(), filterL.getInput().getRowType());
    logger.debug("semi_join_index_plan_info: left conditions after transforming: {}", leftTransformedConditions);
    RexNode rightTransformedConditions = IndexPlanUtils.transform(filterL.getInput().getRowType().getFieldList().size(),
                                                                    input.getCluster().getRexBuilder(), filterR.getCondition(),
                                                                    filterR.getInput().getRowType());
    logger.debug("semi_join_index_plan_info: right conditions after transforming: {}", rightTransformedConditions);
    combineConditions.add(leftTransformedConditions);
    combineConditions.add(rightTransformedConditions);
    RexNode finalCondition = IndexPlanUtils.transform(RexUtil.composeConjunction(input.getCluster().getRexBuilder(),
            combineConditions,
            false), inputColMap, input.getCluster().getRexBuilder());
    return Pair.of(new FilterPrel(input.getCluster(), input.getTraitSet(), input, finalCondition), inputColMap);
  }

  public static Pair<RelNode, Map<Integer, Integer>> mergeProject(ProjectPrel projectL, ProjectPrel projectR,
                                     RelNode input, RelBuilder builder, FunctionImplementationRegistry functionRegistry,
                                     Map<Integer, Integer> inputColMap, boolean uniquify) {
    RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    List<RexNode> combinedProjects = Lists.newArrayList();
    List<RexNode> leftProjects = IndexPlanUtils.projectsTransformer(0, rexBuilder, projectL.getProjects(),
            projectL.getInput().getRowType());
    combinedProjects.addAll(leftProjects);
    List<RexNode> rightProjects = IndexPlanUtils.projectsTransformer(projectL.getProjects().size(), rexBuilder, projectR.getProjects(),
            projectR.getInput().getRowType());
    combinedProjects.addAll(rightProjects);

    List<String> leftProjectsNames = projectL.getRowType().getFieldNames();
    List<String> rightProjectsNames = projectR.getRowType().getFieldNames();

    Pair<Pair<List<RexNode>, List<String>>, Map<Integer, Integer>> normalizedInfo = SemiJoinTransformUtils.normalize(rexBuilder,
            ListUtils.union(leftProjects, rightProjects), functionRegistry, uniquify,
            ListUtils.union(leftProjectsNames, rightProjectsNames), inputColMap);
    List<RexNode> listOfProjects = Lists.newArrayList();
    listOfProjects.addAll(combinedProjects);
    Project proj1 = (Project) DrillRelFactories.DRILL_LOGICAL_PROJECT_FACTORY.createProject(input, Collections.emptyList(), normalizedInfo.left.left, normalizedInfo.left.right);
    ProjectPrel upperProject = new ProjectPrel(input.getCluster(), input.getTraitSet(), input, proj1.getProjects(),
            proj1.getRowType());
    if (input instanceof ProjectPrel &&
            !DrillRelOptUtil.containsComplexFunction(upperProject, functionRegistry) &&
            !DrillRelOptUtil.containsComplexFunction((ProjectPrel)input, functionRegistry)) {
      RelNode proj = DrillRelOptUtil.mergeProjects(upperProject, (ProjectPrel)input, false, builder);
      if (proj instanceof LogicalProject) {
        return Pair.of(new ProjectPrel(input.getCluster(), input.getTraitSet(), proj.getInput(0),
                ((LogicalProject) proj).getProjects(), proj.getRowType()), normalizedInfo.right);
      } else {
        return Pair.of(proj, normalizedInfo.right);
      }
    } else {
      return Pair.of(upperProject, normalizedInfo.right);
    }
  }

  public static RelDataType merge(RelDataType first, RelDataType second) {
    return new RelRecordType(ListUtils.union(first.getFieldList(), second.getFieldList()));
  }

  public static RelNode getRightInputOfRowKeyJoin(RowKeyJoinPrel rkj) {
    return rkj.getInput(1);
  }

  public static List<RelDataType> relDataTypeFromRelFieldType(List<RelDataTypeField> fieldTypes) {
    List<RelDataType> result = Lists.newArrayList();

    for (RelDataTypeField type : fieldTypes) {
      result.add(type.getType());
    }
    return result;
  }

  /*
   * builds one phase and two phase hash agg plans.   *
   */
  public static List<RelNode> buildAgg(SemiJoinIndexPlanCallContext joinContext,
                                       DrillAggregateRel distinct, RelNode input) throws InvalidRelException {
    List<RelNode> result = Lists.newArrayList();
    result.addAll(generateHashAgg(joinContext, distinct, input));
    result.addAll(generateStreamAgg(joinContext, distinct, input));
    return result;
  }

  public static List<RelNode> buildStreamAgg(SemiJoinIndexPlanCallContext joinContext,
                                       DrillAggregateRel distinct, RelNode input) throws InvalidRelException {
    List<RelNode> result = Lists.newArrayList();
    result.addAll(generateStreamAgg(joinContext, distinct, input));
    return result;
  }

  private static List<RelNode> generateHashAgg(SemiJoinIndexPlanCallContext joinContext,
                                               DrillAggregateRel distinct, RelNode input) throws InvalidRelException {
    Preconditions.checkNotNull(joinContext.call);
    boolean hashAggEnabled = PrelUtil.getPlannerSettings(joinContext.call.getPlanner()).isHashAggEnabled();
    List<RelNode> result = Lists.newArrayList();
    final DrillDistributionTrait distOnAllKeys =
            new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED,
                    ImmutableList.copyOf(AggPruleBase.getDistributionField(distinct, true)));
    if (hashAggEnabled) {
      // generating one phase aggregation.
      result.add(HashAggPrule.singlePhaseHashAgg(joinContext.call, distinct, input.getTraitSet().plus(distOnAllKeys).plus(DRILL_PHYSICAL), input));

      //generate two phase aggregation.
      if (AggPruleBase.create2PhasePlan(joinContext.call, distinct)) {
        result.add(new HashAggPrule.TwoPhaseHashAggWithHashExchange(joinContext.call, distOnAllKeys).convertChild(distinct, input));
      }
    }
    return result;
  }

  private static List<RelNode> generateStreamAgg(SemiJoinIndexPlanCallContext joinContext,
                                               DrillAggregateRel distinct, RelNode input) throws InvalidRelException {
    Preconditions.checkNotNull(joinContext.call);
    boolean streamAggEnabled = PrelUtil.getPlannerSettings(joinContext.call.getPlanner()).isStreamAggEnabled();
    List<RelNode> result = Lists.newArrayList();
    final RelCollation collation = DrillRelOptUtil.getCollation(distinct);
    final DrillDistributionTrait distOnAllKeys =
            new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED,
                    ImmutableList.copyOf(AggPruleBase.getDistributionField(distinct, true)));
    if (streamAggEnabled) {
      // generating one phase aggregation.
      result.add(StreamAggPrule.singlePhaseStreamAgg(distinct, input.getTraitSet().plus(DRILL_PHYSICAL).plus(distOnAllKeys).plus(collation), input));

      //generate two phase aggregation.
      if (AggPruleBase.create2PhasePlan(joinContext.call, distinct)) {
        result.add(new StreamAggPrule.TwoPhaseStreamAggWithHashMergeExchange(joinContext.call, distOnAllKeys, collation).convertChild(distinct, input));
      }
    }
    return result;
  }

  public static boolean checkSameTableScan(DrillScanRelBase scanA, DrillScanRelBase scanB) {
    return scanA.getTable().getQualifiedName().equals(scanB.getTable().getQualifiedName());
  }
}
