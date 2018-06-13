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
package org.apache.drill.exec.planner.index.generators;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.drill.exec.planner.index.FlattenPhysicalPlanCallContext;
import org.apache.drill.exec.planner.index.IndexDescriptor;
import org.apache.drill.exec.planner.index.SemiJoinIndexPlanCallContext;
import org.apache.drill.exec.planner.index.generators.common.SemiJoinIndexPlanUtils;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.Prule;
import org.apache.drill.exec.planner.physical.RowKeyJoinPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;

import static org.apache.drill.exec.planner.physical.Prel.DRILL_PHYSICAL;

import java.util.List;

/**
 * Generate a Non covering index plan that is semantically equivalent to the original plan.
 *
 * This plan will be further optimized by the filter pushdown rule of the Index plugin which should
 * push the filter into the index scan and restricted scan respectively.
 */
public class SemiJoinMergeRowKeyJoinGenerator extends NonCoveringIndexPlanGenerator {
  private final SemiJoinIndexPlanCallContext joinContext;
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SemiJoinMergeRowKeyJoinGenerator.class);

  public SemiJoinMergeRowKeyJoinGenerator(SemiJoinIndexPlanCallContext indexContext,
                                          IndexDescriptor indexDesc,
                                          IndexGroupScan indexGroupScan,
                                          RexNode indexCondition,
                                          RexNode remainderCondition,
                                          RexNode totalCondition,
                                          RexBuilder builder,
                                          PlannerSettings settings) {
    super(indexContext.rightSide, indexDesc, indexGroupScan,
            indexCondition, remainderCondition, totalCondition, builder, settings);
    this.joinContext = indexContext;
    this.setGenerateRangePartitionExchange(false);
  }

  @Override
  public boolean forceConvert() {
    return true;
  }

  /**
   * convertChild is called by base class to transform the respective pattern to a physical plan.
   * SemiJoinMergeRowKeyJoinGenerator transforms the following pattern.
   *              JOIN                                              RKJ
   *        PROJECT  AGGREGATION                          PROJECT      HASHAGG
   *     SCAN           FILTER              ===>    RESTRICTED SCAN       PROJECT
   *                        PROJECT                                         FILTER
   *                            SCAN                                          INDEX SCAN
   * The above mentioned pattern is transformed to right side physical plan using following algorithm.
   * Call the convertChild of the base class, note that base class plan generated is a non covering index.
   * Then gather all the projects that were created in the base class plan till the RKJ operator, these
   * are stored in the projectRels. Gather all the left side relation nodes on the RKJ operator and prepare a
   * IndexPhysicalPlanCallcontext (leftContext). Similarly gather all the left side relation nodes on the root join
   * operator and prepare a IndexPhysicalCallContext(leftSideJoinContext).
   *
   * These gathered plan context's are merged with each other and ProjectRels are created with root as the child.
   *
   * This root is left side input for new RKJ operator and right side input is a hashAgg whose input is original RKJ's
   * (i.e from super class generated plan) right input.
   */
  @Override
  public List<RelNode> convertChildMulti(final RelNode join, final RelNode input) throws InvalidRelException {
    List<ProjectPrel> projectRels = Lists.newArrayList();

    // get the non covering index plan from the super class.
    RelNode nonCoveringIndexScanPrel = super.convertChild(joinContext.rightSide.upperProject, input);
    if (nonCoveringIndexScanPrel == null) {
      logger.info("semi_join_index_plan_info: Non covering index plan(base class) is null.");
      throw new InvalidRelException("Non-covering index plan generated by base class is null");
    }

    logger.info("semi_join_index_plan_info: Non covering index plan is generated by base class");
    logger.debug("semi_join_index_plan_info: Non covering index plan generated: {}", nonCoveringIndexScanPrel);

    //gather the top level projects and get the RKJ operator.
    RowKeyJoinPrel rkj = getRKJAndGatherProjsAboveRKJ(nonCoveringIndexScanPrel, projectRels);
    logger.debug("semi_join_index_plan_info: Gathered top level project rels: {}", projectRels);

    //gather the left side rel's of the RKJ operator.
    FlattenPhysicalPlanCallContext leftContext = gatherLeftSideRelsOfRKJ(rkj);
    //get the aggregation nodes input.
    RelNode aggInput = SemiJoinIndexPlanUtils.getRightInputOfRowKeyJoin(rkj);
    List<RelNode> agg = SemiJoinIndexPlanUtils.buildAgg(joinContext, joinContext.distinct, aggInput);
    logger.debug("semi_join_index_plan_info: generated hash aggregation operators: {}", agg);


    //gather the left side relations of the root join.
    FlattenPhysicalPlanCallContext leftSideJoinContext = SemiJoinIndexPlanUtils.gatherLeftSideRelsOfJoin(joinContext);
    //merge the left side relations of the join and left side relations of the RKJ operator.
    RelNode root = merge(leftSideJoinContext, leftContext);
    //apply the top level projects.
    root = SemiJoinIndexPlanUtils.applyProjects(root, projectRels, joinContext.join.getInput(0), this.joinContext.call.builder());
    //build the top project to make sure that no fields are selected from the RKJ operator's relation nodes.
    root = DrillRelOptUtil.mergeProjects(
            SemiJoinIndexPlanUtils.buildProject(root, joinContext.join.getInput(0)),
            (ProjectPrel)root, false, joinContext.call.builder());

    if (root instanceof LogicalProject) {
      root = new ProjectPrel(input.getCluster(), input.getTraitSet(), root.getInput(0),
              ((LogicalProject) root).getProjects(), root.getRowType());
    }
    logger.info("semi_join_index_plan_info: create top level ROW KEY join");
    //build the top ROWKEY join operator on the merged root and agg.
    return SemiJoinIndexPlanUtils.buildRowKeyJoin(joinContext, root, buildRangePartitioners(agg));
  }

  /**
   * Merge algorithm creates the nodes in the reverse order. It first merges the scan nodes and then
   * builds other top (project, filter, project) with previous nodes as the input.
   */
  private RelNode merge(FlattenPhysicalPlanCallContext leftJoinContext, FlattenPhysicalPlanCallContext leftRKJContext) {
    RelNode leftInput = leftJoinContext.getScan();
    RelNode rightInput = leftRKJContext.getScan();
    Preconditions.checkArgument(leftInput != null && rightInput != null);
    RelNode input = SemiJoinIndexPlanUtils.mergeScan((ScanPrel)leftInput, (ScanPrel)rightInput);
    logger.debug("semi_join_index_plan_info: merge scan ( {}, {} ) => {} ", leftInput, rightInput, input );

    if (leftJoinContext.getLeafProjectAboveScan() != null || leftRKJContext.getLeafProjectAboveScan() != null) {
      leftInput = SemiJoinIndexPlanUtils.getProject(leftInput, (ProjectPrel) leftJoinContext.getLeafProjectAboveScan());
      rightInput = SemiJoinIndexPlanUtils.getProject(rightInput, (ProjectPrel) leftRKJContext.getLeafProjectAboveScan());
      input = SemiJoinIndexPlanUtils.mergeProject((ProjectPrel) leftInput, (ProjectPrel) rightInput, input, this.joinContext.call.builder());
      logger.debug("semi_join_index_plan_info: merge project ( {}, {} ) => {} ", leftInput, rightInput, input);
    }

    if (leftJoinContext.getFilterBelowFlatten() != null || leftRKJContext.getFilterBelowFlatten() != null) {
      leftInput = SemiJoinIndexPlanUtils.getFilter(leftInput, (FilterPrel) leftJoinContext.getFilterBelowFlatten());
      rightInput = SemiJoinIndexPlanUtils.getFilter(rightInput, (FilterPrel) leftRKJContext.getFilterBelowFlatten());
      input = SemiJoinIndexPlanUtils.mergeFilter((FilterPrel) leftInput, (FilterPrel) rightInput, input);
      logger.debug("semi_join_index_plan_info: merge filter ( {}, {} ) => {} ", leftInput, rightInput, input);
    }

    if (leftJoinContext.getProjectWithFlatten() != null || leftRKJContext.getProjectWithFlatten() != null) {
      leftInput = SemiJoinIndexPlanUtils.getProject(leftInput, (ProjectPrel) leftJoinContext.getProjectWithFlatten());
      rightInput = SemiJoinIndexPlanUtils.getProject(rightInput, (ProjectPrel) leftRKJContext.getProjectWithFlatten());
      input = SemiJoinIndexPlanUtils.mergeProject((ProjectPrel) leftInput, (ProjectPrel) rightInput, input, this.joinContext.call.builder());
      logger.debug("semi_join_index_plan_info: merge project ( {}, {} ) => {} ", leftInput, rightInput, input);
    }

    if (leftJoinContext.getFilterAboveFlatten() != null || leftRKJContext.getFilterAboveFlatten() != null) {
      leftInput = SemiJoinIndexPlanUtils.getFilter(leftInput, (FilterPrel) leftJoinContext.getFilterAboveFlatten());
      rightInput = SemiJoinIndexPlanUtils.getFilter(rightInput, (FilterPrel) leftRKJContext.getFilterAboveFlatten());
      input = SemiJoinIndexPlanUtils.mergeFilter((FilterPrel) leftInput, (FilterPrel) rightInput, input);
      logger.debug("semi_join_index_plan_info: merge filter ( {}, {} ) => {} ", leftInput, rightInput, input);
    }

    if (leftJoinContext.getProjectAboveFlatten() != null || leftRKJContext.getProjectAboveFlatten() != null) {
      leftInput = SemiJoinIndexPlanUtils.getProject(leftInput, (ProjectPrel) leftJoinContext.getProjectAboveFlatten());
      rightInput = SemiJoinIndexPlanUtils.getProject(rightInput, (ProjectPrel) leftRKJContext.getProjectAboveFlatten());
      input = SemiJoinIndexPlanUtils.mergeProject((ProjectPrel) leftInput, (ProjectPrel) rightInput, input, this.joinContext.call.builder());
      logger.debug("semi_join_index_plan_info: merge project ( {}, {} ) => {} ", leftInput, rightInput, input);
    }

    return input;
  }

  private RowKeyJoinPrel getRKJAndGatherProjsAboveRKJ(RelNode node, List<ProjectPrel> projectRels) {
    Preconditions.checkArgument(node instanceof RowKeyJoinPrel ||
                                node instanceof ProjectPrel);
    if (node instanceof  ProjectPrel ) {
      projectRels.add((ProjectPrel) node);
      return getRKJAndGatherProjsAboveRKJ(((ProjectPrel) node).getInput(), projectRels);
    } else {
      return (RowKeyJoinPrel) node;
    }
  }

  private FlattenPhysicalPlanCallContext gatherLeftSideRelsOfRKJ(RowKeyJoinPrel rkj) {
    RelNode node = rkj.getInput(0);
    List<RelNode> relNodes = Lists.newArrayList();
    SemiJoinIndexPlanUtils.getRelNodesBottomUp(node, relNodes);
    return SemiJoinIndexPlanUtils.getPhysicalContext(relNodes);
  }

  @Override
  public boolean goMulti() throws InvalidRelException {
    RelNode top = indexContext.getCall().rel(0);
    if (top instanceof DrillJoinRel) {
      DrillJoinRel join = (DrillJoinRel) top;
      final RelNode input0 = join.getInput(0);
      final RelNode input1 = join.getInput(1);
      RelTraitSet traits0 = input0.getTraitSet().plus(DRILL_PHYSICAL);
      RelNode convertedInput0 = Prule.convert(input0, traits0);
      RelTraitSet traits1 = input1.getTraitSet().plus(DRILL_PHYSICAL);
      RelNode convertedInput1 = Prule.convert(input1, traits1);
      return this.goMulti(top, convertedInput0) && this.goMulti(top, convertedInput1);
    } else {
      return false;
    }
  }
}
