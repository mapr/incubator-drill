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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptUtil;
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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.collections.ListUtils;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.drill.exec.planner.index.FlattenPhysicalPlanCallContext;
import org.apache.drill.exec.planner.index.IndexPhysicalPlanCallContext;
import org.apache.drill.exec.planner.index.IndexPlanUtils;
import org.apache.drill.exec.planner.index.SemiJoinIndexPlanCallContext;
import org.apache.drill.exec.planner.index.rules.AbstractMatchFunction;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.physical.AggPruleBase;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.physical.RowKeyJoinPrel;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.HashAggPrule;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.JoinPruleBase;
import org.apache.drill.exec.planner.physical.StreamAggPrule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.drill.exec.planner.logical.DrillRel.DRILL_LOGICAL;
import static org.apache.drill.exec.planner.physical.Prel.DRILL_PHYSICAL;


public class SemiJoinIndexPlanUtils {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SemiJoinIndexPlanUtils.class);


  public static RelNode applyProjects(RelNode node, List<ProjectPrel> projectRels,
                                      RelNode leftSideJoinNode, RelBuilder builder) {
    RelNode input = node;
    RelNode inputNode = leftSideJoinNode;
    for (ProjectPrel proj : Lists.reverse(projectRels)) {
      inputNode = buildProject(inputNode, inputNode);
      input = mergeProject((ProjectPrel) inputNode, proj, input, builder);
    }
    return input;
  }

  /**
   * Given a join context it traverses the nodes and creates the corresponding physical rels.
   */
  public static FlattenPhysicalPlanCallContext gatherLeftSideRelsOfJoin(SemiJoinIndexPlanCallContext joinContext) {
    List<RelNode> nodes = Lists.newArrayList();
    physicalRel(joinContext.leftSide.upperProject,
            physicalRel(joinContext.leftSide.filter,
                    physicalRel(joinContext.leftSide.lowerProject,
                            physicalRel(joinContext.leftSide.scan, nodes))));
    logger.debug("semi_join_index_plan_info: gathered nodes from left side of join: {}", nodes);
    return SemiJoinIndexPlanUtils.getPhysicalContext(nodes);
  }


  private static List<RelNode> physicalRel(DrillScanRel scan, List<RelNode> collectionOfRels) {
    Preconditions.checkNotNull(scan);
    Preconditions.checkArgument(scan.getGroupScan() instanceof DbGroupScan);

    DbGroupScan restrictedScan = ((DbGroupScan)scan.getGroupScan()).getRestrictedScan(scan.getColumns());
    restrictedScan.setComplexFilterPushDown(true);
    collectionOfRels.add(new ScanPrel(scan.getCluster(), scan.getTraitSet().plus(DRILL_PHYSICAL),
            restrictedScan, scan.getRowType(), scan.getTable()));
    return collectionOfRels;
  }

  private static List<RelNode> physicalRel(DrillProjectRel projectRel, List<RelNode> collectionOfRels) {
    if (projectRel == null) {
      return collectionOfRels;
    }

    collectionOfRels.add(new ProjectPrel(projectRel.getCluster(), projectRel.getTraitSet().plus(DRILL_PHYSICAL),
            collectionOfRels.get(collectionOfRels.size()-1), projectRel.getProjects(), projectRel.getRowType()));
    return collectionOfRels;
  }

  private static List<RelNode> physicalRel(DrillFilterRel filter, List<RelNode> collectionOfRels) {
    if (filter == null) {
      return collectionOfRels;
    }

    collectionOfRels.add(new FilterPrel(filter.getCluster(), filter.getTraitSet().plus(DRILL_PHYSICAL),
            collectionOfRels.get(collectionOfRels.size()-1), filter.getCondition()));
    return collectionOfRels;
  }

  public static List<RelNode> buildRowKeyJoin(SemiJoinIndexPlanCallContext joinContext,
                                        RelNode leftInput, List<RelNode> distinct) throws InvalidRelException {

    List<RelNode> rkjNodes = new ArrayList<>();
    for (RelNode agg : distinct) {
      rkjNodes.add(new RowKeyJoinPrel(leftInput.getCluster(), distinct.get(0).getTraitSet(), leftInput, agg,
              joinContext.join.getCondition(), JoinRelType.INNER));
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

  public static FilterPrel buildFilter(RelNode input) {
    return new FilterPrel(input.getCluster(), input.getTraitSet().plus(DRILL_PHYSICAL), input,
            input.getCluster().getRexBuilder().makeLiteral(true));
  }

  public static DrillFilterRel buildFilter(DrillRel input) {
    return new DrillFilterRel(input.getCluster(), input.getTraitSet().plus(DRILL_LOGICAL), input,
            input.getCluster().getRexBuilder().makeLiteral(true));
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

  public static ScanPrel mergeScan(ScanPrel leftScan, ScanPrel rightScan) {
    Preconditions.checkArgument(SemiJoinIndexPlanUtils.checkSameTableScan(leftScan, rightScan));
    Preconditions.checkArgument(leftScan.getGroupScan() instanceof DbGroupScan &&
                                rightScan.getGroupScan() instanceof DbGroupScan);

    List<String> rightSideColumns = rightScan.getRowType().getFieldNames();
    List<String> leftSideColumns = leftScan.getRowType().getFieldNames();
    List<RelDataType> rightSideTypes = relDataTypeFromRelFieldType(rightScan.getRowType().getFieldList());
    List<RelDataType> leftSideTypes = relDataTypeFromRelFieldType(leftScan.getRowType().getFieldList());

    DbGroupScan leftGroupScan = (DbGroupScan) leftScan.getGroupScan();
    DbGroupScan rightGroupScan = (DbGroupScan) rightScan.getGroupScan();

    DbGroupScan restrictedGroupScan  = ((DbGroupScan) IndexPlanUtils.getGroupScan(leftScan))
            .getRestrictedScan(ListUtils.union(leftGroupScan.getColumns(), rightGroupScan.getColumns()));
    restrictedGroupScan.setComplexFilterPushDown(true);
    return new ScanPrel(leftScan.getCluster(),
            rightScan.getTraitSet(),
            restrictedGroupScan,
            leftScan.getCluster().getTypeFactory().createStructType(ListUtils.union(leftSideTypes, rightSideTypes),
                    SqlValidatorUtil.uniquify(ListUtils.union(leftSideColumns, rightSideColumns))),
            leftScan.getTable());
  }

  public static RelNode mergeFilter(FilterPrel filterL,
                                    FilterPrel filterR,
                                    RelNode input) {
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
    return new FilterPrel(input.getCluster(), input.getTraitSet(), input,
            RexUtil.composeConjunction(input.getCluster().getRexBuilder(), combineConditions, false));
  }

  public static RelNode mergeProject(ProjectPrel projectL, ProjectPrel projectR,
                                     RelNode input, RelBuilder builder) {
    RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    List<RexNode> combinedProjects = Lists.newArrayList();
    combinedProjects.addAll(IndexPlanUtils.projectsTransformer(0,rexBuilder, projectL.getProjects(),
            projectL.getInput().getRowType()));
    combinedProjects.addAll(IndexPlanUtils.projectsTransformer(projectL.getProjects().size(), rexBuilder, projectR.getProjects(),
            projectR.getInput().getRowType()));
    List<RexNode> listOfProjects = Lists.newArrayList();
    listOfProjects.addAll(combinedProjects);
    Project proj1 = (Project) RelOptUtil.createProject(input, listOfProjects,
            ListUtils.union(projectL.getRowType().getFieldNames(), projectR.getRowType().getFieldNames()));
    ProjectPrel upperProject = new ProjectPrel(input.getCluster(), input.getTraitSet(), input, listOfProjects,
            proj1.getRowType());
    if (input instanceof ProjectPrel) {
      RelNode proj = DrillRelOptUtil.mergeProjects(upperProject,(ProjectPrel) input, false, builder);
      if (proj instanceof LogicalProject) {
        return new ProjectPrel(input.getCluster(), input.getTraitSet(), proj.getInput(0),
                ((LogicalProject) proj).getProjects(), proj.getRowType());
      } else {
        return proj;
      }
    } else {
      return upperProject;
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

  public static List<RelNode> getRelNodesBottomUp(RelNode node, List<RelNode> result) {
    if (node.getInputs().size() == 0) {
      result.add(node);
      return result;
    }

    getRelNodesBottomUp(node.getInputs().get(0), result);
    result.add(node);
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
    // generating one phase aggregation.
    if (hashAggEnabled) {
      result.add(HashAggPrule.singlePhaseHashAgg(joinContext.call, distinct, input.getTraitSet().plus(DRILL_PHYSICAL), input));
    }
    DbGroupScan grpScan = (DbGroupScan) joinContext.leftSide.scan.getGroupScan();

    //generate two phase aggregation.
    if (hashAggEnabled && AggPruleBase.create2PhasePlan(joinContext.call, distinct)) {
      DrillDistributionTrait distOnAllKeys = JoinPruleBase.getRangePartitionTrait(joinContext.join, grpScan, distinct.getRowType());
      result.add(new HashAggPrule.TwoPhaseHashAggWithRangeExchange(joinContext.call, distOnAllKeys).convertChild(distinct, input));
    }
    return result;
  }

  private static List<RelNode> generateStreamAgg(SemiJoinIndexPlanCallContext joinContext,
                                               DrillAggregateRel distinct, RelNode input) throws InvalidRelException {
    Preconditions.checkNotNull(joinContext.call);
    boolean streamAggEnabled = PrelUtil.getPlannerSettings(joinContext.call.getPlanner()).isStreamAggEnabled();
    List<RelNode> result = Lists.newArrayList();
    final RelCollation collation = DrillRelOptUtil.getCollation(distinct);

    // generating one phase aggregation.
    if (streamAggEnabled) {
      result.add(StreamAggPrule.singlePhaseStreamAgg(distinct, input.getTraitSet().plus(DRILL_PHYSICAL).plus(collation), input));
    }
    DbGroupScan grpScan = (DbGroupScan) joinContext.leftSide.scan.getGroupScan();

    //generate two phase aggregation.
    if (streamAggEnabled && AggPruleBase.create2PhasePlan(joinContext.call, distinct)) {
      DrillDistributionTrait distOnAllKeys = JoinPruleBase.getRangePartitionTrait(joinContext.join, grpScan, distinct.getRowType());
      result.add(new StreamAggPrule.TwoPhaseStreamAggWithHashMergeExchange(joinContext.call, distOnAllKeys, collation).convertChild(distinct, input));
    }
    return result;
  }

  /**
   * Given collection of relation nodes this function creates an physical Plan call context.
   * This function assumes that all the nodes are physical rels and are one of the
   * ScanPrel, FilterPrel, ProjectPrel.
   */
  public static FlattenPhysicalPlanCallContext getPhysicalContext(List<RelNode> nodes) {
    Preconditions.checkArgument( nodes.size() >= 1 && nodes.get(0) instanceof ScanPrel);

    ScanPrel scan = null;
    ProjectPrel projectAboveScan = null;
    ProjectPrel projectWithFlatten = null;
    FilterPrel filterBelowFlatten = null;
    FilterPrel filterAboveFlatten = null;
    ProjectPrel projectAboveFlatten = null;
    boolean encounteredProjWithFlattens = false;
    for (RelNode nd : nodes) {
      Preconditions.checkArgument(nd instanceof ProjectPrel
              || nd instanceof FilterPrel
              || nd instanceof ScanPrel);

      if (nd instanceof ScanPrel) {
        Preconditions.checkArgument(scan == null);
        scan = (ScanPrel)nd;
      } else if (nd instanceof ProjectPrel) {
        boolean isProjectwithFlatten = AbstractMatchFunction.projectHasFlatten((ProjectPrel)nd, true, null,null);
        Preconditions.checkArgument(!encounteredProjWithFlattens || !isProjectwithFlatten);

        encounteredProjWithFlattens = encounteredProjWithFlattens || isProjectwithFlatten;
        Preconditions.checkArgument(projectAboveScan == null || projectAboveFlatten == null || projectWithFlatten == null);
        if (!encounteredProjWithFlattens && projectAboveScan == null) {
          Preconditions.checkArgument(projectAboveScan == null);
          projectAboveScan = (ProjectPrel)nd;
        } else if(isProjectwithFlatten) {
          projectWithFlatten = (ProjectPrel) nd;
        } else {
          projectAboveFlatten = (ProjectPrel) nd;
        }
      } else {
        Preconditions.checkArgument(filterBelowFlatten == null || filterAboveFlatten == null);
        if (!encounteredProjWithFlattens) {
          Preconditions.checkArgument(filterBelowFlatten == null);
          filterBelowFlatten = (FilterPrel) nd;
        } else {
          Preconditions.checkArgument(filterAboveFlatten == null);
          filterAboveFlatten = (FilterPrel) nd;
        }
      }
    }
    Map<String, RexCall> flattenMap = Maps.newHashMap();
    List<RexNode> nonFlattenExprs = Lists.newArrayList();
    return new FlattenPhysicalPlanCallContext(projectAboveFlatten, filterAboveFlatten,projectWithFlatten,filterBelowFlatten,projectAboveScan,scan, flattenMap,nonFlattenExprs);
  }

  public static boolean checkSameTableScan(DrillScanRelBase scanA, DrillScanRelBase scanB) {
    return scanA.getTable().getQualifiedName().equals(scanB.getTable().getQualifiedName());
  }

}
