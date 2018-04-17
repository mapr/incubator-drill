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
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.collections.ListUtils;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.drill.exec.planner.index.IndexPhysicalPlanCallContext;
import org.apache.drill.exec.planner.index.IndexPlanUtils;
import org.apache.drill.exec.planner.index.SemiJoinIndexPlanCallContext;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.physical.HashAggPrel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.physical.RowKeyJoinPrel;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.JoinPruleBase;
import org.apache.drill.exec.planner.physical.HashAggPrule;

import java.util.List;

import static org.apache.drill.exec.planner.physical.AggPrelBase.OperatorPhase.PHASE_1of1;
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

  public static RelNode buildRowKeyJoin(SemiJoinIndexPlanCallContext joinContext,
                                        RelNode leftInput, List<HashAggPrel> distinct) throws InvalidRelException {

    return new RowKeyJoinPrel(leftInput.getCluster(), distinct.get(0).getTraitSet(), leftInput, distinct.get(0),
            joinContext.join.getCondition(), JoinRelType.INNER);

  }

  public static ProjectPrel getProject(RelNode input, ProjectPrel project) {
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

  public static FilterPrel buildFilter(RelNode input) {
    return new FilterPrel(input.getCluster(), input.getTraitSet(), input, input.getCluster().getRexBuilder().makeLiteral(true));
  }

  public static ProjectPrel buildProject(RelNode input, RelNode projectsRelNode) {
    return new ProjectPrel(input.getCluster(), input.getTraitSet(), input,
            IndexPlanUtils.projects(input, projectsRelNode.getRowType().getFieldNames()),projectsRelNode.getRowType());
  }

  public static ScanPrel mergeScan(ScanPrel leftScan, ScanPrel rightScan) {
    Preconditions.checkArgument(SemiJoinIndexPlanUtils.checkSameTableScan(leftScan, rightScan));

    List<String> rightSideColumns = rightScan.getRowType().getFieldNames();
    List<String> leftSideColumns = leftScan.getRowType().getFieldNames();
    List<RelDataType> rightSideTypes = relDataTypeFromRelFieldType(rightScan.getRowType().getFieldList());
    List<RelDataType> leftSideTypes = relDataTypeFromRelFieldType(leftScan.getRowType().getFieldList());
    DbGroupScan restrictedGroupScan  = ((DbGroupScan) IndexPlanUtils.getGroupScan(leftScan))
            .getRestrictedScan(ListUtils.union(leftSideColumns,rightSideColumns));
    return new ScanPrel(leftScan.getCluster(),
            rightScan.getTraitSet(),
            restrictedGroupScan,
            leftScan.getCluster().getTypeFactory().createStructType(ListUtils.union(leftSideTypes, rightSideTypes),
                    ListUtils.union(leftSideColumns, rightSideColumns)),
            leftScan.getTable());
  }

  public static RelNode mergeFilter(FilterPrel filterL,
                                    FilterPrel filterR,
                                    RelNode input) {
    List<RexNode> combineConditions = Lists.newArrayList();
    RexNode leftTransformedConditions = IndexPlanUtils.transform(0,input.getCluster().getRexBuilder(),
                                                                  filterL.getCondition(), filterL.getInput().getRowType(),
                                                                  input.getRowType());
    logger.debug("semi_join_index_plan_info: left conditions after transforming: {}", leftTransformedConditions);
    RexNode rightTransformedConditions = IndexPlanUtils.transform(filterL.getInput().getRowType().getFieldList().size(),
                                                                    input.getCluster().getRexBuilder(), filterR.getCondition(),
                                                                    filterR.getInput().getRowType(), input.getRowType());
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
            projectL.getInput().getRowType(), input.getRowType()));
    combinedProjects.addAll(IndexPlanUtils.projectsTransformer(projectL.getProjects().size(), rexBuilder, projectR.getProjects(),
            projectR.getInput().getRowType(), input.getRowType()));
    List<RexNode> listOfProjects = Lists.newArrayList();
    listOfProjects.addAll(combinedProjects);
    ProjectPrel upperProject = new ProjectPrel(input.getCluster(), input.getTraitSet(), input, listOfProjects,
            merge(projectL.getRowType(), projectR.getRowType()));
    if (input instanceof ProjectPrel) {
      RelNode proj = DrillRelOptUtil.mergeProjects(upperProject,(ProjectPrel) input, false, builder);
      if (proj instanceof LogicalProject) {
        return new ProjectPrel(input.getCluster(), input.getTraitSet(), proj.getInput(0),
                ((LogicalProject) proj).getProjects(), proj.getRowType());
      } else {
        return proj;
      }
    } else {
      return new ProjectPrel(input.getCluster(), input.getTraitSet(), input, listOfProjects,
              merge(projectL.getRowType(), projectR.getRowType()));
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

  public static List<RelNode> getRelNodes(RelNode node, List<RelNode> result) {
    if (node.getInputs().size() == 0) {
      result.add(node);
      return result;
    }

    result.add(node);
    return getRelNodes(node.getInputs().get(0), result);
  }

  /*
   * builds one phase and two phase hash agg plans.
   */
  public static List<HashAggPrel> buildHashAgg(SemiJoinIndexPlanCallContext joinContext,
                                         DrillAggregateRel distinct, RelNode input) throws InvalidRelException {
    List<HashAggPrel> result = Lists.newArrayList();
    // generating one phase aggregation.
    result.add(new HashAggPrel(distinct.getCluster(), input.getTraitSet().plus(DRILL_PHYSICAL),input,
            distinct.indicator,distinct.getGroupSet(), distinct.getGroupSets(), distinct.getAggCallList(),PHASE_1of1));
    DbGroupScan grpScan = (DbGroupScan) joinContext.leftSide.scan.getGroupScan();

    //generate two phase aggregation.
    DrillDistributionTrait distOnAllKeys = JoinPruleBase.getRangePartitionTrait(joinContext.join, grpScan,result.get(0).getRowType());
    result.add((HashAggPrel)new HashAggPrule.TwoPhaseHashAggWithRangeExchange(joinContext.call, distOnAllKeys).convertChild(distinct, input));
    return result;
  }

  /**
   * Given collection of relation nodes this function creates an physical Plan call context.
   * This function assumes that all the nodes are physical rels and are one of the
   * ScanPrel, FilterPrel, ProjectPrel.
   */
  public static IndexPhysicalPlanCallContext getPhysicalContext(List<RelNode> nodes) {
    ScanPrel scan = null;
    ProjectPrel lowerProj = null;
    FilterPrel filter = null;
    ProjectPrel upperProj = null;
    for (RelNode nd : Lists.reverse(nodes)) {
      Preconditions.checkArgument(nd instanceof ProjectPrel || nd instanceof FilterPrel || nd instanceof ScanPrel);
      if (nd instanceof ScanPrel) {
        Preconditions.checkArgument(scan == null);
        scan = (ScanPrel)nd;
      } else if (nd instanceof ProjectPrel) {
        Preconditions.checkArgument(lowerProj == null || upperProj == null);
        if (filter == null) {
          lowerProj = (ProjectPrel)nd;
        } else {
          upperProj = (ProjectPrel)nd;
        }
      } else {
        Preconditions.checkArgument(filter == null);
        filter = (FilterPrel)nd;
      }
    }
    return new IndexPhysicalPlanCallContext(null, upperProj, filter, lowerProj, scan);
  }

  public static boolean checkSameTableScan(DrillScanRelBase scanA, DrillScanRelBase scanB) {
    return scanA.getTable().getQualifiedName().equals(scanB.getTable().getQualifiedName());
  }
}
