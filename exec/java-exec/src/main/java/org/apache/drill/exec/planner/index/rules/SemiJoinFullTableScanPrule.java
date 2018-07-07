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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.index.FlattenIndexPlanCallContext;
import org.apache.drill.exec.planner.index.IndexLogicalPlanCallContext;
import org.apache.drill.exec.planner.index.IndexPlanUtils;
import org.apache.drill.exec.planner.index.SemiJoinIndexPlanCallContext;
import org.apache.drill.exec.planner.index.generators.common.SemiJoinIndexPlanUtils;
import org.apache.drill.exec.planner.index.generators.common.SemiJoinTransformUtils;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ScanPrel;

public class SemiJoinFullTableScanPrule extends AbstractIndexPrule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SemiJoinFullTableScanPrule.class);

  public static RelOptRule JOIN_FILTER_PROJECT = new SemiJoinFullTableScanPrule(
      RelOptHelper.some(DrillJoinRel.class,
          RelOptHelper.any(AbstractRelNode.class),
          RelOptHelper.some(DrillAggregateRel.class,
                  RelOptHelper.some(DrillProjectRel.class,
                          RelOptHelper.some(DrillFilterRel.class,
                                  RelOptHelper.any(DrillProjectRel.class))))),
  "SemiJoinFullTableScanPrule:Join_Project_Filter_Project", new MatchJSPFP());

  private final MatchFunction<SemiJoinIndexPlanCallContext> match;

  private SemiJoinFullTableScanPrule(RelOptRuleOperand operand,
                                     String description,
                                     MatchFunction<SemiJoinIndexPlanCallContext> match) {
    super(operand, description);
    this.match = match;
  }

  private static class MatchJSPFP extends AbstractMatchFunction<SemiJoinIndexPlanCallContext> {

    @Override
    public boolean match(RelOptRuleCall call) {
      DrillAggregateRel aggRel = call.rel(2);
      DrillProjectRel lowerProject = call.rel(5);

      // if Project does not contain a FLATTEN expression, rule does not apply
      if (!projectHasFlatten(lowerProject, true, null, null)) {
        return false;
      }

      // get the context for the left side of the join
      IndexLogicalPlanCallContext leftContext = IndexPlanUtils.generateContext(call, call.rel(1), logger);

      if (leftContext.scan == null || !checkScan(leftContext.scan)) {
        return false;
      }

      if (aggRel.getAggCallList().size() > 0) {
        return false;
      }

      return true;
    }

    @Override
    public SemiJoinIndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillProjectRel upperProject = call.rel(3);
      final DrillFilterRel filter = call.rel(4);
      final DrillProjectRel rootProjectWithFlatten = call.rel(5);

      final DrillJoinRel join = call.rel(0);
      final DrillAggregateRel distinct = call.rel(2);

      DrillScanRel rightScan = getDescendantScan(rootProjectWithFlatten);

      if (rightScan == null || !checkScan(rightScan)) {
        return null;
      }

      // get the context for the left side of the join
      IndexLogicalPlanCallContext leftContext = IndexPlanUtils.generateContext(call, call.rel(1), logger);

      // Scans on the left and right side of the join should be for the same table
      if (!SemiJoinIndexPlanUtils.checkSameTableScan(leftContext.scan, rightScan)) {
        return null;
      }

      FlattenIndexPlanCallContext rightContext = new FlattenIndexPlanCallContext(call,
          upperProject,
          filter,
          rootProjectWithFlatten,
          rightScan);

      SemiJoinIndexPlanCallContext idxContext = new SemiJoinIndexPlanCallContext(call, join, distinct,
              leftContext, rightContext);
      idxContext.setCoveringIndexPlanApplicable(!projectHasFlatten(leftContext.lowerProject, true, null, null) &&
              !projectHasFlatten(leftContext.upperProject, true, null, null));
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

  private static ScanPrel buildScanPrel(DrillScanRelBase scan) {
    DbGroupScan dbGroupScan = (DbGroupScan) IndexPlanUtils.getGroupScan(scan);
    dbGroupScan = ((DbGroupScan) dbGroupScan.clone(dbGroupScan.getColumns()));
    dbGroupScan.setComplexFilterPushDown(true);
    DrillDistributionTrait partition = IndexPlanUtils.scanIsPartition(dbGroupScan) ?
            DrillDistributionTrait.RANDOM_DISTRIBUTED : DrillDistributionTrait.SINGLETON;

    // add a default collation trait otherwise Calcite runs into a ClassCastException, which at first glance
    // seems like a Calcite bug
    RelTraitSet indexScanTraitSet = scan.getTraitSet().plus(Prel.DRILL_PHYSICAL).
            plus(RelCollationTraitDef.INSTANCE.getDefault()).plus(partition);

    ScanPrel scanPrel = new ScanPrel(scan.getCluster(),
            indexScanTraitSet, dbGroupScan,
            scan.getRowType(), scan.getTable());

    return scanPrel;
  }

  private RelNode getFullPlan(FlattenIndexPlanCallContext indexContext) {
    DrillScanRelBase origScan = indexContext.scan;
    DrillFilterRel leafFilter = (DrillFilterRel) indexContext.getFilterBelowLeafFlatten();
    DrillFilterRel flattenFilter = (DrillFilterRel) indexContext.getFilterAboveRootFlatten();
    DrillProjectRel leafProject = (DrillProjectRel) indexContext.getLeafProjectAboveScan();
    DrillProjectRel projectWithFlatten = (DrillProjectRel) indexContext.getProjectWithRootFlatten();
    DrillProjectRel upperProject = (DrillProjectRel) indexContext.getUpperProject();

    RelNode newRel = buildScanPrel(origScan);
    if (leafProject != null) {
      newRel = SemiJoinIndexPlanUtils.buildProject(newRel, leafProject);
    }
    if (leafFilter != null) {
      newRel = SemiJoinIndexPlanUtils.buildFilter(newRel, leafFilter);
    }

    newRel = indexContext.buildPhysicalProjectsBottomUp(newRel);

    newRel = SemiJoinIndexPlanUtils.buildFilter(newRel, flattenFilter);
    if (upperProject != null) {
      newRel = SemiJoinIndexPlanUtils.buildProject(newRel, upperProject);
    }
    return newRel;
  }

  private void doOnMatch(SemiJoinIndexPlanCallContext indexContext) {
    if (indexContext != null && indexContext.join != null) {
      PlannerSettings ps = PrelUtil.getPlannerSettings(indexContext.call.getPlanner());
      FlattenIndexPlanCallContext context = SemiJoinTransformUtils.transformJoinToSingleTableScan(indexContext, ps.functionImplementationRegistry, logger);
      indexContext.set(context);
      if (context == null) {
        logger.warn("Covering Index Scan cannot be applied as FlattenIndexPlanContext is null");
      } else {
        try {
          RelNode fulltablePlan = getFullPlan(context);
          indexContext.call.transformTo(fulltablePlan);
        } catch (Exception e) {
          logger.warn("Exception while trying to generate covering index plan", e);
        }
      }
    }
  }
}
