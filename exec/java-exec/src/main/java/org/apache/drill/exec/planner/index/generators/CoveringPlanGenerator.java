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
package org.apache.drill.exec.planner.index.generators;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.index.IndexPlanUtils;
import org.apache.drill.exec.planner.index.SemiJoinIndexPlanCallContext;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.calcite.rel.RelNode;


/**
 * Generate a covering index plan that is equivalent to the original plan.
 *
 * This plan will be further optimized by the filter pushdown rule of the Index plugin which should
 * push this filter into the index scan.
 */
public class CoveringPlanGenerator extends AbstractCoveringPlanGenerator  {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoveringPlanGenerator.class);
  public final SemiJoinIndexPlanCallContext semiJoinIndexPlanCallContext;

  public CoveringPlanGenerator(SemiJoinIndexPlanCallContext indexContext,
                               RexNode indexCondition,
                               RexBuilder builder,
                               PlannerSettings settings) {
    super(indexContext.getConveringIndexContext(), indexCondition, null, builder, settings);
    semiJoinIndexPlanCallContext = indexContext;
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

  @Override
  public RelNode convertChild(final RelNode filter, final RelNode input) throws InvalidRelException {
    ScanPrel primaryScanPrel = buildScanPrel(origScan);

    RelNode fullTablePlan = getIndexPlan(primaryScanPrel, indexCondition, builder,
            null, origProject, indexContext, origScan, upperProject);

    logger.debug("CoveringPlanGenerator got finalRel {} from origScan {}, original digest {}, new digest {}.",
            fullTablePlan.toString(), origScan.toString(),
            upperProject==null?indexContext.getFilter().getDigest(): upperProject.getDigest(), fullTablePlan.getDigest());
    return fullTablePlan;
  }
}
