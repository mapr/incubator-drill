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
package org.apache.drill.exec.planner.index;

import static org.apache.drill.exec.planner.physical.Prel.DRILL_PHYSICAL;


import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.DrillFilterRel;

public class SemiJoinIndexPlanCallContext {
  public final RelOptRuleCall call;
  public final IndexLogicalPlanCallContext leftSide;
  public final FlattenIndexPlanCallContext rightSide;
  public final DrillJoinRel join;
  public final DrillAggregateRel distinct;
  private boolean isCoveringIndexPlanApplicable = true;


  private FlattenIndexPlanCallContext converingIndexContext;

  public SemiJoinIndexPlanCallContext(RelOptRuleCall call,
                                      DrillJoinRel join,
                                      DrillAggregateRel distinct,
                                      IndexLogicalPlanCallContext leftSide,
                                      FlattenIndexPlanCallContext rightSide) {
    this.call = call;
    this.join = join;
    this.distinct = distinct;
    this.leftSide = leftSide;
    this.rightSide = rightSide;
  }

  public void set(FlattenIndexPlanCallContext coveringIndexContext) {
    this.converingIndexContext = coveringIndexContext;
  }

  public FlattenIndexPlanCallContext getCoveringIndexContext() {
    return converingIndexContext;
  }

  public void setCoveringIndexPlanApplicable(boolean isApplicable) {
    this.isCoveringIndexPlanApplicable = isApplicable;
  }

  public boolean isCoveringIndexPlanApplicable() {
    return this.isCoveringIndexPlanApplicable;
  }

  /**
   * Generate a FlattenPhysicalPlanCallContext for the left side of the SemiJoin
   * @return
   */
  public FlattenPhysicalPlanCallContext getLeftFlattenPhysicalPlanCallContext() {
    // 1. start with the leaf node  (scan which will be converted to RestrictedScan)
    DrillScanRel scan = leftSide.scan;
    Preconditions.checkNotNull(scan);
    Preconditions.checkArgument(scan.getGroupScan() instanceof DbGroupScan);

    DbGroupScan restrictedScan = ((DbGroupScan) scan.getGroupScan()).getRestrictedScan(scan.getColumns());
    restrictedScan.setComplexFilterPushDown(true);
    ScanPrel scanPrel = new ScanPrel(scan.getCluster(), scan.getTraitSet().plus(DRILL_PHYSICAL),
            restrictedScan, scan.getRowType(), scan.getTable());

    Prel inputPrel = scanPrel;
    ProjectPrel lowerProjectPrel = null;
    FilterPrel  filterPrel = null;
    ProjectPrel upperProjectPrel = null;

    // 2. process the lowerProject if any
    if (leftSide.hasLowerProject()) {
      DrillProjectRel lowerProject = (DrillProjectRel) leftSide.getLowerProject();
      lowerProjectPrel = new ProjectPrel(lowerProject.getCluster(),
          lowerProject.getTraitSet().plus(DRILL_PHYSICAL),
          inputPrel, lowerProject.getProjects(), lowerProject.getRowType());

      inputPrel = lowerProjectPrel;
    }

    // 3. process the filter if any
    if (leftSide.filter != null) {
      DrillFilterRel filter = (DrillFilterRel) leftSide.filter;
      filterPrel = new FilterPrel(filter.getCluster(), filter.getTraitSet().plus(DRILL_PHYSICAL),
          inputPrel, filter.getCondition());

      inputPrel = filterPrel;
    }

    // 4. process the upperProject if any
    if (leftSide.hasUpperProject()) {
      DrillProjectRel upperProject = (DrillProjectRel) leftSide.getUpperProject();
      upperProjectPrel = new ProjectPrel(upperProject.getCluster(),
          upperProject.getTraitSet().plus(DRILL_PHYSICAL),
          inputPrel, upperProject.getProjects(), upperProject.getRowType());
    }

    // create a basic physical context
    FlattenPhysicalPlanCallContext flattenPhysicalContext =
        new FlattenPhysicalPlanCallContext(upperProjectPrel, filterPrel, lowerProjectPrel, scanPrel);

    return flattenPhysicalContext;
  }

}
