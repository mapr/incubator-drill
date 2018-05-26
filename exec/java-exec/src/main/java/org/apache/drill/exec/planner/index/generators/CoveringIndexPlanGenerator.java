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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.index.IndexLogicalPlanCallContext;
import org.apache.drill.exec.planner.index.IndexDescriptor;
import org.apache.drill.exec.planner.index.FunctionalIndexInfo;
import org.apache.drill.exec.planner.index.IndexPlanUtils;
import org.apache.drill.exec.planner.index.SimpleRexRemap;
import org.apache.drill.exec.planner.logical.DrillMergeProjectRule;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillRelFactories;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * Generate a covering index plan that is equivalent to the original plan.
 *
 * This plan will be further optimized by the filter pushdown rule of the Index plugin which should
 * push this filter into the index scan.
 */
public class CoveringIndexPlanGenerator extends AbstractCoveringPlanGenerator {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoveringIndexPlanGenerator.class);
  final protected IndexGroupScan indexGroupScan;
  final protected IndexDescriptor indexDesc;

  // Ideally This functionInfo should be cached along with indexDesc.
  final protected FunctionalIndexInfo functionInfo;

  public CoveringIndexPlanGenerator(IndexLogicalPlanCallContext indexContext,
                                    FunctionalIndexInfo functionInfo,
                                    IndexGroupScan indexGroupScan,
                                    RexNode indexCondition,
                                    RexNode remainderCondition,
                                    RexBuilder builder,
                                    PlannerSettings settings) {
    super(indexContext, indexCondition, remainderCondition, builder, settings);
    this.indexGroupScan = indexGroupScan;
    this.functionInfo = functionInfo;
    this.indexDesc = functionInfo.getIndexDesc();
  }

  @Override
  public RelNode convertChild(final RelNode filter, final RelNode input) throws InvalidRelException {

    if (indexGroupScan == null) {
      logger.error("CoveringIndexPlanGenerator: Null indexgroupScan in CoveringIndexPlanGenerator.convertChild");
      return null;
    }

    ScanPrel indexScanPrel =
            IndexPlanUtils.buildCoveringIndexScan(origScan, indexGroupScan, indexContext, indexDesc);

    RexNode newIndexCondition = null;
    // If remainder condition, then combine the index and remainder conditions. This is a covering plan so we can
    // pushed the entire condition into the index.
    RexNode coveringCondition = IndexPlanUtils.getTotalFilter(indexCondition, remainderCondition, indexScanPrel.getCluster().getRexBuilder());
    newIndexCondition =
            rewriteFunctionalCondition(coveringCondition, indexScanPrel.getRowType(), functionInfo, origScan, builder);

    RelNode indexPlan = getIndexPlan(indexScanPrel, newIndexCondition, builder, functionInfo,
                          origProject, indexContext, origScan, upperProject);
    logger.debug("CoveringIndexPlanGenerator got finalRel {} from origScan {}, original digest {}, new digest {}.",
            indexPlan.toString(), origScan.toString(),
            upperProject==null?indexContext.getFilter().getDigest(): upperProject.getDigest(), indexPlan.getDigest());

    return indexPlan;
  }

  public static ProjectPrel replace(Project topProject, Project bottomProject) {
    final List<RexNode> newProjects =
        RelOptUtil.pushPastProject(topProject.getProjects(), bottomProject);

    // replace the two projects with a combined projection
    if (topProject instanceof DrillProjectRel) {
      ProjectPrel newProjectRel = (ProjectPrel)DrillRelFactories.DRILL_LOGICAL_PROJECT_FACTORY.createProject(
          bottomProject.getInput(), newProjects,
          topProject.getRowType().getFieldNames());

      return newProjectRel;
    } else {
      final RelNode child = bottomProject.getInput();
      final RelOptCluster cluster = child.getCluster();
      final RelDataType rowType = RexUtil.createStructType(cluster.getTypeFactory(), newProjects, topProject.getRowType().getFieldNames());
      return new ProjectPrel(cluster, child.getTraitSet().plus(Prel.DRILL_PHYSICAL),
          child, Lists.newArrayList(newProjects), rowType);
    }
  }
}
