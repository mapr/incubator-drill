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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.index.FlattenIndexPlanCallContext;
import org.apache.drill.exec.planner.index.FunctionalIndexHelper;
import org.apache.drill.exec.planner.index.FunctionalIndexInfo;
import org.apache.drill.exec.planner.index.IndexCallContext;
import org.apache.drill.exec.planner.index.IndexPlanUtils;
import org.apache.drill.exec.planner.logical.DrillMergeProjectRule;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.Prule;
import org.apache.drill.exec.planner.physical.ScanPrel;

import java.util.List;

public abstract class AbstractCoveringPlanGenerator extends AbstractIndexPlanGenerator {

  public AbstractCoveringPlanGenerator(IndexCallContext indexContext,
                                    RexNode indexCondition,
                                    RexNode remainderCondition,
                                    RexBuilder builder,
                                    PlannerSettings settings) {
    super(indexContext, indexCondition, remainderCondition, builder, settings);
  }

  protected static RelNode getIndexPlan(ScanPrel scanPrel, RexNode indexCondition, RexBuilder builder,
                                        FunctionalIndexInfo functionInfo, DrillProjectRelBase origProject,
                                        IndexCallContext indexContext, DrillScanRelBase origScan,
                                        DrillProjectRelBase upperProject) {

    // build collation for filter
    RelTraitSet indexFilterTraitSet = scanPrel.getTraitSet();

    FilterPrel indexFilterPrel = new FilterPrel(scanPrel.getCluster(), indexFilterTraitSet,
            scanPrel, indexCondition);

    RelNode currentInputPrel = indexFilterPrel;

    ProjectPrel indexProjectPrel = null;
    if (origProject != null) {
      if (indexContext instanceof FlattenIndexPlanCallContext) {
        FlattenIndexPlanCallContext flattenContext = ((FlattenIndexPlanCallContext) indexContext);

        Preconditions.checkArgument(flattenContext.getProjectWithRootFlatten() == origProject); // temporary check

        currentInputPrel = flattenContext.buildPhysicalProjectsBottomUpWithoutFlatten(currentInputPrel,
            origScan.getCluster());

      } else {
        RelCollation collation = IndexPlanUtils.buildCollationProject(IndexPlanUtils.getProjects(origProject), null,
                origScan, functionInfo, indexContext);
        indexProjectPrel = new ProjectPrel(origScan.getCluster(), indexFilterTraitSet.plus(collation),
                indexFilterPrel, IndexPlanUtils.getProjects(origProject), origProject.getRowType());
      }
    }

    RelNode finalRel;
    if (indexProjectPrel != null) {
      finalRel = indexProjectPrel;
    } else {
      finalRel = indexFilterPrel;
    }

    if (upperProject != null) {
      RelCollation newCollation = null;
      if (functionInfo != null) {
        newCollation =
                IndexPlanUtils.buildCollationProject(IndexPlanUtils.getProjects(upperProject), origProject,
                        origScan, functionInfo, indexContext);
      }

      ProjectPrel cap = new ProjectPrel(upperProject.getCluster(),
              newCollation==null?finalRel.getTraitSet() : finalRel.getTraitSet().plus(newCollation),
              finalRel, IndexPlanUtils.getProjects(upperProject), upperProject.getRowType());

      if (functionInfo != null && functionInfo.hasFunctional()) {
        //if there is functional index field, then a rewrite may be needed in upperProject/indexProject
        //merge upperProject with indexProjectPrel(from origProject) if both exist,
        ProjectPrel newProject = cap;
        if (indexProjectPrel != null) {
          newProject = (ProjectPrel) DrillMergeProjectRule.replace(newProject, indexProjectPrel);
        }
        // then rewrite functional expressions in new project.
        List<RexNode> newProjects = Lists.newArrayList();
        DrillParseContext parseContxt = new DrillParseContext(PrelUtil.getPlannerSettings(newProject.getCluster()));
        for(RexNode projectRex: newProject.getProjects()) {
          RexNode newRex = IndexPlanUtils.rewriteFunctionalRex(indexContext, parseContxt, null, origScan, projectRex, scanPrel.getRowType(), functionInfo);
          newProjects.add(newRex);
        }

        ProjectPrel rewrittenProject = new ProjectPrel(newProject.getCluster(),
                newCollation==null? newProject.getTraitSet() : newProject.getTraitSet().plus(newCollation),
                indexFilterPrel, newProjects, newProject.getRowType());

        cap = rewrittenProject;
      }

      finalRel = cap;
    }

    if (indexContext.getSort() != null) {
      finalRel = getSortNode(indexContext, finalRel, false,true, true);
      Preconditions.checkArgument(finalRel != null);
    }

    finalRel = Prule.convert(finalRel, finalRel.getTraitSet().plus(Prel.DRILL_PHYSICAL));

    return finalRel;
  }

  /**
   *
   * @param inputIndex
   * @param functionInfo functional index information that may impact rewrite
   * @return
   */
  protected static RexNode rewriteFunctionalCondition(RexNode inputIndex, RelDataType newRowType,
                                                      FunctionalIndexInfo functionInfo, DrillScanRelBase origScan, RexBuilder builder) {
    if (!functionInfo.hasFunctional()) {
      return inputIndex;
    }
    return FunctionalIndexHelper.convertConditionForIndexScan(inputIndex,
            origScan, newRowType, builder, functionInfo);
  }
}
