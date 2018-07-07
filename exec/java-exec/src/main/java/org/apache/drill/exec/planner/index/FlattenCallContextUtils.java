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

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.physical.ProjectPrel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.drill.exec.planner.physical.Prel.DRILL_PHYSICAL;

public class FlattenCallContextUtils {

  public static RelNode buildPhysicalProjectsBottomUpWithoutFlatten(
      FlattenCallContext flattenCallContext,
      RelNode inputRel,
      RelOptCluster cluster,
      List<ProjectPrel> newProjectList) {

    RelNode currentInputRel = inputRel;

    List<RelNode> projectList =
        new ArrayList<>(flattenCallContext.getProjectToFlattenMapForAllProjects().keySet());
    Collections.reverse(projectList);

    for (RelNode node : projectList) {
      DrillProjectRelBase origProject = (DrillProjectRelBase) node;
      List<RelDataTypeField> origProjFields = origProject.getRowType().getFieldList();
      List<RelDataTypeField> newProjFields = Lists.newArrayList();
      List<Pair<RexNode, String>> projExprList = origProject.getNamedProjects();
      // build the row type for the new Project
      List<RexNode> newProjectExprs = Lists.newArrayList();

      int origFieldIndex = 0;

      for (Pair<RexNode, String> p : projExprList) {
        newProjFields.add(origProjFields.get(origFieldIndex));
        // if this expr is a flatten, only keep the input of flatten.  Note that we cannot drop
        // the expr altogether because the new Project will be added to the same RelSubset as the old Project and
        // the RowType of both should be the same to pass validation checks in the VolcanoPlanner.
        if (flattenCallContext.getFlattenMapForProject(origProject).containsKey(p.right)) {
          newProjectExprs.add(((RexCall)p.left).getOperands().get(0));
        } else {
          newProjectExprs.add(p.left);
        }
        origFieldIndex++;
      }

      RelDataTypeFactory.Builder newProjectFieldTypeBuilder = cluster.getTypeFactory().builder();
      newProjectFieldTypeBuilder.addAll(newProjFields);
      final RelDataType newProjectRowType = newProjectFieldTypeBuilder.build();

      // to be safe, use empty collation since there is no guarantee that flatten pushdown
      // to index will allow collation trait
      // TODO: this may change in the future if we determine otherwise
      final RelCollation collation = RelCollations.EMPTY;

      currentInputRel = new ProjectPrel(cluster,
          currentInputRel.getTraitSet().plus(collation).plus(DRILL_PHYSICAL),
          currentInputRel, newProjectExprs, newProjectRowType);

      if (newProjectList != null) {
        newProjectList.add((ProjectPrel)currentInputRel);
      }

    }
    return currentInputRel;
  }

}
