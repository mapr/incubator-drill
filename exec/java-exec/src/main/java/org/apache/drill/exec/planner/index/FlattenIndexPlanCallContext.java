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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.common.DrillFilterRelBase;
import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.index.rules.AbstractMatchFunction;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.ProjectPrel;

import org.apache.drill.shaded.guava.com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.drill.exec.planner.physical.Prel.DRILL_PHYSICAL;

public class FlattenIndexPlanCallContext extends IndexLogicalPlanCallContext
    implements FlattenCallContext {

  /**
   * Filter directly above the root Flatten's project (this filter cannot be pushed down)
   */
  protected DrillFilterRelBase filterAboveRootFlatten = null;

  /**
   * Filter below the the leaf Flatten (and above the Scan)
   */
  protected DrillFilterRelBase filterBelowLeafFlatten = null;

  /**
   * Project that has the Flatten
   */
  protected DrillProjectRelBase projectWithRootFlatten = null;

  /**
   * Project directly above the Scan
   */
  protected DrillProjectRelBase leafProjectAboveScan = null;

  /**
   * 2-level map of Project to another map of Flatten field names to the corresponding RexCall.
   * This 2-level mapping is needed since there could be chain of Project-Project-Project each
   * containing Flatten
   */
  LinkedHashMap<RelNode, Map<String, RexCall>> projectToFlattenExprsMap;

  /**
   * Map of Project to list of non-Flatten exprs in the Project containing Flatten.
   * This mapping is needed since there could be chain of Project-Project-Project each
   * containing Flatten and non-Flatten exprs
   */
  LinkedHashMap<RelNode, List<RexNode>> projectToNonFlattenExprsMap;

  /**
   * List of other relevant expressions in the leaf Project above Scan
   */
  protected List<RexNode> relevantExprsInLeafProject = null;

  /**
   * Placeholder for individual filter expressions referencing Flatten output.
   * For instance, suppose Flatten output is 'f', and the filter references f.b < 10, f.c > 20 then the index planning
   * rule will create an ITEM expression representing this condition. This map holds a mapping from
   * 'f' to the list of references f.b, f.c.
   */
  protected Map<String, List<RexNode>> filterExprsReferencingFlattenMap = null;

  /**
   * List of input references for the leaf filter that is directly above the Scan
   */
  protected List<RexInputRef> leafFilterExprs = null;

  /**
   * Is valid Flatten context.
   */
  public final boolean isValid;

  public FlattenIndexPlanCallContext(RelOptRuleCall call,
      DrillProjectRel upperProject,
      DrillFilterRel filterAboveRootFlatten,
      DrillProjectRel projectWithRootFlatten,
      DrillScanRel scan) {
    super(call,
        null, // no Sort
        upperProject,
        filterAboveRootFlatten,
        projectWithRootFlatten, // same as lowerProject
        scan);
    this.filterAboveRootFlatten = filterAboveRootFlatten;
    this.projectWithRootFlatten = projectWithRootFlatten;
    this.projectToFlattenExprsMap = Maps.newLinkedHashMap();
    this.projectToNonFlattenExprsMap = Maps.newLinkedHashMap();

    isValid = initializeContext();
  }

  @Override
  public LinkedHashMap<RelNode, Map<String, RexCall>> getProjectToFlattenMapForAllProjects() {
    return projectToFlattenExprsMap;
  }

  @Override
  public Map<String, RexCall> getFlattenMapForProject(RelNode project) {
    return projectToFlattenExprsMap.get(project);
  }

  @Override
  public LinkedHashMap<RelNode, List<RexNode>> getNonFlattenExprsMapForAllProjects() {
    return projectToNonFlattenExprsMap;
  }

  @Override
  public List<RexNode> getNonFlattenExprsForProject(RelNode project) {
    return projectToNonFlattenExprsMap.get(project);
  }

  @Override
  public DrillFilterRelBase getFilterAboveRootFlatten() {
    return filterAboveRootFlatten;
  }

  @Override
  public DrillFilterRelBase getFilterBelowLeafFlatten() {
    return filterBelowLeafFlatten;
  }

  @Override
  public void setFilterExprsReferencingFlatten(Map<String, List<RexNode>> exprsReferencingFlattenMap) {
    this.filterExprsReferencingFlattenMap = exprsReferencingFlattenMap;
  }

  @Override
  public DrillProjectRelBase getProjectWithRootFlatten() {
    return projectWithRootFlatten;
  }

  @Override
  public Map<String, List<RexNode>> getFilterExprsReferencingFlatten() {
    return filterExprsReferencingFlattenMap;
  }

  @Override
  public DrillProjectRelBase getLeafProjectAboveScan() {
    return leafProjectAboveScan;
  }

  @Override
  public void setRelevantExprsInLeafProject(List<RexNode> exprList) {
    this.relevantExprsInLeafProject = exprList;
  }

  @Override
  public List<RexNode> getRelevantExprsInLeafProject() {
    return relevantExprsInLeafProject;
  }

  @Override
  public void setExprsForLeafFilter(List<RexInputRef> exprList) {
    this.leafFilterExprs = exprList;
  }

  @Override
  public List<RexInputRef> getExprsForLeafFilter() {
    return leafFilterExprs;
  }

  @Override
  public DrillScanRelBase getScan() {
    return scan;
  }

  @Override
  public DrillProjectRelBase getProjectAboveRootFlatten() {
    return this.upperProject;
  }

  @Override
  public void setFilterBelowLeafFlatten(DrillFilterRelBase filter) {
    this.filterBelowLeafFlatten = filter;
  }

  @Override
  public void setLeafProjectAboveScan(DrillProjectRelBase project) {
    this.leafProjectAboveScan = project;
  }

  private boolean initializeContext() {
    DrillScanRelBase scan = AbstractMatchFunction.initializeContext(this);
    return scan != null ? true : false;
  }

  public RelNode buildPhysicalProjectsBottomUp(RelNode inputRel) {
    List<RelNode> projectList = new ArrayList<RelNode>(getProjectToFlattenMapForAllProjects().keySet());
    Collections.reverse(projectList);

    RelNode currentInputRel = inputRel;

    for (RelNode n : projectList) {
      DrillProjectRelBase currentProject = (DrillProjectRelBase) n;

      final ProjectPrel tmpProject = new ProjectPrel(currentInputRel.getCluster(),
          currentInputRel.getTraitSet().plus(DRILL_PHYSICAL), currentInputRel,
          currentProject.getProjects(), currentProject.getRowType());
      currentInputRel = tmpProject;
    }
    return currentInputRel;
  }

  public RelNode buildPhysicalProjectsBottomUpWithoutFlatten(RelNode inputRel,
      RelOptCluster cluster) {

    return FlattenCallContextUtils.buildPhysicalProjectsBottomUpWithoutFlatten(this, inputRel, cluster, null);

  }

}
