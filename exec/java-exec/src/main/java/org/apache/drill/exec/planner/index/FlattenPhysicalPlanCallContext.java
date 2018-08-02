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

import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.common.DrillFilterRelBase;
import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.index.rules.AbstractMatchFunction;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FlattenPhysicalPlanCallContext implements FlattenCallContext {

  /**
   * Filter directly above the root Flatten's project (this filter cannot be pushed down)
   */
  protected FilterPrel filterAboveRootFlatten = null;

  /**
   * Filter below the leaf Flatten (and above the Scan)
   */
  protected FilterPrel filterBelowLeafFlatten = null;

  /**
   * Project that has the Flatten
   */
  protected ProjectPrel projectWithRootFlatten = null;

  /**
   * Project directly above the Scan
   */
  protected ProjectPrel leafProjectAboveScan = null;

  /**
   * project above root flatten
   */
  protected ProjectPrel projectAboveRootFlatten = null;

  /**
   * Scan
   */
  protected ScanPrel scan = null;

  /**
   * 2-level map of Project to another map of Flatten field names to the corresponding RexCall.
   * This 2-level mapping is needed since there could be chain of Project-Project-Project each
   * containing Flatten
   */
  protected LinkedHashMap<RelNode, Map<String, RexCall>> projectToFlattenExprsMap = null;

  /**
   * Map of Project to list of non-Flatten exprs in the Project containing Flatten.
   * This mapping is needed since there could be chain of Project-Project-Project each
   * containing Flatten and non-Flatten exprs
   */
  protected LinkedHashMap<RelNode, List<RexNode>> projectToNonFlattenExprsMap = null;

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
   * List of filter exprs for the leaf filter that is directly above the Scan
   */
  protected List<RexInputRef> leafFilterExprs = null;

  /**
   * Non null root node of the physical call context.
   */
  private final RelNode root;

  public FlattenPhysicalPlanCallContext(
      ProjectPrel upperProject,
      FilterPrel filterAboveRootFlatten,
      ProjectPrel projectWithRootFlatten,
      FilterPrel filterBelowLeafFlatten,
      ProjectPrel leafProjectAboveScan,
      ScanPrel scan) {
    this.projectAboveRootFlatten = upperProject;
    this.filterAboveRootFlatten = filterAboveRootFlatten;
    this.projectWithRootFlatten = projectWithRootFlatten;
    this.filterBelowLeafFlatten = filterBelowLeafFlatten;
    this.leafProjectAboveScan = leafProjectAboveScan;
    this.scan = scan;
    this.projectToFlattenExprsMap = Maps.newLinkedHashMap();
    this.projectToNonFlattenExprsMap = Maps.newLinkedHashMap();

    initializeContext();

    // set the root after the above initialization step
    this.root = getRootInternal();
  }

  public FlattenPhysicalPlanCallContext(
      ProjectPrel upperProject,
      FilterPrel filterAboveRootFlatten,
      ProjectPrel projectWithRootFlatten,
      ScanPrel scan) {
    this(upperProject, filterAboveRootFlatten, projectWithRootFlatten,
        null, // filter below leaf flatten
        null, // leaf project above scan
        scan);
  }

  public FlattenPhysicalPlanCallContext(
      ProjectPrel upperProject,
      FilterPrel filterAboveRootFlatten,
      ProjectPrel projectWithRootFlatten) {
    this(upperProject, filterAboveRootFlatten, projectWithRootFlatten,
        null, // filter below leaf flatten
        null, // leaf project above scan
        null  // scan will be set by initializer
     );
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

//  @Override
  public DrillScanRelBase getScan() {
    return scan;
  }

//  @Override
  public DrillProjectRelBase getProjectAboveRootFlatten() {
    return projectAboveRootFlatten;
  }

  @Override
  public void setFilterBelowLeafFlatten(DrillFilterRelBase filter) {
    this.filterBelowLeafFlatten = (FilterPrel) filter;
  }

  @Override
  public void setLeafProjectAboveScan(DrillProjectRelBase project) {
    this.leafProjectAboveScan = (ProjectPrel) project;
  }

  private RelNode getRootInternal() {
    if (projectAboveRootFlatten != null) {
      return projectAboveRootFlatten;
    } else if (filterAboveRootFlatten != null) {
      return filterAboveRootFlatten;
    } else if (projectWithRootFlatten != null) {
      return projectWithRootFlatten;
    } else if (filterBelowLeafFlatten != null){
      return filterBelowLeafFlatten;
    } else if (leafProjectAboveScan != null) {
      return leafProjectAboveScan;
    } else {
      return scan;
    }
  }

  public RelNode getRoot() {
    return root;
  }

  private void initializeContext() {
    DrillScanRelBase tmpScan = AbstractMatchFunction.initializeContext(this);
    if (this.scan == null) {
      this.scan = (ScanPrel) tmpScan;
    }
  }

  public RelNode buildPhysicalProjectsBottomUpWithoutFlatten(RelNode inputRel,
      RelOptCluster cluster,
      List<ProjectPrel> newProjectList) {

    return FlattenCallContextUtils.buildPhysicalProjectsBottomUpWithoutFlatten(this, inputRel, cluster, newProjectList);

  }

}
