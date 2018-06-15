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

import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.common.DrillFilterRelBase;
import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;

public class FlattenPhysicalPlanCallContext implements FlattenCallContext {

  /**
   * Filter directly above the Flatten's project (this filter cannot be pushed down)
   */
  protected FilterPrel filterAboveFlatten = null;

  /**
   * Filter below the Flatten (and above the Scan)
   */
  protected FilterPrel filterBelowFlatten = null;

  /**
   * Project that has the Flatten
   */
  protected ProjectPrel projectWithFlatten = null;

  /**
   * Project directly above the Scan
   */
  protected ProjectPrel leafProjectAboveScan = null;


  /**
   * project above flatten
   */
  protected ProjectPrel projectAboveFlatten = null;

  /**
   * Scan
   */
  protected ScanPrel scan = null;


  /**
   * Map of Flatten field names to the corresponding RexCall
   */
  protected Map<String, RexCall> flattenMap = null;

  /**
   * List of the non-Flatten expressions in the Project containing Flatten
   */
  protected List<RexNode> nonFlattenExprs = null;

  /**
   * List of other relevant expressions in the leaf Project above Scan
   */
  protected List<RexNode> relevantExprsInLeafProject = null;

  /**
   * Placeholder for individual filter expressions referencing Flatten output.
   * For instance, suppose Flatten output is 'f', and the filter references f.b < 10, then the index planning
   * rule will create an ITEM expression representing this condition.
   */
  protected List<RexNode> filterExprsReferencingFlatten = null;

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
      FilterPrel filterAboveFlatten,
      ProjectPrel projectWithFlatten,
      FilterPrel filterBelowFlatten,
      ProjectPrel leafProjectAboveScan,
      ScanPrel scan,
      Map<String, RexCall> flattenMap,
      List<RexNode> nonFlattenExprs) {
    this.projectAboveFlatten = upperProject;
    this.filterAboveFlatten = filterAboveFlatten;
    this.filterBelowFlatten = filterBelowFlatten;
    this.projectWithFlatten = projectWithFlatten;
    this.leafProjectAboveScan = leafProjectAboveScan;
    this.scan = scan;
    this.flattenMap = flattenMap;
    this.nonFlattenExprs = nonFlattenExprs;
    this.root = getRootInternal();
  }

  @Override
  public Map<String, RexCall> getFlattenMap() {
    return flattenMap;
  }

  @Override
  public List<RexNode> getNonFlattenExprs() {
    return nonFlattenExprs;
  }

  @Override
  public DrillFilterRelBase getFilterAboveFlatten() {
    return filterAboveFlatten;
  }

  @Override
  public DrillFilterRelBase getFilterBelowFlatten() {
    return filterBelowFlatten;
  }

  @Override
  public void setFilterExprsReferencingFlatten(List<RexNode> exprList) {
    this.filterExprsReferencingFlatten = exprList;
  }

  @Override
  public DrillProjectRelBase getProjectWithFlatten() {
    return projectWithFlatten;
  }

  @Override
  public List<RexNode> getFilterExprsReferencingFlatten() {
    return filterExprsReferencingFlatten;
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
  public DrillProjectRelBase getProjectAboveFlatten() {
    return projectAboveFlatten;
  }

  private RelNode getRootInternal() {
    if (projectAboveFlatten != null) {
      return projectAboveFlatten;
    } else if (filterAboveFlatten != null) {
      return filterAboveFlatten;
    } else if (projectWithFlatten != null) {
      return projectWithFlatten;
    } else if (filterBelowFlatten != null){
      return filterBelowFlatten;
    } else if (leafProjectAboveScan != null) {
      return leafProjectAboveScan;
    } else {
      return scan;
    }
  }

  public RelNode getRoot() {
    return root;
  }
}
