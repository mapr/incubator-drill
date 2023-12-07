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

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillSortRel;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.common.OrderedRel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.apache.calcite.rel.RelNode;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class IndexLogicalPlanCallContext implements IndexCallContext {
  /**
   * These are inputs to the index planner rules
   */
  final public RelOptRuleCall call;
  final public DrillSortRel sort;
  final public DrillProjectRel upperProject;
  final public DrillFilterRel filter;
  final public DrillProjectRel lowerProject;
  final public DrillScanRel scan;
  final public String indexHint;
  final public DrillParseContext defaultParseContext;


  /**
   * The following are properties identified while doing the index analysis
   */
  public Set<LogicalExpression> leftOutPathsInFunctions;
  public IndexableExprMarker origMarker;
  public List<LogicalExpression> sortExprs;
  public RexNode origPushedCondition;
  public RexNode indexCondition;
  public RexNode remainderCondition;
  public boolean isValidIndexHint;
  public double totalRows = 0;
  public double filterRows = 0;


  public IndexLogicalPlanCallContext(RelOptRuleCall call,
                       DrillProjectRel capProject,
                       DrillFilterRel filter,
                       DrillProjectRel project,
                       DrillScanRel scan) {
    this(call, null, capProject, filter, project, scan);
  }

  public IndexLogicalPlanCallContext(RelOptRuleCall call,
      DrillSortRel sort,
      DrillProjectRel capProject,
      DrillFilterRel filter,
      DrillProjectRel project,
      DrillScanRel scan) {
    this.call = call;
    this.sort = sort;
    this.upperProject = capProject;
    this.filter = filter;
    this.lowerProject = project;
    this.scan = scan;
    this.indexHint = scan == null ? null : ((DbGroupScan)this.scan.getGroupScan()).getIndexHint();
    this.defaultParseContext = new DrillParseContext(PrelUtil.getPlannerSettings(getCall().rel(0).getCluster()));
  }

  public IndexLogicalPlanCallContext(RelOptRuleCall call,
                       DrillSortRel sort,
                       DrillProjectRel project,
                       DrillScanRel scan) {
    this.call = call;
    this.sort = sort;
    this.upperProject = null;
    this.filter = null;
    this.lowerProject = project;
    this.scan = scan;
    this.indexHint = ((DbGroupScan)this.scan.getGroupScan()).getIndexHint();
    this.defaultParseContext = new DrillParseContext(PrelUtil.getPlannerSettings(getCall().rel(0).getCluster()));
  }

  @Override
  public DbGroupScan getGroupScan() {
    return (DbGroupScan) scan.getGroupScan();
  }

  @Override
  public DrillScanRelBase getScan() {
    return scan;
  }


  @Override
  public RelCollation getCollation() {
    if (sort != null) {
      return sort.getCollation();
    }
    return null;
  }

  @Override
  public boolean hasLowerProject() {
    return lowerProject != null;
  }

  @Override
  public boolean hasUpperProject() {
    return upperProject != null;
  }

  @Override
  public RelOptRuleCall getCall() {
    return call;
  }

  @Override
  public Set<LogicalExpression> getLeftOutPathsInFunctions() {
    return leftOutPathsInFunctions;
  }

  @Override
  public DrillFilterRel getFilter() {
    return filter;
  }

  @Override
  public IndexableExprMarker getOrigMarker() {
    return origMarker;
  }

  @Override
  public List<LogicalExpression> getSortExprs() {
    return sortExprs;
  }

  @Override
  public DrillProjectRelBase getLowerProject() {
    return lowerProject;
  }

  @Override
  public DrillProjectRelBase getUpperProject() {
    return upperProject;
  }

  @Override
  public void setLeftOutPathsInFunctions(Set<LogicalExpression> exprs) {
    leftOutPathsInFunctions = exprs;
  }

  @Override
  public List<SchemaPath> getScanColumns() {
    return scan.getColumns();
  }

  @Override
  public RexNode getFilterCondition() {
    return filter.getCondition();
  }

  @Override
  public RexNode getOrigCondition() {
    return origPushedCondition;
  }

  @Override
  public OrderedRel getSort() {
    return sort;
  }

  @Override
  public void createSortExprs() {
    sortExprs = Lists.newArrayList();
  }

  @Override
  public RelNode getExchange() { return null; }

  @Override
  public List<DistributionField> getDistributionFields() { return Collections.emptyList(); }

  @Override
  public DrillParseContext getDefaultParseContext() {
    return defaultParseContext;
  }
}
