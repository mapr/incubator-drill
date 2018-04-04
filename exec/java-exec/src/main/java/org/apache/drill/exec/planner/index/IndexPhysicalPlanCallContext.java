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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractDbGroupScan;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.common.OrderedRel;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.physical.ExchangePrel;
import org.apache.drill.exec.planner.physical.HashToRandomExchangePrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class IndexPhysicalPlanCallContext implements IndexCallContext {

  final public RelOptRuleCall call;
  final public OrderedRel sort;
  final public ProjectPrel upperProject;
  final public FilterPrel filter;
  final public ProjectPrel lowerProject;
  final public ScanPrel scan;
  final public ExchangePrel exch;
  final public String indexHint;
  final public DrillParseContext defaultParseContext;

  public Set<LogicalExpression> leftOutPathsInFunctions;

  public IndexableExprMarker origMarker;
  public List<LogicalExpression> sortExprs;

  public RexNode origPushedCondition;

  public IndexPhysicalPlanCallContext(RelOptRuleCall call,
                                      ProjectPrel capProject,
                                      FilterPrel filter,
                                      ProjectPrel project,
                                      ScanPrel scan) {
    this(call, null, capProject, filter, project, scan, null);
  }

  public IndexPhysicalPlanCallContext(RelOptRuleCall call,
                                      OrderedRel sort,
                                      ProjectPrel capProject,
                                      FilterPrel filter,
                                      ProjectPrel project,
                                      ScanPrel scan, ExchangePrel exch) {
    this.call = call;
    this.sort = sort;
    this.upperProject = capProject;
    this.filter = filter;
    this.lowerProject = project;
    this.scan = scan;
    this.exch = exch;
    this.indexHint = ((DbGroupScan)this.scan.getGroupScan()).getIndexHint();
    this.defaultParseContext = new DrillParseContext(PrelUtil.getPlannerSettings(getCall().rel(0).getCluster()));
  }

  public IndexPhysicalPlanCallContext(RelOptRuleCall call,
                                      OrderedRel sort,
                                      ProjectPrel project,
                                      ScanPrel scan, ExchangePrel exch) {
    this.call = call;
    this.sort = sort;
    this.upperProject = null;
    this.filter = null;
    this.lowerProject = project;
    this.scan = scan;
    this.exch = exch;
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
  public List<RelCollation> getCollationList() {
    if (sort != null) {
      return sort.getCollationList();
    }
    return null;
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
  public RelNode getFilter() {
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
    return ((AbstractDbGroupScan)scan.getGroupScan()).getColumns();
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
  public RelNode getExchange() {
    return exch;
  }

  @Override
  public void createSortExprs() {
    sortExprs = Lists.newArrayList();
  }

  @Override
  public List<DistributionField> getDistributionFields() {
    if (exch != null) {
      return ((HashToRandomExchangePrel) exch).getFields();
    } else {
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public DrillParseContext getDefaultParseContext() {
    return defaultParseContext;
  }
}
