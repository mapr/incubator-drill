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
package org.apache.drill.exec.store.mapr.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.index.FlattenPhysicalPlanCallContext;
import org.apache.drill.exec.planner.index.generators.common.FlattenConditionUtils;
import org.apache.drill.exec.planner.index.rules.AbstractMatchFunction;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.hbase.HBaseScanSpec;
import org.apache.drill.exec.store.mapr.db.binary.BinaryTableGroupScan;
import org.apache.drill.exec.store.mapr.db.binary.MapRDBFilterBuilder;
import org.apache.drill.exec.store.mapr.db.json.JsonConditionBuilder;
import org.apache.drill.exec.store.mapr.db.json.JsonScanSpec;
import org.apache.drill.exec.store.mapr.db.json.JsonTableGroupScan;
import org.apache.drill.exec.store.mapr.db.json.RestrictedJsonTableGroupScan;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;


public abstract class MapRDBPushFilterIntoScan extends StoragePluginOptimizerRule {

  private MapRDBPushFilterIntoScan(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  public static final StoragePluginOptimizerRule FILTER_ON_SCAN = new MapRDBPushFilterIntoScan(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)), "MapRDBPushFilterIntoScan:Filter_On_Scan") {

    @Override
    public void onMatch(RelOptRuleCall call) {
      final FilterPrel filter = call.rel(0);
      final ScanPrel scan = call.rel(1);

      final RexNode condition = filter.getCondition();

      if (scan.getGroupScan() instanceof BinaryTableGroupScan) {
        BinaryTableGroupScan groupScan = (BinaryTableGroupScan)scan.getGroupScan();
        doPushFilterIntoBinaryGroupScan(call, filter, null, scan, groupScan, condition);
      } else {
        assert(scan.getGroupScan() instanceof JsonTableGroupScan);
        JsonTableGroupScan groupScan = (JsonTableGroupScan)scan.getGroupScan();
        doPushFilterIntoJsonGroupScan(call, filter, null, scan, groupScan, condition);
      }
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final ScanPrel scan = (ScanPrel) call.rel(1);
      if (scan.getGroupScan() instanceof BinaryTableGroupScan ||
          scan.getGroupScan() instanceof JsonTableGroupScan) {
        return super.matches(call);
      }
      return false;
    }
  };

  // public static final StoragePluginOptimizerRule FILTER_ON_PROJECT = new MapRDBPushFilterIntoScan(RelOptHelper.some(FilterPrel.class, RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(ScanPrel.class))), "MapRDBPushFilterIntoScan:Filter_On_Project") {
  public static final StoragePluginOptimizerRule FILTER_ON_PROJECT = new MapRDBPushFilterIntoScan(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ProjectPrel.class)), "MapRDBPushFilterIntoScan:Filter_On_Project") {

    @Override
    public void onMatch(RelOptRuleCall call) {
      final FilterPrel filter = call.rel(0);
      ProjectPrel project = call.rel(1);
      final ScanPrel scan = (ScanPrel) getDescendantScan(project);
      prepareContextAndPushDownFilter(call, scan, project, filter);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final ProjectPrel project = call.rel(1);
      RelNode descendantRel = getDescendantScan(project);
      if ( descendantRel == null || ! (descendantRel instanceof ScanPrel) ) {
        return false;
      }
      final ScanPrel scan = (ScanPrel) descendantRel;
      if (scan.getGroupScan() instanceof BinaryTableGroupScan ||
          scan.getGroupScan() instanceof JsonTableGroupScan) {
        return super.matches(call);
      }
      return false;
    }
  };

  protected void doPushFilterIntoJsonGroupScan(RelOptRuleCall call,
                                               FilterPrel filter, final ProjectPrel project, ScanPrel scan,
                                               JsonTableGroupScan groupScan, RexNode condition) {
    this.doPushFilterIntoJsonGroupScan(call, filter, project, scan, groupScan, ImmutableList.of(condition));
  }

  protected void doPushFilterIntoJsonGroupScan(RelOptRuleCall call,
      FilterPrel filter, final ProjectPrel project, ScanPrel scan,
      JsonTableGroupScan groupScan, List<RexNode> conditions) {

    if (groupScan.isDisablePushdown() // Do not pushdown filter if it is disabled in plugin configuration
        || groupScan.isFilterPushedDown()) { // see below
      /*
       * The rule can get triggered again due to the transformed "scan => filter" sequence
       * created by the earlier execution of this rule when we could not do a complete
       * conversion of Optiq Filter's condition to HBase Filter. In such cases, we rely upon
       * this flag to not do a re-processing of the rule on the already transformed call.
       */
      return;
    }

    LogicalExpression conditionExp;
    boolean allExpressionsConverted = true;
    List<JsonScanSpec> scanSpecs = new ArrayList<>();
    JsonScanSpec newScanSpec = null;
    String functionName = "booleanAnd";

    for (RexNode condition: conditions) {
      try {
        conditionExp = DrillOptiq.toDrill(new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
      } catch (ClassCastException e) {
        // MD-771 bug in DrillOptiq.toDrill() causes filter condition on ITEM operator to throw ClassCastException
        // For such cases, we return without pushdown
        return;
      }
      JsonConditionBuilder jsonConditionBuilder = new JsonConditionBuilder(groupScan, conditionExp);
      allExpressionsConverted = jsonConditionBuilder.isAllExpressionsConverted() && allExpressionsConverted;
      JsonScanSpec scanSpec = jsonConditionBuilder.parseTree();
      if (newScanSpec == null) {
        newScanSpec = scanSpec;
      } else {
        newScanSpec.mergeScanSpec(functionName, scanSpec);
      }
    }

    if (newScanSpec == null) {
      return; // no filter pushdown ==> No transformation.
    }

    final JsonTableGroupScan newGroupsScan = (JsonTableGroupScan) groupScan.clone(newScanSpec);
    newGroupsScan.setFilterPushedDown(true);

    final ScanPrel newScanPrel = new ScanPrel(scan.getCluster(), filter.getTraitSet(), newGroupsScan, scan.getRowType(), scan.getTable());

    // Depending on whether is a project in the middle, assign either scan or copy of project to childRel.
    final RelNode childRel = project == null ? newScanPrel : project.copy(project.getTraitSet(), ImmutableList.of((RelNode)newScanPrel));

    if (allExpressionsConverted) {
        /*
         * Since we could convert the entire filter condition expression into an HBase filter,
         * we can eliminate the filter operator altogether.
         */
      call.transformTo(childRel);
    } else {
      call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of(childRel)));
    }
  }

  protected void doPushFilterIntoBinaryGroupScan(final RelOptRuleCall call,
                                                 final FilterPrel filter,
                                                 final ProjectPrel project,
                                                 final ScanPrel scan,
                                                 final BinaryTableGroupScan groupScan,
                                                 final RexNode condition) {

    if (groupScan.isFilterPushedDown()) {
      /*
       * The rule can get triggered again due to the transformed "scan => filter" sequence
       * created by the earlier execution of this rule when we could not do a complete
       * conversion of Optiq Filter's condition to HBase Filter. In such cases, we rely upon
       * this flag to not do a re-processing of the rule on the already transformed call.
       */
      return;
    }

    final LogicalExpression conditionExp = DrillOptiq.toDrill(new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
    final MapRDBFilterBuilder maprdbFilterBuilder = new MapRDBFilterBuilder(groupScan, conditionExp);
    final HBaseScanSpec newScanSpec = maprdbFilterBuilder.parseTree();
    if (newScanSpec == null) {
      return; //no filter pushdown ==> No transformation.
    }

    // Pass tableStats from old groupScan so we do not go and fetch stats (an expensive operation) again from MapR DB client.
    final BinaryTableGroupScan newGroupsScan =
        new BinaryTableGroupScan(groupScan.getUserName(), groupScan.getStoragePlugin(),
            groupScan.getFormatPlugin(), newScanSpec, groupScan.getColumns(),
            groupScan.getTableStats(), groupScan.getMetadataProvider());
    newGroupsScan.setFilterPushedDown(true);

    final ScanPrel newScanPrel = new ScanPrel(scan.getCluster(), filter.getTraitSet(), newGroupsScan, scan.getRowType(), scan.getTable());

    // Depending on whether is a project in the middle, assign either scan or copy of project to childRel.
    final RelNode childRel = project == null ? newScanPrel : project.copy(project.getTraitSet(), ImmutableList.of((RelNode)newScanPrel));

    if (maprdbFilterBuilder.isAllExpressionsConverted()) {
        /*
         * Since we could convert the entire filter condition expression into an HBase filter,
         * we can eliminate the filter operator altogether.
         */
      call.transformTo(childRel);
    } else {
      call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of(childRel)));
    }
  }

  protected void prepareContextAndPushDownFilter(RelOptRuleCall call, ScanPrel scan,
                                                 ProjectPrel projectWithFlatten, FilterPrel filterAboveFlatten)  {

    RexNode condition = null;
    List<RexNode> conditions = new ArrayList<>();

    // check if this filter-on-project is part of a non-covering index plan
    if (scan.getGroupScan() instanceof RestrictedJsonTableGroupScan) {
      // check if filter is referencing Flatten expressions from the child Project
      if (AbstractMatchFunction.projectHasFlatten(projectWithFlatten, false, null, null)) {
        RexBuilder builder = filterAboveFlatten.getCluster().getRexBuilder();
        FlattenConditionUtils.ComposedConditionInfo cInfo =
                new FlattenConditionUtils.ComposedConditionInfo(builder);

        FlattenPhysicalPlanCallContext flattenContext = new FlattenPhysicalPlanCallContext(
            null, // upper project
            filterAboveFlatten,
            projectWithFlatten,
            scan);

        FlattenConditionUtils.composeConditions(flattenContext, builder, cInfo);

        conditions.addAll(cInfo.getflattenConditions());
        conditions.addAll(cInfo.getOtherRemainderConjuncts());
        if (cInfo.getConditionBelowFlatten() != null) {
          conditions.add(cInfo.getConditionBelowFlatten());
        }

        // Create a new Project after dropping the Flatten
        int origFieldIndex = 0;
        List<RelDataTypeField> origProjFields = projectWithFlatten.getRowType().getFieldList();
        List<RelDataTypeField> newProjFields = Lists.newArrayList();
        List<RexNode> newProjExprs = Lists.newArrayList();
        RelDataTypeFactory.FieldInfoBuilder newProjFieldTypeBuilder = scan.getCluster().getTypeFactory().builder();
        Map<String, RexCall> flattenMap = flattenContext.getFlattenMapForProject(projectWithFlatten);

        for (Pair<RexNode, String> p : projectWithFlatten.getNamedProjects()) {
          newProjFields.add(origProjFields.get(origFieldIndex));
          // if this expr is a flatten, only keep the input of flatten.  Note that we cannot drop
          // the expr altogether because the new Project will be added to the same RelSubset as the old Project and
          // the RowType of both should be the same to pass validation checks in the VolcanoPlanner.
          if (flattenMap.containsKey(p.right)) {
            newProjExprs.add(((RexCall)p.left).getOperands().get(0));
          } else {
            newProjExprs.add(p.left);
          }
          origFieldIndex++;
        }

        newProjFieldTypeBuilder.addAll(newProjFields);

        final RelDataType newProjRowType = newProjFieldTypeBuilder.build();

        // assign a new Project
        projectWithFlatten = new ProjectPrel(scan.getCluster(), scan.getTraitSet(), scan, newProjExprs, newProjRowType);
      }
    }

    if (conditions.size() == 0) {
      // convert the filter to one that references the child of the project
      condition =  RelOptUtil.pushPastProject(filterAboveFlatten.getCondition(), projectWithFlatten);
      conditions.add(condition);
    }

    if (scan.getGroupScan() instanceof BinaryTableGroupScan) {
      BinaryTableGroupScan groupScan = (BinaryTableGroupScan)scan.getGroupScan();
      doPushFilterIntoBinaryGroupScan(call, filterAboveFlatten, projectWithFlatten, scan, groupScan, condition);
    } else {
      assert(scan.getGroupScan() instanceof JsonTableGroupScan);
      JsonTableGroupScan groupScan = (JsonTableGroupScan)scan.getGroupScan();
      doPushFilterIntoJsonGroupScan(call, filterAboveFlatten, projectWithFlatten, scan, groupScan, conditions);
    }
  }

  /**
   * Get the descendant Scan node starting from this Project with the assumption that the child rels are unary.
   * @param rootProject
   * @return RelNode which is either a (logical) DrillScanRel or a (physical) ScanPrel if all child rels are unary,
   *   or null otherwise
   */
  // TODO: consolidate this method with similar method for logical scan in AbstractMatchFunction
  private static RelNode getDescendantScan(ProjectPrel rootProject) {
    RelNode current = rootProject;
    while (! (current instanceof ScanPrel) && ! (current instanceof DrillScanRel)) {
      if (current instanceof RelSubset) {
        if (((RelSubset) current).getBest() != null) {
          current = ((RelSubset) current).getBest();
        } else {
          current = ((RelSubset) current).getOriginal();
        }
      }

      int numinputs = current.getInputs().size();
      if (numinputs > 1) {
        return null;  // an n-ary operator was encountered
      } else if (numinputs > 0) {
        current = current.getInput(0);
      }
    }
    Preconditions.checkArgument(current instanceof ScanPrel || current instanceof DrillScanRel);
    return current;
  }

}
