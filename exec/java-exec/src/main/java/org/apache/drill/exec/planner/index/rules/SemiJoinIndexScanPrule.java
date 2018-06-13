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
package org.apache.drill.exec.planner.index.rules;

import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.index.FlattenIndexPlanCallContext;
import org.apache.drill.exec.planner.index.SemiJoinIndexPlanCallContext;
import org.apache.drill.exec.planner.index.IndexDescriptor;
import org.apache.drill.exec.planner.index.IndexPlanUtils;
import org.apache.drill.exec.planner.index.FunctionalIndexInfo;
import org.apache.drill.exec.planner.index.IndexLogicalPlanCallContext;
import org.apache.drill.exec.planner.index.generators.SemiJoinMergeRowKeyJoinGenerator;
import org.apache.drill.exec.planner.index.generators.SemiJoinToRowKeyJoinGenerator;
import org.apache.drill.exec.planner.index.generators.SemiJoinToCoveringIndexScanGenerator;
import org.apache.drill.exec.planner.index.generators.IndexPlanGenerator;
import org.apache.drill.exec.planner.index.generators.AbstractIndexPlanGenerator;
import org.apache.drill.exec.planner.index.generators.common.SemiJoinIndexPlanUtils;
import org.apache.drill.exec.planner.index.generators.common.SemiJoinTransformUtils;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.physical.PlannerSettings;

import java.util.List;
import java.util.Map;

public class SemiJoinIndexScanPrule extends AbstractIndexPrule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SemiJoinIndexScanPrule.class);

  private static RelOptRule JOIN_FILTER_PROJECT_SCAN = null;
  private static RelOptRule JOIN_FILTER_PROJECT_FILTER_SCAN = null;
  private static RelOptRule JOIN_FILTER_PROJECT_FILTER_PROJECT_SCAN = null;

  final private MatchFunction<SemiJoinIndexPlanCallContext> match;
  final private FunctionImplementationRegistry functionRegistry;

  public static List<RelOptRule> getRuleInstances(FunctionImplementationRegistry functionRegistry) {
    if (JOIN_FILTER_PROJECT_SCAN == null) {
      JOIN_FILTER_PROJECT_SCAN = new SemiJoinIndexScanPrule(
              RelOptHelper.some(DrillJoinRel.class,
                      RelOptHelper.any(AbstractRelNode.class),
                      RelOptHelper.some(DrillAggregateRel.class,
                              RelOptHelper.some(DrillProjectRel.class,
                                      RelOptHelper.some(DrillFilterRel.class,
                                              RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class)))))),
              "SemiJoinIndexScanPrule:Join_Project_Filter_Project_Scan", functionRegistry, new MatchJSPFPS());
    }

    if (JOIN_FILTER_PROJECT_FILTER_SCAN == null) {
      JOIN_FILTER_PROJECT_FILTER_SCAN = new SemiJoinIndexScanPrule(
              RelOptHelper.some(DrillJoinRel.class,
                      RelOptHelper.any(AbstractRelNode.class),
                      RelOptHelper.some(DrillAggregateRel.class,
                              RelOptHelper.some(DrillProjectRel.class,
                                      RelOptHelper.some(DrillFilterRel.class,
                                              RelOptHelper.some(DrillProjectRel.class,
                                                      RelOptHelper.some(DrillFilterRel.class, RelOptHelper.any(DrillScanRel.class))))))),
              "SemiJoinIndexScanPrule:Join_Project_Filter_Project_Filter_Scan", functionRegistry, new MatchJSPFPFS());
    }

    if (JOIN_FILTER_PROJECT_FILTER_PROJECT_SCAN == null) {
      JOIN_FILTER_PROJECT_FILTER_PROJECT_SCAN = new SemiJoinIndexScanPrule(
              RelOptHelper.some(DrillJoinRel.class,
                      RelOptHelper.any(AbstractRelNode.class),
                      RelOptHelper.some(DrillAggregateRel.class,
                              RelOptHelper.some(DrillProjectRel.class,
                                      RelOptHelper.some(DrillFilterRel.class,
                                              RelOptHelper.some(DrillProjectRel.class,
                                                      RelOptHelper.some(DrillFilterRel.class,
                                                              RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class)))))))),
              "SemiJoinIndexScanPrule:Join_Project_Filter_Project_Filter_Project_Scan", functionRegistry, new MatchJSPFPFPS());
    }

    List<RelOptRule> rules = Lists.newArrayList();
    rules.add(JOIN_FILTER_PROJECT_SCAN);
    rules.add(JOIN_FILTER_PROJECT_FILTER_SCAN);
    rules.add(JOIN_FILTER_PROJECT_FILTER_PROJECT_SCAN);
    return rules;
  }

  private SemiJoinIndexScanPrule(RelOptRuleOperand operand,
                                 String description, FunctionImplementationRegistry functionRegistry,
                                 MatchFunction<SemiJoinIndexPlanCallContext> match) {
    super(operand, description);
    this.functionRegistry = functionRegistry;
    this.match = match;
  }

  private static class SemiJoinCoveringIndexPlanGenerator implements IndexPlanGenerator {
    private final SemiJoinIndexPlanCallContext context;

    private SemiJoinCoveringIndexPlanGenerator(SemiJoinIndexPlanCallContext context) {
      this.context = context;
    }

    @Override
    public AbstractIndexPlanGenerator getCoveringIndexGen(FunctionalIndexInfo functionInfo,
                                                          IndexGroupScan indexGroupScan,
                                                          RexNode indexCondition,
                                                          RexNode remainderCondition,
                                                          RexBuilder builder, PlannerSettings settings) {
      return new SemiJoinToCoveringIndexScanGenerator(context, functionInfo, indexGroupScan,
              indexCondition, remainderCondition, builder, settings);
    }

    @Override
    public AbstractIndexPlanGenerator getNonCoveringIndexGen(IndexDescriptor indexDesc,
                                                             IndexGroupScan indexGroupScan,
                                                             RexNode indexCondition,
                                                             RexNode remainderCondition,
                                                             RexNode totalCondition,
                                                             RexBuilder builder, PlannerSettings settings) {
      throw new RuntimeException();
    }
  }

  private static class SemiJoinNonCoveringIndexPlanGenerator implements IndexPlanGenerator {
    private final SemiJoinIndexPlanCallContext context;

    private SemiJoinNonCoveringIndexPlanGenerator(SemiJoinIndexPlanCallContext context) {
      this.context = context;
    }

    @Override
    public AbstractIndexPlanGenerator getCoveringIndexGen(FunctionalIndexInfo functionInfo,
                                                          IndexGroupScan indexGroupScan,
                                                          RexNode indexCondition,
                                                          RexNode remainderCondition,
                                                          RexBuilder builder, PlannerSettings settings) {
      return new SemiJoinToRowKeyJoinGenerator(context, functionInfo, indexGroupScan,
              indexCondition, remainderCondition, builder, settings);
    }

    @Override
    public AbstractIndexPlanGenerator getNonCoveringIndexGen(IndexDescriptor indexDesc,
                                                             IndexGroupScan indexGroupScan,
                                                             RexNode indexCondition,
                                                             RexNode remainderCondition,
                                                             RexNode totalCondition,
                                                             RexBuilder builder, PlannerSettings settings) {
      return new SemiJoinMergeRowKeyJoinGenerator(context, indexDesc, indexGroupScan, indexCondition,
              remainderCondition, totalCondition, builder, settings);
    }
  }

  private static class MatchJSPFPFPS extends AbstractMatchFunction<SemiJoinIndexPlanCallContext> {
    IndexLogicalPlanCallContext context = null;

    @Override
    public boolean match(RelOptRuleCall call) {
      DrillJoinRel joinRel = call.rel(0);
      DrillAggregateRel aggRel = call.rel(2);
      DrillProjectRel upperProject = call.rel(3);
      DrillFilterRel upperFilter = call.rel(4);
      DrillProjectRel lowerProject = call.rel(5);
      DrillFilterRel lowerFilter= call.rel(6);
      DrillProjectRel lowerProjectAboveScan = call.rel(7);
      DrillScanRel rightRel = call.rel(8);

      IndexLogicalPlanCallContext context = IndexPlanUtils.generateContext(call, call.rel(1), logger);

      if (context.scan == null) {
        return false;
      }

      this.context = context;
      if (!SemiJoinIndexPlanUtils.checkSameTableScan(context.scan, rightRel)) {
        return false;
      }

      if (aggRel.getAggCallList().size() > 0) {
        return false;
      }

      if (checkScan(rightRel)) {
        //check if the lower project contains the flatten.
        return projectHasFlatten(lowerProject, true, null, null);
      } else {
        return false;
      }
    }

    @Override
    public SemiJoinIndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillProjectRel upperProject = call.rel(3);
      final DrillFilterRel upperFilter = call.rel(4);
      final DrillProjectRel lowerProject = call.rel(5);
      final DrillFilterRel lowerFilter = call.rel(6);
      final DrillProjectRel lowerProjectAboveScan = call.rel(7);
      final DrillScanRel rightScan = call.rel(8);

      final DrillJoinRel join = call.rel(0);
      final DrillAggregateRel distinct = call.rel(2);

      Map<String, RexCall> flattenMap = Maps.newHashMap();
      List<RexNode> nonFlattenExprs = Lists.newArrayList();

      // populate the flatten and non-flatten collections
      projectHasFlatten(lowerProject, false, flattenMap, nonFlattenExprs);

      FlattenIndexPlanCallContext rightSideContext = new FlattenIndexPlanCallContext(call, upperProject,
              upperFilter, lowerProject,lowerFilter, lowerProjectAboveScan, rightScan, flattenMap, nonFlattenExprs);
      SemiJoinIndexPlanCallContext idxContext = new SemiJoinIndexPlanCallContext(call, join, distinct, this.context, rightSideContext);
      idxContext.setCoveringIndexPlanApplicable(!projectHasFlatten(context.lowerProject, true, null,null) &&
              !projectHasFlatten(context.upperProject, true, null, null));
      return idxContext;
    }
  }

  private static class MatchJSPFPFS extends AbstractMatchFunction<SemiJoinIndexPlanCallContext> {
    IndexLogicalPlanCallContext context = null;

    @Override
    public boolean match(RelOptRuleCall call) {
      DrillJoinRel joinRel = call.rel(0);
      DrillAggregateRel aggRel = call.rel(2);
      DrillProjectRel upperProject = call.rel(3);
      DrillFilterRel upperFilter = call.rel(4);
      DrillProjectRel lowerProject = call.rel(5);
      DrillFilterRel lowerFilter= call.rel(6);
      DrillScanRel rightRel = call.rel(7);

      IndexLogicalPlanCallContext context = IndexPlanUtils.generateContext(call, call.rel(1), logger);

      if (context.scan == null) {
        return false;
      }

      this.context = context;
      if (!SemiJoinIndexPlanUtils.checkSameTableScan(context.scan, rightRel)) {
        return false;
      }

      if (aggRel.getAggCallList().size() > 0) {
        return false;
      }

      if (checkScan(rightRel)) {
        //check if the lower project contains the flatten.
        return projectHasFlatten(lowerProject, true, null, null);
      } else {
        return false;
      }
    }

    @Override
    public SemiJoinIndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillProjectRel upperProject = call.rel(3);
      final DrillFilterRel upperFilter = call.rel(4);
      final DrillProjectRel lowerProject = call.rel(5);
      final DrillFilterRel lowerFilter = call.rel(6);
      final DrillScanRel rightScan = call.rel(7);

      final DrillJoinRel join = call.rel(0);
      final DrillAggregateRel distinct = call.rel(2);

      Map<String, RexCall> flattenMap = Maps.newHashMap();
      List<RexNode> nonFlattenExprs = Lists.newArrayList();

      // populate the flatten and non-flatten collections
      projectHasFlatten(lowerProject, false, flattenMap, nonFlattenExprs);

      FlattenIndexPlanCallContext rightSideContext = new FlattenIndexPlanCallContext(call, upperProject,
              upperFilter, lowerProject,lowerFilter, rightScan, flattenMap, nonFlattenExprs);
      SemiJoinIndexPlanCallContext idxContext = new SemiJoinIndexPlanCallContext(call, join, distinct, this.context, rightSideContext);
      idxContext.setCoveringIndexPlanApplicable(!projectHasFlatten(context.lowerProject, true, null,null) &&
              !projectHasFlatten(context.upperProject, true, null, null));
      return idxContext;
    }
  }

  private static class MatchJSPFPS extends AbstractMatchFunction<SemiJoinIndexPlanCallContext> {

    IndexLogicalPlanCallContext context = null;

    @Override
    public boolean match(RelOptRuleCall call) {
      DrillJoinRel joinRel = call.rel(0);
      DrillAggregateRel aggRel = call.rel(2);
      DrillProjectRel upperProject = call.rel(3);
      DrillFilterRel filter = call.rel(4);
      DrillProjectRel lowerProject = call.rel(5);
      DrillScanRel rightRel = call.rel(6);

      IndexLogicalPlanCallContext context = IndexPlanUtils.generateContext(call, call.rel(1), logger);

      if (context.scan == null) {
        return false;
      }

      this.context = context;
      if (!SemiJoinIndexPlanUtils.checkSameTableScan(context.scan, rightRel)) {
        return false;
      }

      if (aggRel.getAggCallList().size() > 0) {
        return false;
      }

      if (checkScan(rightRel)) {
        //check if the lower project contains the flatten.
        return projectHasFlatten(lowerProject, true, null, null);
      } else {
        return false;
      }
    }

    @Override
    public SemiJoinIndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillProjectRel upperProject = call.rel(3);
      final DrillFilterRel filter = call.rel(4);
      final DrillProjectRel lowerProject = call.rel(5);
      final DrillScanRel rightScan = call.rel(6);

      final DrillJoinRel join = call.rel(0);
      final DrillAggregateRel distinct = call.rel(2);

      Map<String, RexCall> flattenMap = Maps.newHashMap();
      List<RexNode> nonFlattenExprs = Lists.newArrayList();

      // populate the flatten and non-flatten collections

      projectHasFlatten(lowerProject, false, flattenMap, nonFlattenExprs);

      FlattenIndexPlanCallContext rightSideContext = new FlattenIndexPlanCallContext(call, upperProject, filter, lowerProject,
              null, rightScan, flattenMap, nonFlattenExprs);

      SemiJoinIndexPlanCallContext idxContext = new SemiJoinIndexPlanCallContext(call, join, distinct, this.context, rightSideContext);
      idxContext.setCoveringIndexPlanApplicable(!projectHasFlatten(context.lowerProject, true, null,null) &&
              !projectHasFlatten(context.upperProject, true, null, null));
      return idxContext;
    }
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return match.match(call);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    doOnMatch(match.onMatch(call));
  }

  private void doOnMatch(SemiJoinIndexPlanCallContext indexContext) {
    if (indexContext.join != null) {
      FlattenIndexPlanCallContext context = SemiJoinTransformUtils.transformJoinToSingleTableScan(indexContext, functionRegistry, logger);
      indexContext.set(context);
      if (context == null) {
        logger.warn("Covering Index Scan cannot be applied as FlattenIndexPlanContext is null");
      } else {
        if (FlattenToIndexScanPrule.FILTER_PROJECT_SCAN.doOnMatch(context, new SemiJoinCoveringIndexPlanGenerator(indexContext))) {
          return;
        }
      }
    }

    //As we couldn't generate a single table scan for the join,
    //try index planning by only considering the PFPS.
    FlattenToIndexScanPrule.FILTER_PROJECT_SCAN.doOnMatch(indexContext.rightSide, new SemiJoinNonCoveringIndexPlanGenerator(indexContext));
  }
}
