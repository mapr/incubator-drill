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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
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
import org.apache.drill.exec.planner.logical.DrillSemiJoinRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

import static org.apache.drill.exec.planner.logical.DrillRel.DRILL_LOGICAL;

public class SemiJoinIndexScanPrule extends AbstractIndexPrule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SemiJoinIndexScanPrule.class);

  public static RelOptRule JOIN_FILTER_PROJECT = new SemiJoinIndexScanPrule(
      RelOptHelper.some(DrillJoinRel.class,
          RelOptHelper.any(AbstractRelNode.class),
          RelOptHelper.some(DrillAggregateRel.class,
                  RelOptHelper.some(DrillProjectRel.class,
                          RelOptHelper.some(DrillFilterRel.class,
                                  RelOptHelper.any(DrillProjectRel.class))))),
  "SemiJoinIndexScanPrule:Join_Project_Filter_Project", new MatchJSPFP());

  public static RelOptRule SEMI_JOIN_FILTER_PROJECT = new SemiJoinIndexScanPrule(
          RelOptHelper.some(DrillSemiJoinRel.class,
                  RelOptHelper.any(AbstractRelNode.class),
                          RelOptHelper.some(DrillProjectRel.class,
                                  RelOptHelper.some(DrillFilterRel.class,
                                          RelOptHelper.any(DrillProjectRel.class)))),
          "SemiJoinIndexScanPrule:Semi_Join_Project_Filter_Project", new MatchSJSPFP());

  final private MatchFunction<SemiJoinIndexPlanCallContext> match;

  private SemiJoinIndexScanPrule(RelOptRuleOperand operand,
                                 String description,
                                 MatchFunction<SemiJoinIndexPlanCallContext> match) {
    super(operand, description);
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

  private static class MatchSJSPFP extends AbstractMatchFunction<SemiJoinIndexPlanCallContext> {

    @Override
    public boolean match(RelOptRuleCall call) {
      DrillProjectRel lowerProject = call.rel(4);

      // if Project does not contain a FLATTEN expression, rule does not apply
      if (!projectHasFlatten(lowerProject, true, null, null)) {
        return false;
      }

      // get the context for the left side of the join
      IndexLogicalPlanCallContext leftContext = IndexPlanUtils.generateContext(call, call.rel(1), logger);

      if (leftContext.scan == null || !checkScan(leftContext.scan)) {
        return false;
      }

      return true;
    }

    @Override
    public SemiJoinIndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillProjectRel upperProject = call.rel(2);
      final DrillFilterRel filter = call.rel(3);
      final DrillProjectRel lowerProject = call.rel(4); // project with flatten

      final DrillSemiJoinRel join = call.rel(0);

      IndexLogicalPlanCallContext leftContext = IndexPlanUtils.generateContext(call, call.rel(1), logger);

      DrillScanRel rightScan = getDescendantScan(lowerProject);

      if (rightScan == null || !checkScan(rightScan)) {
        return null;
      }

      if (!SemiJoinIndexPlanUtils.checkSameTableScan(leftContext.scan, rightScan)) {
        return null;
      }

      FlattenIndexPlanCallContext rightContext = new FlattenIndexPlanCallContext(call,
              upperProject,
              filter,
              lowerProject,
              rightScan);

      if (!rightContext.isValid) {
        return null;
      }

      SemiJoinIndexPlanCallContext idxContext = new SemiJoinIndexPlanCallContext(call, join, getAgg(join, join.getInput(1)),
              leftContext, rightContext);
      idxContext.setCoveringIndexPlanApplicable(!projectHasFlatten(leftContext.lowerProject, true, null, null) &&
              !projectHasFlatten(leftContext.upperProject, true, null, null));
      return idxContext;
    }

    private DrillAggregateRel getAgg(DrillSemiJoinRel semijoinRel, RelNode input) {
      Pair<ImmutableBitSet, List<ImmutableBitSet>> grpSets = createGroupSets(semijoinRel);
      return new DrillAggregateRel(semijoinRel.getCluster(), semijoinRel.getTraitSet().plus(DRILL_LOGICAL),
              input, grpSets.left, grpSets.right, new ArrayList<>());
    }

    private Pair<ImmutableBitSet, List<ImmutableBitSet>> createGroupSets(DrillSemiJoinRel semiJoinRel) {
      List<Integer> newGrpCols = Lists.newArrayList();
      for (int groupCol : semiJoinRel.getRightKeys()) {
        newGrpCols.add(groupCol);
      }
      ImmutableBitSet grpSet = ImmutableBitSet.of(newGrpCols);
      List<ImmutableBitSet> grpSets = Lists.newArrayList();
      grpSets.add(grpSet);
      return Pair.of(grpSet, grpSets);
    }
  }

  private static class MatchJSPFP extends AbstractMatchFunction<SemiJoinIndexPlanCallContext> {

    @Override
    public boolean match(RelOptRuleCall call) {
      DrillAggregateRel aggRel = call.rel(2);
      DrillProjectRel lowerProject = call.rel(5);

      // if Project does not contain a FLATTEN expression, rule does not apply
      if (!projectHasFlatten(lowerProject, true, null, null)) {
        return false;
      }

      // get the context for the left side of the join
      IndexLogicalPlanCallContext leftContext = IndexPlanUtils.generateContext(call, call.rel(1), logger);

      if (leftContext.scan == null || !checkScan(leftContext.scan)) {
        return false;
      }

      if (aggRel.getAggCallList().size() > 0) {
        return false;
      }

      return true;
    }

    @Override
    public SemiJoinIndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillProjectRel upperProject = call.rel(3);
      final DrillFilterRel filter = call.rel(4);
      final DrillProjectRel lowerProject = call.rel(5); // project with flatten

      final DrillJoinRel join = call.rel(0);
      final DrillAggregateRel distinct = call.rel(2);

      IndexLogicalPlanCallContext leftContext = IndexPlanUtils.generateContext(call, call.rel(1), logger);

      DrillScanRel rightScan = getDescendantScan(lowerProject);

      if (rightScan == null || !checkScan(rightScan)) {
        return null;
      }

      if (!SemiJoinIndexPlanUtils.checkSameTableScan(leftContext.scan, rightScan)) {
        return null;
      }

      FlattenIndexPlanCallContext rightContext = new FlattenIndexPlanCallContext(call,
          upperProject,
          filter,
          lowerProject,
          rightScan);

      if (!rightContext.isValid) {
        return null;
      }

      SemiJoinIndexPlanCallContext idxContext = new SemiJoinIndexPlanCallContext(call, join, distinct,
              leftContext, rightContext);
      idxContext.setCoveringIndexPlanApplicable(!projectHasFlatten(leftContext.lowerProject, true, null,null) &&
              !projectHasFlatten(leftContext.upperProject, true, null, null));
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
    if (indexContext == null) {
      return;
    }

    if (indexContext != null && indexContext.join != null) {
      PlannerSettings ps = PrelUtil.getPlannerSettings(indexContext.call.getPlanner());
      FlattenIndexPlanCallContext context = SemiJoinTransformUtils.transformJoinToSingleTableScan(indexContext, ps.functionImplementationRegistry, logger);
      indexContext.set(context);
      if (context == null) {
        logger.warn("Covering Index Scan cannot be applied as FlattenIndexPlanContext is null");
      } else {
        if (FlattenToIndexScanPrule.FILTER_PROJECT.doOnMatch(context, new SemiJoinCoveringIndexPlanGenerator(indexContext))) {
          return;
        }
      }
    }

    //As we couldn't generate a single table scan for the join,
    //try index planning by only considering the PFP.
    FlattenToIndexScanPrule.FILTER_PROJECT.doOnMatch(indexContext.rightSide, new SemiJoinNonCoveringIndexPlanGenerator(indexContext));
  }

}
