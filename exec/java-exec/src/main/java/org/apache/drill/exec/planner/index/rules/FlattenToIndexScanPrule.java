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

import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.index.FlattenIndexPlanCallContext;
import org.apache.drill.exec.planner.index.FunctionalIndexInfo;
import org.apache.drill.exec.planner.index.IndexCollection;
import org.apache.drill.exec.planner.index.IndexConditionInfo;
import org.apache.drill.exec.planner.index.IndexDescriptor;
import org.apache.drill.exec.planner.index.IndexGroup;
import org.apache.drill.exec.planner.index.IndexLogicalPlanCallContext;
import org.apache.drill.exec.planner.index.IndexPlanUtils;
import org.apache.drill.exec.planner.index.IndexProperties;
import org.apache.drill.exec.planner.index.IndexSelector;
import org.apache.drill.exec.planner.index.generators.AbstractIndexPlanGenerator;
import org.apache.drill.exec.planner.index.generators.IndexPlanGenerator;
import org.apache.drill.exec.planner.index.generators.CoveringIndexPlanGenerator;
import org.apache.drill.exec.planner.index.generators.NonCoveringIndexPlanGenerator;
import org.apache.drill.exec.planner.index.generators.common.FlattenConditionUtils;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.logical.partition.RewriteAsBinaryOperators;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class FlattenToIndexScanPrule extends AbstractIndexPrule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FlattenToIndexScanPrule.class);

  public static final FlattenToIndexScanPrule FILTER_PROJECT = new FlattenToIndexScanPrule(
      RelOptHelper.some(DrillFilterRel.class,
          RelOptHelper.any(DrillProjectRel.class)),
      "FlattenToIndexScanPrule:Filter_Project", new MatchFP());

  final private MatchFunction<FlattenIndexPlanCallContext> match;

  private FlattenToIndexScanPrule(RelOptRuleOperand operand,
                                   String description,
                                   MatchFunction<FlattenIndexPlanCallContext> match) {
    super(operand, description);
    this.match = match;
  }

  private static class MatchFP extends AbstractMatchFunction<FlattenIndexPlanCallContext> {

    public boolean match(RelOptRuleCall call) {
      final DrillProjectRel project = (DrillProjectRel) call.rel(1);
      // if Project does not contain a FLATTEN expression, rule does not apply
      return projectHasFlatten(project, true, null, null);
    }

    public FlattenIndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillFilterRel filterAboveFlatten = call.rel(0);
      final DrillProjectRel rootProjectWithFlatten = call.rel(1);

      DrillScanRel scan = getDescendantScan(rootProjectWithFlatten);

      if (scan == null || !checkScan(scan)) {
        return null;
      }

      FlattenIndexPlanCallContext idxContext = new FlattenIndexPlanCallContext(call,
          null, // upper project
          filterAboveFlatten,
          rootProjectWithFlatten,
          scan);

      return idxContext;
    }
  }

  public static class FlattenIndexPlanGenerator implements IndexPlanGenerator {
    private final IndexLogicalPlanCallContext indexContext;

    private FlattenIndexPlanGenerator(IndexLogicalPlanCallContext context) {
      this.indexContext = context;
    }


    @Override
    public AbstractIndexPlanGenerator getCoveringIndexGen(FunctionalIndexInfo functionInfo,
                                                          IndexGroupScan indexGroupScan,
                                                          RexNode indexCondition,
                                                          RexNode remainderCondition,
                                                          RexBuilder builder,
                                                          PlannerSettings settings) {
      return new CoveringIndexPlanGenerator(indexContext, functionInfo, indexGroupScan, indexCondition, remainderCondition, builder, settings);
    }

    @Override
    public AbstractIndexPlanGenerator getNonCoveringIndexGen(IndexDescriptor indexDesc,
                                                             IndexGroupScan indexGroupScan,
                                                             RexNode indexCondition,
                                                             RexNode remainderCondition,
                                                             RexNode totalCondition,
                                                             RexBuilder builder,
                                                             PlannerSettings settings) {
      return new NonCoveringIndexPlanGenerator(indexContext, indexDesc, indexGroupScan, indexCondition, remainderCondition, totalCondition, builder, settings);
    }
  }


  @Override
  public boolean matches(RelOptRuleCall call) {
    return match.match(call);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    FlattenIndexPlanCallContext context = match.onMatch(call);
    if (context != null) {
      doOnMatch(context,new FlattenIndexPlanGenerator(context));
    }
  }

  public boolean doOnMatch(FlattenIndexPlanCallContext indexContext, IndexPlanGenerator generator) {
    boolean result = false;
    Stopwatch indexPlanTimer = Stopwatch.createStarted();
    final PlannerSettings settings = PrelUtil.getPlannerSettings(indexContext.call.getPlanner());
    final IndexCollection indexCollection = getIndexCollection(settings, indexContext.scan);

    if (indexCollection == null ||
        !indexCollection.supportsArrayIndexes()) {
      return false;
    }

    logger.debug("Index Rule {} starts", this.description);

    RexBuilder builder = indexContext.getFilterAboveRootFlatten().getCluster().getRexBuilder();

    FlattenConditionUtils.ComposedConditionInfo cInfo =
        new FlattenConditionUtils.ComposedConditionInfo(builder);

    // compose new conditions, one for each Flatten expr and combining with
    // conditions below the Flatten
    FlattenConditionUtils.composeConditions(indexContext, builder, cInfo);

    if (cInfo.numEntries() == 0) {
      return false;
    }

    if (indexCollection.supportsIndexSelection()) {
      Set<String> fieldNamesWithFlatten = cInfo.getFieldNamesWithFlatten();
      boolean nonCoveringOnly = false;
      if (fieldNamesWithFlatten.size() > 1) {
        // For the AND-ed filter conditions, if the number of unique Flatten field names is > 1 then it means
        // that we cannot generate a covering index plan.  Set the flag appropriately so we can skip covering.
        nonCoveringOnly = true;
      }

      // Process each unique flatten field expression independently for index planning.  There may be multiple
      // index plans created and let costing decide which is cheaper.
      for (String fieldName : fieldNamesWithFlatten) {
        RexNode mainFlattenCondition = cInfo.getMainFlattenCondition(fieldName);
        RexNode remainderFlattenCondition = cInfo.getRemainderFlattenCondition(fieldName);

        // the index analysis code only understands binary operators, so the condition should be
        // rewritten to convert N-ary ANDs and ORs into binary ANDs and ORs
        RewriteAsBinaryOperators visitor = new RewriteAsBinaryOperators(true, builder);
        mainFlattenCondition = mainFlattenCondition.accept(visitor);

        try {
          result = processWithIndexSelection(indexContext,
              settings,
              mainFlattenCondition,
              remainderFlattenCondition,
              indexCollection,
              builder,
              generator,
              nonCoveringOnly);
        } catch(Exception e) {
          logger.warn("Exception while doing index planning ", e);
        }
      }
    } else {
      throw new UnsupportedOperationException("Index collection must support index selection");
    }

    indexPlanTimer.stop();
    logger.debug("Index Planning took {} ms", indexPlanTimer.elapsed(TimeUnit.MILLISECONDS));
    return result;
  }

  public boolean processWithIndexSelection(IndexLogicalPlanCallContext indexContext,
                                          PlannerSettings settings,
                                          RexNode mainFlattenCondition,
                                          RexNode remainderFlattenCondition,
                                          IndexCollection collection,
                                          RexBuilder builder,
                                          IndexPlanGenerator generator,
                                          boolean nonCoveringOnly) {
    boolean result = false;
    DrillScanRel scan = indexContext.scan;
    IndexConditionInfo.Builder infoBuilder = IndexConditionInfo.newBuilder(mainFlattenCondition,
            collection, builder, indexContext.scan);

    if (!analyzeCondition(indexContext, collection, mainFlattenCondition, builder, infoBuilder, logger)) {
      return false;
    }

    if (!initializeStatistics(scan, settings, indexContext,
        mainFlattenCondition, indexContext.isValidIndexHint, logger)) {
      return false;
    }

    List<IndexGroup> coveringIndexes = Lists.newArrayList();
    List<IndexGroup> nonCoveringIndexes = Lists.newArrayList();
    List<IndexGroup> intersectIndexes = Lists.newArrayList();

    IndexSelector selector = createAndInitSelector(indexContext,
            collection,
            builder,
            logger);

    // get the candidate indexes based on selection
    selector.getCandidateIndexes(infoBuilder, coveringIndexes, nonCoveringIndexes, intersectIndexes);

    GroupScan primaryTableScan = indexContext.scan.getGroupScan();

    if (!nonCoveringOnly) {
    try {
      for (IndexGroup index : coveringIndexes) {
        IndexProperties indexProps = index.getIndexProps().get(0);
        IndexDescriptor indexDesc = indexProps.getIndexDesc();
        IndexGroupScan idxScan = indexDesc.getIndexGroupScan();
        FunctionalIndexInfo indexInfo = indexDesc.getFunctionalInfo();

        RexNode indexCondition = indexProps.getLeadingColumnsFilter();
        RexNode remainderCondition = indexProps.getTotalRemainderFilter();

        // combine this remainder condition with the remainder flatten condition supplied by caller
        if (remainderFlattenCondition != null) {
          remainderCondition = remainderCondition == null ? remainderFlattenCondition :
              RexUtil.composeConjunction(builder, ImmutableList.of(remainderCondition,
                  remainderFlattenCondition), false);
        }

        // Copy primary table statistics to index table
        idxScan.setStatistics(((DbGroupScan) scan.getGroupScan()).getStatistics());
        logger.info("index_plan_info: Generating covering index plan for index: {}, query condition {}", indexDesc.getIndexName(), indexCondition.toString());
        AbstractIndexPlanGenerator planGen = generator.getCoveringIndexGen(indexInfo, idxScan, indexCondition, remainderCondition, builder, settings);
        result = planGen.goMulti() || result;
      }
    } catch (Exception e) {
      logger.warn("Exception while trying to generate covering index plan", e);
    }
    } else {
      // add the covering indexes (if any) to the list of non-covering indexes
      nonCoveringIndexes.addAll(coveringIndexes);
    }

    // Create non-covering index plans.

    // First, check if the primary table scan supports creating a restricted scan
    if (primaryTableScan instanceof DbGroupScan &&
            (((DbGroupScan) primaryTableScan).supportsRestrictedScan())) {
      try {
        for (IndexGroup index : nonCoveringIndexes) {
          IndexProperties indexProps = index.getIndexProps().get(0);
          IndexDescriptor indexDesc = indexProps.getIndexDesc();
          IndexGroupScan idxScan = indexDesc.getIndexGroupScan();

          RexNode indexCondition = indexProps.getLeadingColumnsFilter();
          RexNode remainderCondition = indexProps.getTotalRemainderFilter();

          // combine this remainder condition with the remainder flatten condition supplied by caller
          if (remainderFlattenCondition != null) {
            remainderCondition = remainderCondition == null ? remainderFlattenCondition :
                RexUtil.composeConjunction(builder, ImmutableList.of(remainderCondition,
                    remainderFlattenCondition), false);
          }

          // Combine the index and remainder conditions such that the total condition can be re-applied
          RexNode totalCondition = IndexPlanUtils.getTotalFilter(indexCondition, remainderCondition, builder);

          // Copy primary table statistics to index table
          idxScan.setStatistics(((DbGroupScan) primaryTableScan).getStatistics());
          logger.info("index_plan_info: Generating non-covering index plan for index: {}, query condition {}", indexDesc.getIndexName(), indexCondition.toString());
          AbstractIndexPlanGenerator planGen = generator.getNonCoveringIndexGen(indexDesc, idxScan, indexCondition, remainderCondition, totalCondition, builder, settings);
          result = planGen.goMulti() || result;
        }
      } catch (Exception e) {
        logger.warn("Exception while trying to generate non-covering index access plan", e);
      }
    }

    return result;
  }
}
