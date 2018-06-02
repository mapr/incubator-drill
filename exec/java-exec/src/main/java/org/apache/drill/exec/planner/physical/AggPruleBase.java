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
package org.apache.drill.exec.planner.physical;

import java.util.List;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.sql.SqlKind;

import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

// abstract base class for the aggregation physical rules
public abstract class AggPruleBase extends Prule {

  protected AggPruleBase(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  public static List<DistributionField> getDistributionField(DrillAggregateRel rel, boolean allFields) {
    List<DistributionField> groupByFields = Lists.newArrayList();

    for (int group : remapGroupSet(rel.getGroupSet())) {
      DistributionField field = new DistributionField(group);
      groupByFields.add(field);

      if (!allFields && groupByFields.size() == 1) {
        // TODO: if we are only interested in 1 grouping field, pick the first one for now..
        // but once we have num distinct values (NDV) statistics, we should pick the one
        // with highest NDV.
        break;
      }
    }

    return groupByFields;
  }

  // Create 2 phase aggr plan for aggregates such as SUM, MIN, MAX
  // If any of the aggregate functions are not one of these, then we
  // currently won't generate a 2 phase plan.
  public static boolean create2PhasePlan(RelOptRuleCall call, DrillAggregateRel aggregate) {
    PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    RelNode child = call.rel(0).getInputs().get(0);
    boolean smallInput =
        child.estimateRowCount(child.getCluster().getMetadataQuery()) < settings.getSliceTarget();
    if (!settings.isMultiPhaseAggEnabled() || settings.isSingleMode()
        // Can override a small child - e.g., for testing with a small table
        || (smallInput && !settings.isForce2phaseAggr())) {
      return false;
    }

    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      String name = aggCall.getAggregation().getName();
      if (!(name.equals(SqlKind.SUM.name())
          || name.equals(SqlKind.MIN.name())
          || name.equals(SqlKind.MAX.name())
          || name.equals(SqlKind.COUNT.name())
          || name.equals("$SUM0"))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns group-by keys with the remapped arguments for specified aggregate.
   *
   * @param groupSet ImmutableBitSet of aggregate rel node, whose group-by keys should be remapped.
   * @return {@link ImmutableBitSet} instance with remapped keys.
   */
  public static ImmutableBitSet remapGroupSet(ImmutableBitSet groupSet) {
    List<Integer> newGroupSet = Lists.newArrayList();
    int groupSetToAdd = 0;
    for (int ignored : groupSet) {
      newGroupSet.add(groupSetToAdd++);
    }
    return ImmutableBitSet.of(newGroupSet);
  }

  public static abstract class TwoPhaseHashAgg extends SubsetTransformer<DrillAggregateRel, InvalidRelException> {
    protected final DrillDistributionTrait distOnAllKeys;

    public TwoPhaseHashAgg (RelOptRuleCall call, DrillDistributionTrait distOnAllKeys) {
      super(call);
      this.distOnAllKeys = distOnAllKeys;
    }

    @Override
    public RelNode convertChild(DrillAggregateRel aggregate, RelNode input) throws InvalidRelException {

      RelTraitSet traits = newTraitSet(Prel.DRILL_PHYSICAL, input.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE));
      RelNode newInput = convert(input, traits);

      HashAggPrel phase1Agg = new HashAggPrel(
              aggregate.getCluster(),
              traits,
              newInput,
              aggregate.indicator,
              aggregate.getGroupSet(),
              aggregate.getGroupSets(),
              aggregate.getAggCallList(),
              AggPrelBase.OperatorPhase.PHASE_1of2);

      ExchangePrel exch = generateExchange(aggregate, phase1Agg);

      ImmutableBitSet newGroupSet = remapGroupSet(aggregate.getGroupSet());
      List<ImmutableBitSet> newGroupSets = Lists.newArrayList();
      for (ImmutableBitSet groupSet : aggregate.getGroupSets()) {
        newGroupSets.add(remapGroupSet(groupSet));
      }

      return new HashAggPrel(
              aggregate.getCluster(),
              exch.getTraitSet(),
              exch,
              aggregate.indicator,
              newGroupSet,
              newGroupSets,
              phase1Agg.getPhase2AggCalls(),
              AggPrelBase.OperatorPhase.PHASE_2of2);
    }

    public abstract ExchangePrel generateExchange(DrillAggregateRel aggregation, RelNode input);
  }

  public static abstract class TwoPhaseStreamAgg extends SubsetTransformer<DrillAggregateRel, InvalidRelException> {
    protected final DrillDistributionTrait distOnAllKeys;
    protected final RelCollation collation;

    public TwoPhaseStreamAgg (RelOptRuleCall call, DrillDistributionTrait distOnAllKeys, RelCollation collation) {
      super(call);
      this.distOnAllKeys = distOnAllKeys;
      this.collation = collation;
    }

    @Override
    public RelNode convertChild(DrillAggregateRel aggregate, RelNode input) throws InvalidRelException {

      DrillDistributionTrait toDist = input.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE);
      RelTraitSet traits = collation != null ? newTraitSet(Prel.DRILL_PHYSICAL, collation, toDist) : newTraitSet(Prel.DRILL_PHYSICAL, toDist);
      RelNode newInput = convert(input, traits);

      StreamAggPrel phase1Agg = new StreamAggPrel(
              aggregate.getCluster(),
              traits,
              newInput,
              aggregate.indicator,
              aggregate.getGroupSet(),
              aggregate.getGroupSets(),
              aggregate.getAggCallList(),
              AggPrelBase.OperatorPhase.PHASE_1of2);

      ExchangePrel exch = generateExchange(aggregate, phase1Agg, collation);

      ImmutableBitSet newGroupSet = remapGroupSet(aggregate.getGroupSet());
      List<ImmutableBitSet> newGroupSets = Lists.newArrayList();
      for (ImmutableBitSet groupSet : aggregate.getGroupSets()) {
        newGroupSets.add(remapGroupSet(groupSet));
      }

      return new StreamAggPrel(
              aggregate.getCluster(),
              exch.getTraitSet(),
              exch,
              aggregate.indicator,
              newGroupSet,
              newGroupSets,
              phase1Agg.getPhase2AggCalls(),
              AggPrelBase.OperatorPhase.PHASE_2of2);
    }

    public abstract ExchangePrel generateExchange(DrillAggregateRel aggregation, RelNode input, RelCollation collation);

  }
}
