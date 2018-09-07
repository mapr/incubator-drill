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

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.common.DrillFilterRelBase;
import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.index.FlattenCallContext;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;

import org.apache.calcite.util.Pair;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public abstract class AbstractMatchFunction<T> implements MatchFunction<T> {
  public static boolean checkScan(DrillScanRel scanRel) {
    GroupScan groupScan = scanRel.getGroupScan();
    if (groupScan instanceof DbGroupScan) {
      DbGroupScan dbscan = ((DbGroupScan) groupScan);
      // if we already applied index convert rule, and this scan is indexScan or restricted scan already,
      // no more trying index convert rule
      return dbscan.supportsSecondaryIndex() && (!dbscan.isIndexScan()) && (!dbscan.isRestrictedScan());
    }
    return false;
  }

  public static boolean checkScan(GroupScan groupScan) {
    if (groupScan instanceof DbGroupScan) {
      DbGroupScan dbscan = ((DbGroupScan) groupScan);
      // if we already applied index convert rule, and this scan is indexScan or restricted scan already,
      // no more trying index convert rule
      return dbscan.supportsSecondaryIndex() &&
             !dbscan.isRestrictedScan() &&
              (!dbscan.isFilterPushedDown() || dbscan.isIndexScan()) &&
             !containsStar(dbscan);
    }
    return false;
  }

  public static boolean containsStar(DbGroupScan dbscan) {
    for (SchemaPath column : dbscan.getColumns()) {
      if (column.getRootSegment().getPath().startsWith("*")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if a Project contains Flatten expressions and populate a supplied map with a mapping of
   * the field name to the RexCall corresponding to the Flatten expression. If there are multiple
   * Flattens, identify all of them unless firstFlattenOnly is true. Also populate a supplied list
   * of non-flatten exprs in the Project.
   * @param project
   * @param firstFlattenOnly
   * @param flattenMap
   * @param nonFlattenExprs
   * @return True if Flatten was found, False otherwise
   */
  public static boolean projectHasFlatten(DrillProjectRelBase project, boolean firstFlattenOnly, Map<String, RexCall> flattenMap,
      List<RexNode> nonFlattenExprs) {
    boolean found = false;
    if (project == null) {
      return found;
    }

    for (Pair<RexNode, String> p : project.getNamedProjects()) {
      if (p.left instanceof RexCall) {
        RexCall function = (RexCall) p.left;
        String functionName = function.getOperator().getName();
        if (functionName.equalsIgnoreCase("flatten")
            && function.getOperands().size() == 1) {
          if (flattenMap != null) {
            flattenMap.put((String) p.right, (RexCall) p.left);
          }
          if (firstFlattenOnly) {
            return true;
          }
          found = true;
          // continue since there may be multiple FLATTEN exprs which may be
          // referenced by the filter condition
        } else {
          if (nonFlattenExprs != null) {
            nonFlattenExprs.add((RexCall) p.left);
          }
        }
      } else if (nonFlattenExprs != null) {
        nonFlattenExprs.add(p.left);
      }
    }
    return found;
  }

  /**
   * Get the descendant Scan node starting from this Project with the assumption that the child rels are unary.
   * @param rootProject
   * @return The DrillScanRel leaf node if all child rels are unary, or null otherwise
   */
  public static DrillScanRel getDescendantScan(DrillProjectRel rootProject) {
    RelNode current = rootProject;
    while (! (current instanceof DrillScanRel)) {
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
    Preconditions.checkArgument(current instanceof DrillScanRel);
    return (DrillScanRel) current;
  }

  /**
   * Traverse the hierarchy of rels starting from the root of the supplied FlattenCallContext and initialize
   * remaining attributes of flatten context.
   * @param flattenContext
   * @return The leaf level DrillScanRelBase (either logical or physical scan) or NULL
   */
  public static DrillScanRelBase initializeContext(FlattenCallContext flattenContext) {
    boolean encounteredFilter = false;
    // Since there could be a chain of Projects with Flattens (e.g Project-Project-Project-Scan)
    // recurse to the leaf to find the Scan
    RelNode current = flattenContext.getProjectWithRootFlatten();
    final LinkedHashMap<RelNode, Map<String, RexCall>> projectToFlattenExprsMap =
        flattenContext.getProjectToFlattenMapForAllProjects();
    final LinkedHashMap<RelNode, List<RexNode>> projectToNonFlattenExprsMap =
        flattenContext.getNonFlattenExprsMapForAllProjects();

    while (current != null && ! (current instanceof DrillScanRelBase)) {
      if (current instanceof RelSubset) {
        current = (((RelSubset) current).getBest());
      }

      // populate the flatten and non-flatten collections for each Project
      if (current instanceof DrillProjectRelBase) {
        final DrillProjectRelBase currentProject = (DrillProjectRelBase) current;
        final boolean projectHasFlatten =
            AbstractMatchFunction.projectHasFlatten(currentProject, true /* first flatten only */, null, null);

        if ( projectHasFlatten && !projectToFlattenExprsMap.containsKey(currentProject)) {
          projectToFlattenExprsMap.put(current, Maps.newHashMap());
        }

        Map<String, RexCall> flattenMap = projectToFlattenExprsMap.get(currentProject);
        List<RexNode> nonFlattenExprs = Lists.newArrayList();

        //This pattern is not handled by the FlattenIndexPlanCallContext.
        //For now, index planning is disabled because intermediate filter is being dropped.
        if (projectHasFlatten && encounteredFilter) {
          return null;
        }

        if (projectHasFlatten) {
          AbstractMatchFunction.projectHasFlatten(currentProject, false, flattenMap, nonFlattenExprs);
          projectToFlattenExprsMap.put(currentProject, flattenMap);
          projectToNonFlattenExprsMap.put(currentProject, nonFlattenExprs);
        } else {
          // this must be the leaf Project above Scan
          flattenContext.setLeafProjectAboveScan(currentProject);
        }
      } else if (current instanceof DrillFilterRelBase) {
        encounteredFilter = true;
        // this must be the filter below the leaf flatten
        flattenContext.setFilterBelowLeafFlatten((DrillFilterRelBase) current);
      }

      if (current.getInputs().size() > 0) {
        current = current.getInput(0);
      }
    }

    if (current != null && current instanceof DrillScanRelBase) {
      return (DrillScanRelBase) current;
    }

    return null;

  }

}