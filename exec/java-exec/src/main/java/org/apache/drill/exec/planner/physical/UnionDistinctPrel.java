/**
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

import java.io.IOException;
import java.util.List;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.UnionAll;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.UnionRelBase;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

import com.google.common.collect.Lists;

public class UnionDistinctPrel extends UnionPrel {

  public UnionDistinctPrel(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs) throws InvalidRelException {
    super(cluster, traits, inputs, false /* all = false */);
  }


  @Override
  public UnionRelBase copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    try {
      return new UnionDistinctPrel(this.getCluster(), traitSet, inputs);
    }catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }
    double totalInputRowCount = 0;
    for (int i = 0; i < this.getInputs().size(); i++) {
      totalInputRowCount += RelMetadataQuery.getRowCount(this.getInputs().get(i));
    }

    double cpuCost = totalInputRowCount * DrillCostBase.BASE_CPU_COST;
    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    return costFactory.makeCost(totalInputRowCount, cpuCost, 0, 0);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    List<PhysicalOperator> inputPops = Lists.newArrayList();
    
    for (int i = 0; i < this.getInputs().size(); i++) {
      inputPops.add( ((Prel)this.getInputs().get(i)).getPhysicalOperator(creator));
    }

    ///TODO: change this to UnionDistinct once implemented end-to-end..
    UnionAll unionAll = new UnionAll(inputPops.toArray(new PhysicalOperator[inputPops.size()]));
    unionAll.setOperatorId(creator.getOperatorId(this));

    return unionAll;
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

}
