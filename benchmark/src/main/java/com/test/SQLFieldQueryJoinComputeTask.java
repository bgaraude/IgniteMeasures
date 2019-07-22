package com.test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeLoadBalancer;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoadBalancerResource;

public class SQLFieldQueryJoinComputeTask extends ComputeTaskAdapter<Boolean, Long> {

	@IgniteInstanceResource
	transient Ignite ignite;
	@LoadBalancerResource
	ComputeLoadBalancer loadBalancer;

	@Override
	public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Boolean partitionned)
			throws IgniteException {

		if (partitionned) {
			Map<ComputeJob, ClusterNode> ret = new HashMap<>();
			for (ClusterNode node : subgrid) {
				for (int part : ignite.affinity("foo").primaryPartitions(node)) {
					ret.put(new Job(part), node);
				}
			}
			return ret;
		}

		Job job = new Job(-1);
		return Collections.singletonMap(job, loadBalancer.getBalancedNode(job, null));
	}

	@Override
	public Long reduce(List<ComputeJobResult> results) throws IgniteException {
		return results.stream().map(ComputeJobResult::<Long>getData).mapToLong(Long::valueOf).sum();
	}

	static final class Job extends ComputeJobAdapter {

		final int part;

		@IgniteInstanceResource
		transient Ignite ignite;

		Job(int part) {
			this.part = part;
		}

		@Override
		public Long execute() throws IgniteException {
			SqlFieldsQuery query = new SqlFieldsQuery(
					"select f.name, b.value from Foo f join Bar b on f.refBar=b.id");
			if (part >= 0) {
				query.setPartitions(part);
			}

			long sum = 0L;

			try (FieldsQueryCursor<List<?>> cursor = ignite.cache("foo").withKeepBinary().query(query);) {

				for (Iterator<List<?>> iterator = cursor.iterator(); iterator.hasNext();) {
					List<?> entry = iterator.next();
					if (entry.get(1) != null) {
						sum += (Long) entry.get(1);
					}

				}

			}
			return sum;
		}

	}

}
