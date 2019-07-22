package com.test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeLoadBalancer;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoadBalancerResource;

import com.test.model.Foo;

public class SQLQueryPredicateComputeTask extends ComputeTaskAdapter<Boolean, Long> {

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
			SqlQuery<BinaryObject, BinaryObject> query = new SqlQuery<>(Foo.class, "name like '%ab'");
			if (part >= 0) {
				query.setPartitions(part);
			}

			long sum = 0L;

			try (QueryCursor<Entry<BinaryObject, BinaryObject>> cursor = ignite.cache("foo").withKeepBinary().query(query)) {

				BinaryField field = ignite.binary().type(Foo.class).field("value");

				for (Iterator<Entry<BinaryObject, BinaryObject>> iterator = cursor.iterator(); iterator.hasNext();) {
					Entry<BinaryObject, BinaryObject> entry = iterator.next();

					sum += (Long) field.value(entry.getValue());

				}

			}
			return sum;
		}

	}

}
