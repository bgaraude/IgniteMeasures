package com.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;

import com.test.CacheBuilder.Factory;
import com.test.model.Foo;

public class Benchmark {

	final Ignite ignite;

	Benchmark(Ignite ignite) {
		this.ignite = ignite;
	}

	public static void main(String[] args) {
		try (Ignite ignite = Ignition.start()) {
			new Benchmark(ignite).start();
		}
	}

	void start() {

		CacheBuilder.Factory factory = CacheBuilder.newFactory(ignite);
		factory.newBuilder("foo").withMode(CacheMode.PARTITIONED)//
				.withQueryEntity(Integer.class, Foo.class)//
				.withQueryField("name", String.class)//
				.withQueryField("value", long.class)//
				.build()//
				.build()//
				.load(5_000_000, t -> t, Foo::newFoo);

		System.out.println("================================================================================");
		System.out.println("ScanQuery");
		System.out.println("-----");
		System.out.println("non-partitionned query with no predicate");
		long t = System.currentTimeMillis();
		Long res = ignite.compute().execute(ScanQueryComputeTask.class, false);
		System.out.printf("result: %d; time: %d%n", res, System.currentTimeMillis() - t);

		System.out.println("-----");
		System.out.println("partitionned query with no predicate");
		t = System.currentTimeMillis();
		res = ignite.compute().execute(ScanQueryComputeTask.class, true);
		System.out.printf("result: %d; time: %d%n", res, System.currentTimeMillis() - t);

		System.out.println("-----");
		System.out.println("non-partitionned query with predicate");
		t = System.currentTimeMillis();
		res = ignite.compute().execute(ScanQueryPredicateComputeTask.class, false);
		System.out.printf("result: %d; time: %d%n", res, System.currentTimeMillis() - t);

		System.out.println("-----");
		System.out.println("partitionned query with predicate");
		t = System.currentTimeMillis();
		res = ignite.compute().execute(ScanQueryPredicateComputeTask.class, true);
		System.out.printf("result: %d; time: %d%n", res, System.currentTimeMillis() - t);

		
		System.out.println("================================================================================");
		System.out.println("SQLQuery");
		System.out.println("-----");
		System.out.println("non-partitionned query with no predicate");
		 t = System.currentTimeMillis();
		 res = ignite.compute().execute(SQLQueryComputeTask.class, false);
		System.out.printf("result: %d; time: %d%n", res, System.currentTimeMillis() - t);

		System.out.println("-----");
		System.out.println("partitionned query with no predicate");
		t = System.currentTimeMillis();
		res = ignite.compute().execute(SQLQueryComputeTask.class, true);
		System.out.printf("result: %d; time: %d%n", res, System.currentTimeMillis() - t);

		System.out.println("-----");
		System.out.println("non-partitionned query with predicate");
		t = System.currentTimeMillis();
		res = ignite.compute().execute(SQLQueryPredicateComputeTask.class, false);
		System.out.printf("result: %d; time: %d%n", res, System.currentTimeMillis() - t);

		System.out.println("-----");
		System.out.println("partitionned query with predicate");
		t = System.currentTimeMillis();
		res = ignite.compute().execute(SQLQueryPredicateComputeTask.class, true);
		System.out.printf("result: %d; time: %d%n", res, System.currentTimeMillis() - t);

	}

}
