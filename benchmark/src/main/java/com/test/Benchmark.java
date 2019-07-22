package com.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;

import com.test.model.Bar;
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

				.withQueryEntity(Foo.Id.class, Foo.class)//
				.withQueryField("id", int.class)//
				.withQueryField("name", String.class)//
				.withQueryField("value", long.class)//
				.withQueryField("refBar", int.class)//
				.build()//

				.withQueryEntity(Bar.Id.class, Bar.class)//
				.withQueryField("id", int.class)//
				.withQueryField("name", String.class)//
				.withQueryField("value", long.class)//
				.withQueryField("refFoo", int.class)//
				.build()//

				.build()//

				.load(50_000, Foo.Id::newId, Foo::newFoo)//
				.load(50_000, Bar.Id::newId, Bar::newBar);

		long t;
		Long res;

		System.out.println("================================================================================");
		System.out.println("ScanQuery");
		System.out.println("-----");
		System.out.println("non-partitionned query with no predicate");
		t = System.currentTimeMillis();
		res = ignite.compute().execute(ScanQueryComputeTask.class, false);
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

		System.out.println("================================================================================");
		System.out.println("Joins");
		System.out.println("-----");
		System.out.println("non-partitionned scan query with join");
		t = System.currentTimeMillis();
		res = ignite.compute().execute(ScanQueryJoinComputeTask.class, false);
		System.out.printf("result: %d; time: %d%n", res, System.currentTimeMillis() - t);

		System.out.println("-----");
		System.out.println("partitionned scan query with join");
		t = System.currentTimeMillis();
		res = ignite.compute().execute(ScanQueryJoinComputeTask.class, true);
		System.out.printf("result: %d; time: %d%n", res, System.currentTimeMillis() - t);

		System.out.println("-----");
		System.out.println("non-partitionned sql field query with join");
		t = System.currentTimeMillis();
		res = ignite.compute().execute(SQLFieldQueryJoinComputeTask.class, false);
		System.out.printf("result: %d; time: %d%n", res, System.currentTimeMillis() - t);

		System.out.println("-----");
		System.out.println("partitionned sql field query with join");
		t = System.currentTimeMillis();
		res = ignite.compute().execute(SQLFieldQueryJoinComputeTask.class, true);
		System.out.printf("result: %d; time: %d%n", res, System.currentTimeMillis() - t);

	}

}
