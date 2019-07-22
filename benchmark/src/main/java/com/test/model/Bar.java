package com.test.model;

import java.util.concurrent.ThreadLocalRandom;

import com.test.model.Foo.Id;

public final class Bar {

	public int id;
	public String name;
	public long value;
	public int refFoo;

	public Bar(int id, String name, long value, int refFoo) {
		this.id = id;
		this.name = name;
		this.value = value;
		this.refFoo = refFoo;
	}

	public static Bar newBar(int id) {
		ThreadLocalRandom random = ThreadLocalRandom.current();
		return new Bar(id, Long.toHexString(random.nextLong()), random.nextLong(), random.nextInt());
	}

	public static class Id {

		public int id;

		public Id(int id) {
			this.id = id;
		}

		public static Id newId(int id) {
			return new Id(id);
		}
	}
}
