package com.test.model;

import java.util.concurrent.ThreadLocalRandom;

public final class Foo {

	public int id;
	public String name;
	public long value;
	public int refBar;

	public Foo(int id, String name, long value, int refBar) {
		this.id = id;
		this.name = name;
		this.value = value;
		this.refBar = refBar;
	}

	public static Foo newFoo(int id) {
		ThreadLocalRandom random = ThreadLocalRandom.current();
		return new Foo(id, Long.toHexString(random.nextLong()), random.nextLong(), random.nextInt());
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
