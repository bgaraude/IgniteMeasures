package com.test.model;

import java.util.concurrent.ThreadLocalRandom;

public final class Foo {

	public int id;
	public String name;
	public long value;

	public Foo(int id, String name, long value) {
		this.id = id;
		this.name = name;
		this.value = value;
	}

	public static Foo newFoo(int id) {
		ThreadLocalRandom random = ThreadLocalRandom.current();
		return new Foo(id, Long.toHexString(random.nextLong()), random.nextLong());
	}

}
