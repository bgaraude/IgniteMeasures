package com.test;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.IntFunction;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;

final class CacheBuilder<K, V> {

	final Ignite ignite;

	CacheConfiguration<K, V> conf;

	CacheBuilder(Ignite ignite, String cacheName) {
		this.ignite = ignite;
		this.conf = new CacheConfiguration<K, V>(cacheName);
	}

	static CacheBuilder.Factory newFactory(Ignite ignite) {
		return new Factory() {

			@Override
			public <K, V> CacheBuilder<K, V> newBuilder(String cacheName) {
				return new CacheBuilder<K, V>(ignite, cacheName);
			}
		};
	}

	CacheBuilder<K, V> withMode(CacheMode mode) {
		conf.setCacheMode(mode);
		return this;
	}

	<T, U> QueryEntityBuilder<T, U> withQueryEntity(Class<T> keyType, Class<U> valueType) {
		return new QueryEntityBuilder<>(keyType, valueType);
	}

	CacheBuilder<K, V> build() {
		ignite.createCache(conf);
		return this;
	}

	CacheBuilder<K, V> load(int count, IntFunction<K> keySupplier, IntFunction<V> valueSupplier) {

		try (IgniteDataStreamer<K, V> streamer = ignite.dataStreamer(conf.getName())) {

			System.out.printf("Loading cache %s with %s entries%n", conf.getName(), new DecimalFormat().format(count));
			System.out.printf("0%%                     50%%                   100%%%n");
			System.out.printf("|                       |                       |%n");

			for (int i = 0; i < count; i++) {
				streamer.addData(keySupplier.apply(i), valueSupplier.apply(i));
				if (i > 0 && i % (count / 50) == 0) {
					System.out.print('O');
				}
			}
			System.out.println();
		}
		return this;
	}

	final class QueryEntityBuilder<T, U> {

		final QueryEntity queryEntity;

		QueryEntityBuilder(Class<?> keyType, Class<?> valueType) {
			this.queryEntity = new QueryEntity(keyType, valueType);
		}

		QueryEntityBuilder<T, U> withQueryField(String name, Class<?> type) {
			queryEntity.addQueryField(name, type.getName(), null);
			return this;
		}

		QueryEntityBuilder<T, U> withIndex(String... fields) {
			QueryIndex index = new QueryIndex();
			index.setFieldNames(Arrays.asList(fields), true);

			Collection<QueryIndex> indexes = new ArrayList<QueryIndex>(
					queryEntity.getIndexes() == null ? Collections.emptyList() : queryEntity.getIndexes());
			indexes.add(index);
			queryEntity.setIndexes(indexes);
			return this;
		}

		CacheBuilder<K, V> build() {
			Collection<QueryEntity> queryEntities = conf.getQueryEntities();
			queryEntities = new ArrayList<QueryEntity>(queryEntities == null ? Collections.emptyList() : queryEntities);
			queryEntities.add(queryEntity);
			conf.setQueryEntities(queryEntities);
			return CacheBuilder.this;
		}

	}

	static interface Factory {
		<K, V> CacheBuilder<K, V> newBuilder(String cacheName);
	}

}
