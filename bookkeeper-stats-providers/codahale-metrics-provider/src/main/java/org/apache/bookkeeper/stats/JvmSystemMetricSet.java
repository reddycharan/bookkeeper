/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.bookkeeper.stats;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricSet;

public class JvmSystemMetricSet implements MetricSet {

	private Map<String, Metric> metrics;

	private static final long BYTES_PER_MB = 1024L * 1024L;

	public JvmSystemMetricSet() {
		this.metrics = new HashMap<String, Metric>();
		populate();
	}

	@Override
	public Map<String, Metric> getMetrics() {
		return metrics;
	}

	@SuppressWarnings("restriction")
	private void populate() {

		final OperatingSystemMXBean osMbean = ManagementFactory
				.getOperatingSystemMXBean();
		if (osMbean instanceof com.sun.management.OperatingSystemMXBean) {
			final com.sun.management.OperatingSystemMXBean sunOsMbean = (com.sun.management.OperatingSystemMXBean) osMbean;

			metrics.put("system_free_physical_memory_mb", new Gauge<Long>() {
				public Long getValue() {
					return sunOsMbean.getFreePhysicalMemorySize()
							/ BYTES_PER_MB;
				}
			});

			metrics.put("system_free_swap_mb", new Gauge<Long>() {
				public Long getValue() {
					return sunOsMbean.getFreeSwapSpaceSize() / BYTES_PER_MB;
				}
			});

			metrics.put("process_cpu_time_nanos", new Gauge<Long>() {
				public Long getValue() {
					return sunOsMbean.getProcessCpuTime();
				}
			});
		}

		if (osMbean instanceof com.sun.management.UnixOperatingSystemMXBean) {
			@SuppressWarnings("restriction")
			final com.sun.management.UnixOperatingSystemMXBean unixOsMbean = (com.sun.management.UnixOperatingSystemMXBean) osMbean;

			metrics.put("proces_cpu_load", new Gauge<Double>() {
				public Double getValue() {
					return unixOsMbean.getProcessCpuLoad();
				}
			});

			metrics.put("system_cpu_load", new Gauge<Double>() {
				public Double getValue() {
					return unixOsMbean.getSystemCpuLoad();
				}
			});
		}

		final Runtime runtime = Runtime.getRuntime();
		final ClassLoadingMXBean classLoadingBean = ManagementFactory
				.getClassLoadingMXBean();
		MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
		final MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
		final MemoryUsage nonHeapUsage = memoryBean.getNonHeapMemoryUsage();
		final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
		final RuntimeMXBean runtimeMXBean = ManagementFactory
				.getRuntimeMXBean();

		metrics.put("jvm_time_ms", new Gauge<Long>() {
			public Long getValue() {
				return System.currentTimeMillis();
			}
		});

		metrics.put("jvm_available_processors", new Gauge<Integer>() {
			public Integer getValue() {
				return runtime.availableProcessors();
			}
		});

		metrics.put("jvm_memory_free_mb", new Gauge<Long>() {
			public Long getValue() {
				return runtime.freeMemory() / BYTES_PER_MB;
			}
		});

		metrics.put("jvm_memory_max_mb", new Gauge<Long>() {
			public Long getValue() {
				return runtime.maxMemory() / BYTES_PER_MB;
			}
		});

		metrics.put("jvm_memory_total_mb", new Gauge<Long>() {
			public Long getValue() {
				return runtime.totalMemory() / BYTES_PER_MB;
			}
		});

		metrics.put("jvm_class_loaded_count", new Gauge<Integer>() {
			public Integer getValue() {
				return classLoadingBean.getLoadedClassCount();
			}
		});

		metrics.put("jvm_class_total_loaded_count", new Gauge<Long>() {
			public Long getValue() {
				return classLoadingBean.getTotalLoadedClassCount();
			}
		});

		metrics.put("jvm_class_unloaded_count", new Gauge<Long>() {
			public Long getValue() {
				return classLoadingBean.getUnloadedClassCount();
			}
		});

		metrics.put("jvm_gc_collection_time_ms", new Gauge<Long>() {
			public Long getValue() {
				long collectionTimeMs = 0;
				for (GarbageCollectorMXBean bean : ManagementFactory
						.getGarbageCollectorMXBeans()) {
					collectionTimeMs += bean.getCollectionTime();
				}
				return collectionTimeMs;
			}
		});

		metrics.put("jvm_gc_collection_count", new Gauge<Long>() {
			public Long getValue() {
				long collections = 0;
				for (GarbageCollectorMXBean bean : ManagementFactory
						.getGarbageCollectorMXBeans()) {
					collections += bean.getCollectionCount();
				}
				return collections;
			}
		});

		metrics.put("jvm_memory_heap_used_mb", new Gauge<Long>() {
			public Long getValue() {
				return heapUsage.getUsed() / BYTES_PER_MB;
			}
		});

		metrics.put("jvm_memory_heap_committed_mb", new Gauge<Long>() {
			public Long getValue() {
				return heapUsage.getCommitted() / BYTES_PER_MB;
			}
		});

		metrics.put("jvm_memory_heap_max_mb", new Gauge<Long>() {
			public Long getValue() {
				return heapUsage.getMax() / BYTES_PER_MB;
			}
		});

		metrics.put("jvm_memory_non_heap_used_mb", new Gauge<Long>() {
			public Long getValue() {
				return nonHeapUsage.getUsed() / BYTES_PER_MB;
			}
		});

		metrics.put("jvm_memory_non_heap_committed_mb", new Gauge<Long>() {
			public Long getValue() {
				return nonHeapUsage.getCommitted() / BYTES_PER_MB;
			}
		});

		metrics.put("jvm_memory_non_heap_max_mb", new Gauge<Long>() {
			public Long getValue() {
				return nonHeapUsage.getMax() / BYTES_PER_MB;
			}
		});

		metrics.put("jvm_uptime_secs", new Gauge<Long>() {
			public Long getValue() {
				return runtimeMXBean.getUptime() / 1000;
			}
		});

		metrics.put("system_load_avg", new Gauge<Double>() {
			public Double getValue() {
				return osMbean.getSystemLoadAverage();
			}
		});

		metrics.put("jvm_threads_peak", new Gauge<Integer>() {
			public Integer getValue() {
				return threads.getPeakThreadCount();
			}
		});

		metrics.put("jvm_threads_started", new Gauge<Long>() {
			public Long getValue() {
				return threads.getTotalStartedThreadCount();
			}
		});

		metrics.put("jvm_threads_daemon", new Gauge<Integer>() {
			public Integer getValue() {
				return threads.getDaemonThreadCount();
			}
		});

		metrics.put("jvm_threads_active", new Gauge<Integer>() {
			public Integer getValue() {
				return threads.getThreadCount();
			}
		});

		for (final GarbageCollectorMXBean gcMXBean : ManagementFactory
				.getGarbageCollectorMXBeans()) {
			metrics.put("jvm_gc_" + gcMXBean.getName() + "_collection_count",
					new Gauge<Long>() {
						public Long getValue() {
							return gcMXBean.getCollectionCount();
						}
					});
		}

		for (final GarbageCollectorMXBean gcMXBean : ManagementFactory
				.getGarbageCollectorMXBeans()) {
			metrics.put("jvm_gc_" + gcMXBean.getName() + "_collection_time_ms",
					new Gauge<Long>() {
						public Long getValue() {
							return gcMXBean.getCollectionTime();
						}
					});
		}

		metrics.put("jvm_input_arguments", new Gauge<String>() {
			public String getValue() {
				return runtimeMXBean.getInputArguments().toString();
			}
		});

		for (final String property : System.getProperties()
				.stringPropertyNames()) {
			metrics.put("jvm_prop_" + property, new Gauge<String>() {
				public String getValue() {
					return System.getProperty(property);
				}
			});
		}
	}
}
