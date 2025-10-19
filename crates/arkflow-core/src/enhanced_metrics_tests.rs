/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! Unit tests for enhanced metrics collection

use super::*;

#[test]
fn test_counter_basic_operations() {
    let counter = Counter::new("test_counter".to_string());

    assert_eq!(counter.get(), 0);

    counter.increment();
    assert_eq!(counter.get(), 1);

    counter.increment_by(5);
    assert_eq!(counter.get(), 6);

    counter.decrement();
    assert_eq!(counter.get(), 5);
}

#[test]
fn test_gauge_basic_operations() {
    let gauge = Gauge::new("test_gauge".to_string());

    assert_eq!(gauge.get(), 0.0);

    gauge.set(42.0);
    assert_eq!(gauge.get(), 42.0);

    gauge.add(10.0);
    assert_eq!(gauge.get(), 52.0);

    gauge.subtract(2.0);
    assert_eq!(gauge.get(), 50.0);
}

#[test]
fn test_histogram_basic_operations() {
    let histogram = Histogram::new("test_histogram".to_string(), 100);

    // Test empty histogram
    let percentiles = histogram.get_percentiles();
    assert_eq!(percentiles.count, 0);
    assert_eq!(percentiles.mean, 0.0);

    // Add some values
    histogram.observe(10.0);
    histogram.observe(20.0);
    histogram.observe(30.0);

    let percentiles = histogram.get_percentiles();
    assert_eq!(percentiles.count, 3);
    assert_eq!(percentiles.min, 10.0);
    assert_eq!(percentiles.max, 30.0);
    assert_eq!(percentiles.p50, 20.0);
}

#[test]
fn test_histogram_max_samples() {
    let histogram = Histogram::new("test_histogram".to_string(), 3);

    // Add more values than max_samples
    histogram.observe(1.0);
    histogram.observe(2.0);
    histogram.observe(3.0);
    histogram.observe(4.0);
    histogram.observe(5.0);

    let percentiles = histogram.get_percentiles();
    assert_eq!(percentiles.count, 3); // Should be limited to max_samples
    assert_eq!(percentiles.min, 3.0); // Should keep most recent values
    assert_eq!(percentiles.max, 5.0);
}

#[test]
fn test_enhanced_metrics_collection() {
    let metrics = EnhancedMetrics::new();

    // Test counter operations
    metrics.counter("test_counter").unwrap().increment_by(5);
    assert_eq!(metrics.get_counter_value("test_counter"), Some(5));

    // Test gauge operations
    metrics.gauge("test_gauge").unwrap().set(100.0);
    assert_eq!(metrics.get_gauge_value("test_gauge"), Some(100.0));

    // Test histogram operations
    metrics.histogram("test_histogram").unwrap().observe(50.0);
    let percentiles = metrics.get_histogram_percentiles("test_histogram").unwrap();
    assert_eq!(percentiles.count, 1);
    assert_eq!(percentiles.p50, 50.0);
}

#[test]
fn test_metrics_error_handling() {
    let metrics = EnhancedMetrics::new();

    // Test non-existent metrics
    assert!(metrics.get_counter_value("non_existent").is_none());
    assert!(metrics.get_gauge_value("non_existent").is_none());
    assert!(metrics.get_histogram_percentiles("non_existent").is_none());
}

#[test]
fn test_metrics_reset() {
    let metrics = EnhancedMetrics::new();

    metrics.counter("test_counter").unwrap().increment_by(10);
    metrics.gauge("test_gauge").unwrap().set(25.0);
    metrics.histogram("test_histogram").unwrap().observe(15.0);

    metrics.reset();

    assert_eq!(metrics.get_counter_value("test_counter"), Some(0));
    assert_eq!(metrics.get_gauge_value("test_gauge"), Some(0.0));

    let percentiles = metrics.get_histogram_percentiles("test_histogram").unwrap();
    assert_eq!(percentiles.count, 0);
}

#[test]
fn test_metrics_export() {
    let metrics = EnhancedMetrics::new();

    metrics.counter("requests_total").unwrap().increment_by(100);
    metrics.gauge("memory_usage").unwrap().set(1024.0);
    metrics.histogram("request_duration").unwrap().observe(50.0);

    let exported = metrics.export_metrics();

    // Should contain our metrics
    assert!(exported.contains("requests_total"));
    assert!(exported.contains("memory_usage"));
    assert!(exported.contains("request_duration"));

    // Should contain values
    assert!(exported.contains("100")); // counter value
    assert!(exported.contains("1024")); // gauge value
}
