package prober

import (
	"fmt"
	"sync"
	"time"

	"github.com/ricoberger/script_exporter/config"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

)

var cronCache map[string]cronCacheEntry
var cronInflights map[string]int
var cronCacheLock = sync.RWMutex{}

type cronCacheEntry struct {
	cacheTime time.Time
	result    scriptResult
	prommetrics []*prometheus.Metric
}


func getCronCacheKey(script *config.Script) string {
	return fmt.Sprintf("%s", script.Name)
}

func GetCronCacheResult(script *config.Script, useExpiredCache bool) *scriptResult {
	return getCronCacheResult(script, useExpiredCache)
}

func getCronCacheResult(script *config.Script, useExpiredCache bool) *scriptResult {
	cronCacheLock.RLock()
	defer cronCacheLock.RUnlock()

	expire := script.Autorun.Expire

	if expire == 0 {
		expire = script.Autorun.Interval * 2
	}

	if entry, ok := cronCache[getCronCacheKey(script)]; ok {
		if entry.cacheTime.Add(time.Duration(expire*float64(time.Second))).After(time.Now()) {
			return &entry.result
		}
	}

	return nil
}

func setCronCacheResult(script *config.Script, result scriptResult) {
	cronCacheLock.Lock()
	defer cronCacheLock.Unlock()

	if cronCache == nil {
		cronCache = make(map[string]cronCacheEntry)
	}

	prommetrics := make([]*prometheus.Metric, 0)

        for _, mf := range result.GetMetrics() {
                name := ""
                if mf.Name != nil {
                        name = *mf.Name
                }
                help := ""
                if mf.Help != nil {
                        help = *mf.Help
                }
                for _, m := range mf.Metric {
                        labels := make(map[string]string)
                        for i:=0; i<len(m.Label); i++ {
                                labels[*m.Label[i].Name] = *m.Label[i].Value
                        }
                        desc := prometheus.NewDesc(name, help, nil, labels)
                        
                        newmetric := func(desc *prometheus.Desc, mf *dto.MetricFamily, m *dto.Metric, ts time.Time) prometheus.Metric {
                                switch *mf.Type {
                                        case dto.MetricType_COUNTER: return prometheus.MustNewConstMetricWithCreatedTimestamp(desc, prometheus.CounterValue, *m.Counter.Value, ts)
                                        case dto.MetricType_GAUGE: return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, *m.Gauge.Value)
                                        case dto.MetricType_SUMMARY:
                                                quantiles := make(map[float64]float64)
                                                for j:=0; j<len(m.Summary.Quantile); j++ {
                                                        quantiles[*m.Summary.Quantile[j].Quantile] = *m.Summary.Quantile[j].Value
                                                }
                                                return prometheus.MustNewConstSummaryWithCreatedTimestamp(desc, *m.Summary.SampleCount, *m.Summary.SampleSum, quantiles, ts)
                                        case dto.MetricType_UNTYPED: return prometheus.MustNewConstMetric(desc, prometheus.UntypedValue, *m.Untyped.Value)
                                        case dto.MetricType_HISTOGRAM: 
                                                buckets := make(map[float64]uint64)
                                                for j:=0; j<len(m.Histogram.Bucket); j++ {
                                                        buckets[*m.Histogram.Bucket[j].UpperBound] = *m.Histogram.Bucket[j].CumulativeCount
                                                }
                                                return prometheus.MustNewConstHistogramWithCreatedTimestamp(desc, *m.Histogram.SampleCount, *m.Histogram.SampleSum, buckets, ts)
                                        case dto.MetricType_GAUGE_HISTOGRAM: 
                                                buckets := make(map[float64]uint64)
                                                for j:=0; j<len(m.Histogram.Bucket); j++ {
                                                        buckets[*m.Histogram.Bucket[j].UpperBound] = *m.Histogram.Bucket[j].CumulativeCount
                                                }
                                                return prometheus.MustNewConstHistogramWithCreatedTimestamp(desc, *m.Histogram.SampleCount, *m.Histogram.SampleSum, buckets, ts)
                                        default: return nil
                                }
                        }(desc, mf, m, result.GetSTime())

                        prommetrics = append(prommetrics, &newmetric)
                }
        }


	cronCache[getCronCacheKey(script)] = cronCacheEntry{
		cacheTime: time.Now(),
		result:    result,
		prommetrics: prommetrics,
	}
}

func CronCollect(scripts []config.Script) []*prometheus.Metric {
	prommetrics := make([]*prometheus.Metric, 0)
	if cronCache == nil {
		return prommetrics
	}


	for _, script := range scripts {
        	expire := script.Autorun.Expire

	        if expire == 0 {
        	        expire = script.Autorun.Interval * 2
        	}

	        if entry, ok := cronCache[getCronCacheKey(&script)]; ok {
        	        if entry.cacheTime.Add(time.Duration(expire*float64(time.Second))).After(time.Now()) {
                	        prommetrics = append(prommetrics, entry.prommetrics...)
                	}
        	}
	}
        return prommetrics
}

func getCronInflight(script *config.Script) int {
	if cronInflights == nil {
		cronInflights = make(map[string]int)
	}
	if ci, ok := cronInflights[getCronCacheKey(script)]; ok {
		return ci
	}
	return 0
}

func incCronInflight(script *config.Script) {
	if cronInflights == nil {
		cronInflights = make(map[string]int)
	}
	if _, ok := cronInflights[getCronCacheKey(script)]; ok {
		cronInflights[getCronCacheKey(script)] += 1
	} else {
		cronInflights[getCronCacheKey(script)] = 1
	}
}

func decCronInflight(script *config.Script) {
	if cronInflights == nil {
		cronInflights = make(map[string]int)
	}
	if _, ok := cronInflights[getCronCacheKey(script)]; ok {
		cronInflights[getCronCacheKey(script)] -= 1
	}
}
