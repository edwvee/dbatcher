package httpclient

import (
	"testing"
	"time"
)

func BenchmarkSingleRequestsKeepAlive(b *testing.B) {
	rows := [][]interface{}{
		{
			0,
			"htp://site.example/path0/path1/path2?param0=value0&param1=value1&param3=value3",
			"htp://site.example/path0/path1?param0=value0&param1=value1&param3=value3",
			666666,
			666,
			"c",
		},
	}

	config := ClientConfig{
		"http://127.0.0.1:8124", 2 * time.Second, 2 * time.Second,
	}
	client := NewClient(config)
	for i := 0; i < b.N; i++ {
		rows[0][0] = time.Now().Format("2006-01-02 15:04:05.999")
		err := client.Send(
			"`visited_url`", "dt,url, sourse_url, response_time_ms, found_urls, shit",
			10000, 100000, false, false, rows,
		)
		if err != nil {
			b.Error(err)
			b.FailNow()
		}
	}
}

func BenchmarkSingleRequestsNoKeepAlive(b *testing.B) {
	rows := [][]interface{}{
		{
			0,
			"htp://site.example/path0/path1/path2?param0=value0&param1=value1&param3=value3",
			"htp://site.example/path0/path1?param0=value0&param1=value1&param3=value3",
			666666,
			666,
			"a",
		},
	}

	for i := 0; i < b.N; i++ {
		config := ClientConfig{
			"http://127.0.0.1:8124", 2 * time.Second, 2 * time.Second,
		}
		rows[0][0] = time.Now().Format("2006-01-02 15:04:05.999")
		err := Send(
			config,
			"`visited_url`", "dt,url, sourse_url, response_time_ms, found_urls, shit",
			3000, 10000, false, false, rows,
		)
		if err != nil {
			b.Error(err)
			b.FailNow()
		}
	}
}
