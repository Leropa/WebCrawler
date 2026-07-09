package main

import (
	"context"
	"fmt"
	"sync"
	"time"
	"webCrawler/internal/crawler"
	"webCrawler/internal/models"
	"webCrawler/internal/registry"
)

func main() {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	jobs := make(chan string, 100)
	results := make(chan models.CrawlerConfig, 100)

	reg := registry.New()

	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go crawler.Worker(ctx, wg, i, jobs, results, reg)
	}

	go func() {
		for res := range results {
			fmt.Println("URL:", res.URL, "Body:", res.Body)

			for _, link := range res.FoundLinks {
				if len(link) > 4 && link[:4] == "http" {
					jobs <- link
				}
			}
		}

	}()

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				count := reg.Count()
				fmt.Printf("📊 [ANALYTICS] Всего обработано уникальных URL: %d\n", count)
			}
		}
	}()

	fmt.Println("[SYSTEM] Отправка ссылок в работу...")
	jobs <- "https://google.com"
	jobs <- "https://github.com"
	jobs <- "https://habr.com"
	jobs <- "https://golang.org"

	<-ctx.Done()
	fmt.Println("[SYSTEM] Все воркеры отстрелялись. Программа завершена.")
}
