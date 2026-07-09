package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

type CrawlerConfig struct {
	URL        string
	Body       string
	FoundLinks []string
}

type VisitedRegistry struct {
	mu      sync.Mutex
	history map[string]bool
}

func (v *VisitedRegistry) Visit(url string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.history[url] {
		return false
	}
	v.history[url] = true
	return true
}

func worker(
	ctx context.Context,
	wg *sync.WaitGroup,
	id int,
	jobs <-chan string,
	results chan<- CrawlerConfig,
	registry *VisitedRegistry,
) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			if !registry.Visit(job) {
				fmt.Printf("[WORKER %d] URL %s уже посещен, пропуск...\n", id, job)
				continue
			}

			req, err := http.NewRequestWithContext(ctx, "GET", job, nil)
			if err != nil {
				fmt.Printf("[WORKER %d] Ошибка создания запроса для URL %s: %v\n", id, job, err)
				continue
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				fmt.Printf("[WORKER %d] Ошибка выполнения запроса для URL %s: %v\n", id, job, err)
				continue
			}

			doc, err := goquery.NewDocumentFromReader(resp.Body)
			resp.Body.Close()
			if err != nil {
				fmt.Printf("[WORKER %d] Ошибка чтения тела ответа для URL %s: %v\n", id, job, err)
				continue
			}

			var foundLinks []string

			pageTitle := doc.Find("title").Text()
			doc.Find("a").Each(func(i int, s *goquery.Selection) {
				link, exists := s.Attr("href")
				if exists {
					foundLinks = append(foundLinks, link)
				}
			})

			results <- CrawlerConfig{
				URL:        job,
				Body:       pageTitle,
				FoundLinks: foundLinks,
			}

		}
	}

}

func main() {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	jobs := make(chan string, 100)
	results := make(chan CrawlerConfig, 100)

	registry := &VisitedRegistry{
		history: make(map[string]bool),
	}

	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go worker(ctx, wg, i, jobs, results, registry)
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
				registry.mu.Lock()
				count := len(registry.history)
				registry.mu.Unlock()
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
