package crawler

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"webCrawler/internal/models"
	"webCrawler/internal/registry"

	"github.com/PuerkitoBio/goquery"
)

func Worker(
	ctx context.Context,
	wg *sync.WaitGroup,
	id int,
	jobs <-chan string,
	results chan<- models.CrawlerConfig,
	registry *registry.VisitedRegistry,
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

			results <- models.CrawlerConfig{
				URL:        job,
				Body:       pageTitle,
				FoundLinks: foundLinks,
			}

		}
	}

}
