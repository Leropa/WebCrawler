package models

type CrawlerConfig struct {
	URL        string
	Body       string
	FoundLinks []string
}
