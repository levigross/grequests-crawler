package main

import (
	"flag"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"sync/atomic"

	"github.com/PuerkitoBio/goquery"
	"github.com/levigross/grequests"
)

var (
	startingPoint = flag.String("starting-url", "", "This is the 'seed' URL for the crawler")
	crawlLimit    = flag.Uint64("crawl-limit", uint64(100), "The amount of links that the crawler should crawl")
	numThreads    = flag.Uint("num-threads", uint(1), "The number of worker threads that should be used to crawl pages")
	crawlDelay    = flag.Uint("crawl-delay", uint(1), "The number of seconds each crawler should wait in between fetching URLS")

	crawlWaitGroup sync.WaitGroup
	crawlQueue     = make(chan *url.URL, 10)
	parseQueue     = make(chan *grequests.Response, 15)
	shutdownNotify = make(chan struct{})
	urlsCrawled    uint64
	statusRequest  = make(chan os.Signal, 1)
)

const (
	// UserAgent will tell the servers who we are
	UserAgent = "Grequests Example Crawler 0.1"
)

func status() {
	for {
		select {
		case <-statusRequest:
			log.Println("Crawl Queue:", len(crawlQueue), "Parse Queue:", len(parseQueue), "URLs Crawled", atomic.LoadUint64(&urlsCrawled))
		case <-shutdownNotify:
			signal.Stop(statusRequest)
			return
		}
	}
}

func spiders() {
	crawlWaitGroup.Add(1)
	for {
		select {
		case myURL := <-crawlQueue:
			time.Sleep(time.Second * time.Duration(*crawlDelay))
			log.Println("Crawling", myURL.String())
			resp, err := grequests.Get(myURL.String(), &grequests.RequestOptions{UserAgent: UserAgent})
			if err != nil {
				log.Println("Unable to crawl URL", myURL.String(), err)
				continue
			}

			if !resp.Ok {
				log.Println("Did not receive an OK response. Got: ", resp.StatusCode, myURL.String())
				continue
			}
			atomic.AddUint64(&urlsCrawled, 1)
			parseQueue <- resp
		case <-shutdownNotify:
			crawlWaitGroup.Done()
			return

		}
	}
}

func eaters() {
	crawlWaitGroup.Add(1)
	for {
		select {
		case <-shutdownNotify:
			crawlWaitGroup.Done()
			return
		case myResp := <-parseQueue:

			if atomic.LoadUint64(&urlsCrawled) == *crawlLimit {
				log.Println("Crawl limit reached! Shutting down")
				myResp.Close()
				close(shutdownNotify)
				continue
			}

			doc, err := goquery.NewDocumentFromReader(myResp)
			if err != nil {
				log.Println("Unable to parse URL", myResp.RawResponse.Request.URL.String())
			}

			doc.Find("a").Each(func(_ int, selec *goquery.Selection) {
				uri, ok := selec.Attr("href")
				if !ok {
					return
				}
				myURL, err := url.Parse(uri)
				if err != nil {
					log.Println("Unable to parse URL", uri, err)
					return
				}
				// Ignore relative URLs
				if !myURL.IsAbs() {
					return
				}
				// Only look for HTTP or HTTPS links
				if myURL.Scheme != "http" && myURL.Scheme != "https" {
					return
				}

				crawlQueue <- myURL

			})
			myResp.Close()

		}

	}
}

func main() {
	flag.Parse()
	if *startingPoint == "" {
		flag.PrintDefaults()
		os.Exit(0)
	}

	startingURL, err := url.Parse(*startingPoint)
	if err != nil {
		log.Fatal("Unable to parse inital URL", err)
	}
	signal.Notify(statusRequest, syscall.SIGHUP)
	go status()

	crawlQueue <- startingURL

	for i := uint(0); i < *numThreads; i++ {
		go spiders()
		go eaters()
	}
	<-shutdownNotify
	crawlWaitGroup.Wait()

}
