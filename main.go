package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"encoding/json"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	bigquery "google.golang.org/api/bigquery/v2"
)

const (
	minTimestamp = 1493596800000 // 1st May 2017
)

var (
	projectID = os.Getenv("GOOGLE_PROJECT_ID")
)

type myJob struct {
	UserEmail           string `json:"user_email"`
	State               string `json:"state"`
	Query               string `json:"query"`
	ID                  string `json:"id"`
	CreationTime        int64  `json:"creation_time"`
	StartTime           int64  `json:"start_time"`
	EndTime             int64  `json:"end_time"`
	TotalBytesProcessed int64  `json:"total_bytes_processed"`
}

func newMyJob(job *bigquery.JobListJobs) (*myJob, error) {
	if job.Configuration.Query == nil {
		return nil, fmt.Errorf("Not a query (%v)", job.Id)
	}
	return &myJob{
		UserEmail:           job.UserEmail,
		State:               job.Status.State,
		Query:               job.Configuration.Query.Query,
		ID:                  job.Id,
		CreationTime:        job.Statistics.CreationTime,
		StartTime:           job.Statistics.StartTime,
		EndTime:             job.Statistics.EndTime,
		TotalBytesProcessed: job.Statistics.TotalBytesProcessed,
	}, nil
}

func (j *myJob) toJSONString() []byte {
	bs, _ := json.Marshal(j)
	return bs
}

func callBQAPI(js *bigquery.JobsService, w io.Writer, pageToken string) error {
	log.Println(time.Now())
	list, err := js.List(projectID).PageToken(pageToken).AllUsers(true).Projection("full").Do()

	if err != nil {
		panic(fmt.Errorf("call: %v", err))
	}

	for _, job := range list.Jobs {
		if job.Statistics.CreationTime < minTimestamp {
			return nil
		}
		if mj, err := newMyJob(job); err == nil {
			w.Write(mj.toJSONString())
			w.Write([]byte("\n"))
		}
	}
	return callBQAPI(js, w, list.NextPageToken)
}

func main() {
	keyContent, err := ioutil.ReadFile(fmt.Sprintf("%v/%v", os.Getenv("HOME"), ".ssh/google.json"))
	if err != nil {
		panic(fmt.Errorf("key: %v", err))
	}
	conf, err := google.JWTConfigFromJSON(keyContent, []string{bigquery.BigqueryScope}...)
	if err != nil {
		panic(err)
	}
	oauthHTTPClient := conf.Client(oauth2.NoContext)
	bq, err := bigquery.New(oauthHTTPClient)
	if err != nil {
		panic(fmt.Errorf("auth: %v", err))
	}
	js := bigquery.NewJobsService(bq)
	file, err := os.Create("./jobs.json")
	if err != nil {
		panic(fmt.Errorf("file: %v", err))
	}
	if err := callBQAPI(js, file, ""); err != nil {
		panic(fmt.Errorf("call: %v", err))
	}
}
