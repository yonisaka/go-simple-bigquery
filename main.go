package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

func main() {
	projectID := "lacrose-d73a9"
	if projectID == "" {
		fmt.Println("GOOGLE_CLOUD_PROJECT environment variable must be set.")
		os.Exit(1)
	}

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	rows, err := query(ctx, client)
	if err != nil {
		log.Fatal(err)
	}
	if err := printResults(os.Stdout, rows); err != nil {
		log.Fatal(err)
	}
}

// query returns a row iterator suitable for reading query results.
func query(ctx context.Context, client *bigquery.Client) (*bigquery.RowIterator, error) {

	query := client.Query(
		`SELECT
			country_name, alpha_2_code
		FROM ` + "`bigquery-public-data.country_codes.country_codes`" + `
		ORDER BY country_name DESC
		LIMIT 20;`)
	return query.Read(ctx)
}

type Country struct {
	CountryName string `bigquery:"country_name"`
	Alpha2Code  string `bigquery:"alpha_2_code"`
}

// printResults prints results from a query to the Country Code public dataset.
func printResults(w io.Writer, iter *bigquery.RowIterator) error {
	for {
		var row Country
		err := iter.Next(&row)
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return fmt.Errorf("error iterating through results: %w", err)
		}

		fmt.Fprintf(w, "Country Name: %s Alpha 2 Code: %s\n", row.CountryName, row.Alpha2Code)
	}
}
