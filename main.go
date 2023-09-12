package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"go-lambda-assets/helpers"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

// Initialiser function to intialise the env

var db *sql.DB

func initDb() error {
	dsn := os.Getenv("DSN")

	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal("Error connecting to database", err)
	}

	return nil

}

type Request struct {
	LocationId int    `json:"locationId"`
	UserId     string `json:"userId"`
}

func Handler(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	// Check for an existing db connection before running the query
	err := db.Ping()
	if err != nil {
		err = initDb()
		if err != nil {
			return events.APIGatewayProxyResponse{
				StatusCode: http.StatusInternalServerError,
				Body:       "Failed to connect to the database",
			}, err
		}

	}

	// From here the global variable db is available to use
	// Now we get JSON unmarshall the body
	bodyReader := strings.NewReader(request.Body)
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Body:       "Failed to read the request body",
		}, err
	}

	// Now we have the body, we can unmarshall it into a struct
	var req Request
	err = json.Unmarshal(body, &req)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Body:       "Failed to unmarshall the request body",
		}, err
	}

	// Now we have the request body, we can run the query
	initialLocation := []int{req.LocationId}

	allLocations, err := helpers.FetchChildLocations(initialLocation, db)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Failed to fetch the child locations",
		}, err

	}

	locationMap := make(map[int]struct{})
	for _, loc := range allLocations {
		locationMap[loc] = struct{}{}
	}

	assetsMap, err := helpers.FetchAssetsForLocations(allLocations, db)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Failed to fetch the assets",
		}, err
	}

	// Now we have the assets, we can fetch the categories
	var allAssets []helpers.AssetDetail
	for _, assets := range assetsMap {
		allAssets = append(allAssets, assets...)
	}

	categoryIds := make(map[int]struct{})
	for _, asset := range allAssets {
		categoryIds[asset.CategoryId] = struct{}{}
	}

	checkGroupTemplatesMap, err := helpers.FetchCheckGroupTemplates(categoryIds, db)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Failed to fetch the check group templates",
		}, err
	}

	// Now we have the check group templates, we can fetch the checks
	checkItemTemplatesMap, err := helpers.FetchCheckItemsTemplates(checkGroupTemplatesMap, db)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Failed to fetch the check item templates",
		}, err
	}

	err = helpers.ProcessAndInsertAssets(allAssets, checkGroupTemplatesMap, checkItemTemplatesMap, req.UserId, db)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Failed to process and insert the assets",
		}, err
	}

	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
		Body:       "Successfully processed and inserted the assets",
	}, nil

}

func main() {
	lambda.Start(Handler)
}
