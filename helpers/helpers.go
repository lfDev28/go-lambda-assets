package helpers

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type AssetDetail struct {
	id         int
	productId  int
	CategoryId int
}

func FetchAssetsForLocations(locationIds []int, db *sql.DB) (map[int][]AssetDetail, error) {
	// Convert the slice of location IDs into a comma-separated string.
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(locationIds)), ",")

	const baseQuery = `
        SELECT a.id AS assetId, p.id AS productId, p.categoryId
        FROM Assets AS a
        JOIN Product AS p ON a.productId = p.id
        WHERE a.locationId IN (%s)
    `

	query := fmt.Sprintf(baseQuery, placeholders)

	interfaceArgs := make([]interface{}, len(locationIds))
	for i, v := range locationIds {
		interfaceArgs[i] = v
	}

	rows, err := db.Query(query, interfaceArgs...)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	assetsMap := make(map[int][]AssetDetail)
	for rows.Next() {
		var asset AssetDetail
		var locationId int
		err = rows.Scan(&asset.id, &asset.productId, &asset.CategoryId)
		if err != nil {
			return nil, err
		}
		assetsMap[locationId] = append(assetsMap[locationId], asset)
	}

	// Check for errors from iterating over rows.
	if err = rows.Err(); err != nil {
		return nil, err
	}

	// Print the assets map

	return assetsMap, nil
}
func interfaceSlice(slice []int) []interface{} {
	s := make([]interface{}, len(slice))
	for i, v := range slice {
		s[i] = v
	}
	return s
}

func FetchChildLocations(locationIds []int, db *sql.DB) ([]int, error) {
	fmt.Println("Running child locations func")
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(locationIds)), ",")
	query := `SELECT id FROM Locations WHERE parentId IN (` + placeholders + `)`

	rows, err := db.Query(query, interfaceSlice(locationIds)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	childIds := make([]int, 0)
	for rows.Next() {
		var id int
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		childIds = append(childIds, id)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	// If no child locations are found, return the initial location IDs
	if len(childIds) == 0 {
		return locationIds, nil
	}

	grandChildLocations, err := FetchChildLocations(childIds, db)
	if err != nil {
		return nil, err
	}

	childIds = append(childIds, grandChildLocations...)

	return childIds, nil
}

type CheckGroupTemplate struct {
	id         int
	name       string
	categoryId int
}

func FetchCheckGroupTemplates(categoryIds map[int]struct{}, db *sql.DB) (map[int][]CheckGroupTemplate, error) {
	checkGroupTemplatesMap := make(map[int][]CheckGroupTemplate)

	for categoryId := range categoryIds {
		query := `SELECT id, name, categoryId FROM CheckGroupTemplates WHERE categoryId = ?`

		rows, err := db.Query(query, categoryId)
		if err != nil {
			return nil, err
		}

		var templates []CheckGroupTemplate
		for rows.Next() {
			var template CheckGroupTemplate
			err = rows.Scan(&template.id, &template.name, &template.categoryId)
			if err != nil {
				rows.Close()
				return nil, err
			}
			templates = append(templates, template)

		}
		checkGroupTemplatesMap[categoryId] = templates

	}

	return checkGroupTemplatesMap, nil
}

type CheckItemTemplate struct {
	id           int
	name         string
	checkGroupId int
}

func FetchCheckItemsTemplates(checkGroupTemplatesMap map[int][]CheckGroupTemplate, db *sql.DB) (map[int][]CheckItemTemplate, error) {
	checkItemsTemplatesMap := make(map[int][]CheckItemTemplate)
	for _, templates := range checkGroupTemplatesMap {
		for _, template := range templates {
			query := `SELECT id, name, checkGroupId FROM CheckItemsTemplate WHERE checkGroupId = ?`
			rows, err := db.Query(query, template.id)

			if err != nil {
				return nil, err
			}

			var itemsTemplates []CheckItemTemplate
			for rows.Next() {
				var itemTemplate CheckItemTemplate
				err = rows.Scan(&itemTemplate.id, &itemTemplate.name, &itemTemplate.checkGroupId)
				if err != nil {
					rows.Close()
					return nil, err
				}
				itemsTemplates = append(itemsTemplates, itemTemplate)

			}
			checkItemsTemplatesMap[template.id] = itemsTemplates
			rows.Close()

		}
	}
	return checkItemsTemplatesMap, nil

}

const batchSize = 10

func ProcessAndInsertAssets(assets []AssetDetail, checkGroupTemplatesMap map[int][]CheckGroupTemplate, checkItemTemplatesMap map[int][]CheckItemTemplate, userId string, db *sql.DB) error {

	// Split assets into batches
	for i := 0; i < len(assets); i += batchSize {
		end := i + batchSize
		if end > len(assets) {
			end = len(assets)
		}

		batch := assets[i:end]

		err := ProcessBatch(batch, checkGroupTemplatesMap, checkItemTemplatesMap, userId, db)
		if err != nil {
			return err
		}
	}

	return nil
}

func ProcessBatch(assets []AssetDetail, checkGroupTemplatesMap map[int][]CheckGroupTemplate, checkItemTemplatesMap map[int][]CheckItemTemplate, userId string, db *sql.DB) error {
	tx, err := db.Begin()

	fmt.Println("Running the processing of assets")

	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		err = tx.Commit()
	}()

	for _, asset := range assets {
		fmt.Println("Processing asset: ", asset.id)

		assetCheckQuery := `INSERT INTO AssetCheck (assetId, owner, updatedAt, status) VALUES (?, ?, ?, ?)`
		result, err := tx.Exec(assetCheckQuery, asset.id, userId, time.Now(), "pass")
		if err != nil {
			return err
		}

		assetCheckId, err := result.LastInsertId()
		if err != nil {
			return err
		}

		for _, checkGroupTemplate := range checkGroupTemplatesMap[asset.CategoryId] {
			checkGroupQuery := `INSERT INTO CheckGroup (name, assetCheckId, status) VALUES (?, ?, ?)`
			result, err := tx.Exec(checkGroupQuery, checkGroupTemplate.name, assetCheckId, "pass")
			if err != nil {
				return err
			}

			checkGroupId, err := result.LastInsertId()
			if err != nil {
				return err
			}

			for _, checkItemTemplate := range checkItemTemplatesMap[checkGroupTemplate.id] {
				checkItemQuery := `INSERT INTO CheckItems (name, checkGroupId, status, updatedAt) VALUES (?, ?, ?, ?)`
				_, err := tx.Exec(checkItemQuery, checkItemTemplate.name, checkGroupId, "pass", time.Now())
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
