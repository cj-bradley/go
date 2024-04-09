package main

import (
    "bufio"
    "database/sql"
    "fmt"
    "log"
    "os"
    "strconv"
    "strings"
    "sync"

    _ "github.com/go-sql-driver/mysql" // Import MySQL driver
)

// Define a struct to represent a row in the table
type Row map[string]interface{}

func main() {
    // Open the input file
    file, err := os.Open("input.tsv") // Assuming input file is in TSV format
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    // Open a database connection
    db, err := sql.Open("mysql", "username:password@tcp(server:port)/dbname")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Define the table name
    tableName := "your_table"

    // Define the column types (obtained from the table schema)
    colTypes := map[string]string{
        "column1": "string",
        "column2": "int",
        // Add more column names and data types as needed
    }

    // Create a scanner to read the file line by line
    scanner := bufio.NewScanner(file)
    scanner.Split(bufio.ScanLines)

    // Define the batch size
    batchSize := 5000

    // Define the concurrency limit (number of goroutines to run in parallel)
    concurrencyLimit := 10

    // Create a wait group to wait for all goroutines to finish
    var wg sync.WaitGroup

    // Channel to control the concurrency
    sem := make(chan struct{}, concurrencyLimit)

    // Loop through the file
    for scanner.Scan() {
        // Acquire a semaphore to control concurrency
        sem <- struct{}{}

        // Increment the wait group
        wg.Add(1)

        // Launch a goroutine to process each row
        go func(line string) {
            defer func() {
                // Release the semaphore when the goroutine is done
                <-sem
                // Decrement the wait group when the goroutine is done
                wg.Done()
            }()

            // Parse the line into a Row struct
            row, err := parseRow(line, colTypes)
            if err != nil {
                log.Println(err)
                return
            }

            // Insert the row into the database
            if err := insertRow(db, row, tableName); err != nil {
                log.Println(err)
            }
        }(scanner.Text())
    }

    // Wait for all goroutines to finish
    wg.Wait()

    fmt.Println("Data loading complete!")
}

// Function to parse a line from the input file into a Row struct
func parseRow(line string, colTypes map[string]string) (Row, error) {
    // Parse the line into fields (assuming TSV format)
    fields := strings.Split(line, "\t")

    // Create a new row
    row := Row{}

    // Loop through the fields and parse each one based on the column type
    for colName, colType := range colTypes {
        // Get the value from the fields slice
        valStr := fields[getIndex(colName, fields)]

        // Parse the value based on the column type
        var val interface{}
        switch colType {
        case "string":
            val = valStr
        case "int":
            valInt, err := strconv.Atoi(valStr)
            if err != nil {
                return Row{}, err
            }
            val = valInt
        // Add more cases for other data types as needed
        default:
            return Row{}, fmt.Errorf("unsupported data type: %s", colType)
        }

        // Assign the parsed value to the row
        row[colName] = val
    }

    return row, nil
}

// Function to insert a single row into the database
func insertRow(db *sql.DB, row Row, tableName string) error {
    // Prepare the INSERT statement
    query := generateInsertQuery(tableName, row)
    // Execute the INSERT statement
    _, err := db.Exec(query)
    return err
}

// Function to generate INSERT query dynamically based on the table name and row structure
func generateInsertQuery(tableName string, row Row) string {
    var columns []string
    var values []string

    // Extract column names and values from the row map
    for colName, val := range row {
        columns = append(columns, colName)
        values = append(values, fmt.Sprintf("'%v'", val))
    }

    // Construct the INSERT query
    query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(columns, ","), strings.Join(values, ","))
    return query
}

// Function to get the index of a column name in a slice of strings
func getIndex(colName string, fields []string) int {
    for i, field := range fields {
        if field == colName {
            return i
        }
    }
    return -1 // Return -1 if column name not found
}
