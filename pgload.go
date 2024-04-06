package main

import (
    "bufio"
    "database/sql"
    "fmt"
    "log"
    "os"
    "strconv"
    "strings"

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

    // Define a slice to store rows in the current chunk
    var rows []Row

    // Loop through the file
    for scanner.Scan() {
        // Parse the line into a Row struct
        row, err := parseRow(scanner.Text(), colTypes)
        if err != nil {
            log.Fatal(err)
        }

        // Add the row to the slice
        rows = append(rows, row)

        // If the slice size reaches the batch size, insert the rows into the database
        if len(rows) == batchSize {
            if err := insertRows(db, rows, tableName); err != nil {
                log.Fatal(err)
            }
            // Clear the slice for the next batch
            rows = nil
        }
    }

    // Insert any remaining rows
    if len(rows) > 0 {
        if err := insertRows(db, rows, tableName); err != nil {
            log.Fatal(err)
        }
    }

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }

    fmt.Println("Data loading complete!")
}

// Function to parse a line from the input file into a Row struct
func parseRow(line string, colTypes map[string]string) (Row, error) {
    // Split the line into fields (assuming TSV format)
    fields := strings.Split(line, "\t")

    // Create a new row
    row := Row{}

    // Loop through the fields and parse each one based on the column type
    for colName, colType := range colTypes {
        // Get the index of the column name in the colTypes map
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
        case "float64":
            valFloat, err := strconv.ParseFloat(valStr, 64)
            if err != nil {
                return Row{}, err
            }
            val = valFloat
        // Add more cases for other data types as needed
        default:
            return Row{}, fmt.Errorf("unsupported data type: %s", colType)
        }

        // Assign the parsed value to the row
        row[colName] = val
    }

    return row, nil
}

// Function to insert rows into the database
func insertRows(db *sql.DB, rows []Row, tableName string) error {
    // Begin a transaction
    tx, err := db.Begin()
    if err != nil {
        return err
    }
    defer tx.Rollback()

    // Prepare the SQL statement
    query := generateInsertQuery(tableName, rows[0]) // Generate the INSERT query
    stmt, err := tx.Prepare(query)
    if err != nil {
        return err
    }
    defer stmt.Close()

    // Loop through the rows and execute the statement for each row
    for _, row := range rows {
        // Extract values from the row map
        var values []interface{}
        for _, val := range row {
            values = append(values, val)
        }

        // Execute the prepared statement with the values
        _, err := stmt.Exec(values...)
        if err != nil {
            return err
        }
    }

    // Commit the transaction
    if err := tx.Commit(); err != nil {
        return err
    }

    return nil
}

// Function to generate INSERT query dynamically based on the table name and row structure
func generateInsertQuery(tableName string, row Row) string {
    var columns []string
    var placeholders []string

    // Extract column names and placeholders from the row map
    for colName := range row {
        columns = append(columns, colName)
        placeholders = append(placeholders, "?")
    }

    // Construct the INSERT query
    query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(columns, ","), strings.Join(placeholders, ","))
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
