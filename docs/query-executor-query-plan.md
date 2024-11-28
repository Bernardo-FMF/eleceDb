# Query executor

A query executor represents a query that doesn't need a query plan to be executed.
These queries are related to internal database management, like creating/deleting the database, creating/deleting a
table or creating indexes.

Class hierarchy:

- `QueryExecutor`
    - `CreateDbQueryExecutor` - Creates a database.
    - `CreateTableQueryExecutor` - Creates a table.
    - `CreateIndexQueryExecutor` - Creates an index.
    - `DropDbQueryExecutor` - Deletes a database.
    - `DropTableQueryExecutor` - Deletes a table.

# Query plan

A query plan represents the sequence of steps used by the database to fully process the query.
These queries are related to table operations - select/insert/delete/update operations on rows.

## Select query plan (`SelectQueryPlan`)

The `SelectQueryPlan` class represents a detailed execution plan for performing select queries. This plan orchestrates
the different steps that a select query goes through, like scanning possible valid rows, filtering rows, selection of
columns, ordering, and streaming the results.

- **Scan Steps (scanSteps)**: These steps scan the database to retrieve rows potentially matching the query criteria.
  There are 4 types of possible scans:
    - **Equality scan**: This can only be used on indexed columns, it uses the b+ tree to obtain the single row the
      query
      can result in.
    - **Inequality scan**: This can only be used on indexed columns, it uses the b+ tree to obtain all rows from a table
      except, at most, 1.
    - **Range scan**: This can only be used on indexed columns of type integer, it manipulates the tree to obtain the
      rows
      in an efficient manner, by locating the initial node where the start of the range is, and reads the next nodes
      until the end of the range.
    - **Sequential scan**: This uses the clustered column, to iterate through the whole table row by row.
- **Filter Steps (filterSteps)**: These steps filter the retrieved rows based on certain conditions.
- **Start and End Tracer Steps (startTracerStep, endTracerStep)**: These steps mark the beginning and the end of the
  query execution, where the first message sent to the client contains the selected columns, and after the results we
  also send the row count.
- **Selector Step (selectorStep)**: This step selects specific columns from the retrieved rows as specified by the
  query.
- **Order Step (orderStep)**: This step handles ordering of results based on specified columns, using in-memory buffers
  and temporary files if necessary.
- **Deserializer Step (deserializerStep)**: This step deserializes the selected columns into a readable format.
- **Stream Step (streamStep)**: This step streams the processed results to the client.