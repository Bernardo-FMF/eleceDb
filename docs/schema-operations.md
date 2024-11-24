# Schema operations

In this document, we'll be looking at the following database level operations, and how they operate internally:

1. [Create Database](#create-database)
2. [Delete Database](#delete-database)
3. [Create Table](#create-table)
4. [Delete Table](#delete-table)
5. [Create Index](#create-index)

## `Create Database`

### Method

`SchemaManager::createSchema`

### Description

This method creates a new database using the specified name. The database is only created if a database with the same
name does not already exist.

## `Delete Database`

### Method

`SchemaManager::deleteSchema`

### Description

This method deletes the specified database. The process involves calling `SchemaManager::deleteTable` for all existing
tables within the database. The total number of rows deleted is aggregated and sent to the client. Finally, the schema
file is updated to remove the database.

## `Create Table`

### Method

`SchemaManager::createTable`

### Description

This method persists a new table to the schema.

### Process

1. Obtain a unique global ID for the new table.
2. For each column, create the necessary indexes:
    - Implicit `cluster_index` to hold internal row IDs.
    - Indexes for the primary key and any unique columns.

## `Delete Table`

### Method

`SchemaManager::deleteTable`

### Description

This method deletes the specified table from the schema.

### Process

1. Remove all index nodes from the implicit `cluster_index`:
    - Iterate through all nodes to obtain the disk reference pointer for each row and delete the row.
    - Finally, delete all nodes from the clustered index.
2. Remove all other indexes:
    - Only the index nodes need to be deleted since the rows are already deleted when the cluster index is removed.

## `Create Index`

### Method

`SchemaManager::createIndex`

### Description

This method accounts for existing rows in the table when creating a new index.

### Process

1. Iterate through the clustered index.
2. For each disk pointer, read the full row from the disk to obtain the values of the columns being indexed.
3. Add each value, persisting the index headers to disk.