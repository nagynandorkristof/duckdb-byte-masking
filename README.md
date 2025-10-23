# DuckDB Byte Masking Extension

A DuckDB extension that provides byte-level data masking functionality for binary data processing.

## Overview

The Byte Masking extension allows you to extract and mask specific byte ranges from binary data stored as strings in DuckDB. This is useful for processing binary protocols.

## Features

- Extract specific byte ranges from binary data
- Support for both big-endian and little-endian byte ordering
- Flexible mask specification format
- Returns structured data with named fields for extracted bytes

## Installation

### From Repo

You can install the extension directly from the repository:

```sql
INSTALL byte_masking FROM 'https://github.com/nagynandorkristof/duckdb-byte-masking';
LOAD byte_masking;
```

Or force install to get the latest version:

```sql
FORCE INSTALL byte_masking FROM 'https://github.com/nagynandorkristof/duckdb-byte-masking';
LOAD byte_masking;
```

After installation, you can verify the extension is loaded:

```sql
SELECT * FROM duckdb_extensions() WHERE extension_name = 'byte_masking';
```

### Building from Source

1. Clone this repository:
```bash
git clone https://github.com/nagynandorkristof/duckdb-byte-masking.git
cd duckdb-byte-masking
```

2. Build the extension:
```bash
make
```

3. The extension will be built as `build/release/extension/byte_masking/byte_masking.duckdb_extension`

## Usage

### Loading the Extension

```sql
LOAD 'path/to/byte_masking.duckdb_extension';
```

### Function Syntax

```sql
SELECT mask_bytes(mask_definition, payload, [endianness]);
```

**Parameters:**
- `mask_definition` (VARCHAR): Comma-separated list of field definitions in format `name:start-end`
- `payload` (VARCHAR): Binary data as string
- `endianness` (VARCHAR, optional): Either 'big' or 'little' (default: 'big')

### Examples

```sql
-- Extract bytes 0-3 as 'header' and bytes 4-7 as 'payload'
SELECT mask_bytes('header:0-3,payload:4-7', binary_data) FROM my_table;

-- Use little-endian byte ordering
SELECT mask_bytes('field1:0-1,field2:2-3', binary_data, 'little') FROM my_table;
```

## Mask Definition Format

The mask definition string follows this format:
```
field_name:start_byte-end_byte[,field_name:start_byte-end_byte,...]
```

- `field_name`: Name for the extracted field
- `start_byte`: Starting byte position (0-based)
- `end_byte`: Ending byte position (inclusive)
- Multiple fields separated by commas

## Return Value

The function returns a STRUCT with fields corresponding to the names specified in the mask definition. Each field contains the extracted bytes as a BIGINT.

## Based on DuckDB Extension Template

This extension was created using the [DuckDB Extension Template](https://github.com/duckdb/extension-template).