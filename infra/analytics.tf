# 1. Base de Datos en Glue (El contenedor de tablas)
resource "aws_glue_catalog_database" "crypto_db" {
  name = "crypto_analytics_db"
}

# 2. Workgroup de Athena (Donde se ejecutan las consultas)
resource "aws_athena_workgroup" "crypto_workgroup" {
  name = "crypto_analytics_workgroup"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_bucket.bucket}/athena-results/"
    }
  }
}

# 3. Tabla para los Nodos (Criptomonedas)
resource "aws_glue_catalog_table" "nodes_table" {
  database_name = aws_glue_catalog_database.crypto_db.name
  name          = "crypto_nodes"
  role_arn      = aws_iam_role.lambda_role.arn # Reusamos el rol por simplicidad

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "csv"
    "skip.header.line.count" = "1" # Saltamos la cabecera de Neptune (~id, etc)
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_bucket.bucket}/raw/nodes.csv"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.LazySimpleSerDe"
      parameters = {
        "field.delim" = ","
      }
    }

    columns {
      name = "id"
      type = "string"
    }
    columns {
      name = "label"
      type = "string"
    }
    columns {
      name = "name"
      type = "string"
    }
  }
}

# 4. Tabla para las Aristas (Correlaciones) - Esta es la importante para la gráfica
resource "aws_glue_catalog_table" "edges_table" {
  database_name = aws_glue_catalog_database.crypto_db.name
  name          = "crypto_correlations"
  role_arn      = aws_iam_role.lambda_role.arn

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "csv"
    "skip.header.line.count" = "1"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_bucket.bucket}/raw/edges.csv"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.LazySimpleSerDe"
      parameters = {
        "field.delim" = ","
      }
    }

    # Mapeamos las columnas del CSV de Neptune a nombres SQL amigables
    columns {
      name = "source_crypto"
      type = "string"
    }
    columns {
      name = "target_crypto"
      type = "string"
    }
    columns {
      name = "relationship"
      type = "string"
    }
    columns {
      name = "weight"
      type = "double"
    }
    columns {
      name = "lag_minutes"
      type = "int"
    }
    columns {
      name = "raw_correlation"
      type = "double"
    }
  }
}
