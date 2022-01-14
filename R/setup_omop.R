



setup_omop <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           target_schema = "omop_athena_nci",
           steps = c("drop_tables", "copy", "log", "indices", "constraints"),
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE,
           checks = "",
           nci_version,
           owl_folder    = "/Users/mpatel/terminology/NCIT",
           neo4j_folder   = "/Users/mpatel/Desktop/NCIt/neo4j",
           omop_folder   = "/Users/mpatel/Desktop/NCIt/omop") {


    process_owl_to_omop(nci_version = nci_version,
                        owl_folder = owl_folder,
                        neo4j_folder = neo4j_folder,
                        omop_folder = omop_folder)


    path_to_csvs <-
      file.path(omop_folder,
                nci_version)
    release_version <- nci_version

    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(pg13::dc(conn = conn, verbose = verbose), add = TRUE,
              after = TRUE)
    }

    if ("drop_tables" %in% steps) {
      if (verbose) {
        cli::cat_line()
        cli::cat_boxx("Drop Tables", float = "center")
      }
      if (tolower(target_schema) %in% tolower(pg13::ls_schema(conn = conn,
                                                              verbose = verbose, render_sql = render_sql))) {
        if (verbose) {
          secretary::typewrite(sprintf("Existing '%s' schema found. Dropping tables...",
                                       target_schema))
        }
        pg13::drop_cascade(conn = conn, schema = target_schema)
        if (verbose) {
          secretary::typewrite("Tables dropped.")
        }
      }
      pg13::create_schema(conn = conn, schema = target_schema)
      if (verbose) {
        secretary::typewrite(sprintf("'%s' schema created.",
                                     target_schema))
      }
      if (verbose) {
        secretary::typewrite("Creating tables...")
      }


    schema <- target_schema
    pg13::send(conn = conn,
               sql_statement =
                 glue::glue(
              "
              --HINT DISTRIBUTE ON RANDOM
              CREATE TABLE {schema}.concept (
                concept_id			INTEGER			NOT NULL ,
                concept_name			VARCHAR(255)	NOT NULL ,
                domain_id				VARCHAR(20)		NOT NULL ,
                vocabulary_id			VARCHAR(20)		NOT NULL ,
                concept_class_id		VARCHAR(20)		NOT NULL ,
                standard_concept		VARCHAR(1)		NULL ,
                concept_code			VARCHAR(50)		NOT NULL ,
                valid_start_date		DATE			NOT NULL ,
                valid_end_date		DATE			NOT NULL ,
                invalid_reason		VARCHAR(1)		NULL
              )
              ;
              --HINT DISTRIBUTE ON RANDOM
              CREATE TABLE {schema}.vocabulary (
                vocabulary_id			VARCHAR(20)		NOT NULL,
                vocabulary_name		VARCHAR(255)	NOT NULL,
                vocabulary_reference	VARCHAR(255)	NOT NULL,
                vocabulary_version	VARCHAR(255)	NULL,
                vocabulary_concept_id	INTEGER
              )
              ;
              --HINT DISTRIBUTE ON RANDOM
              -- CREATE TABLE {schema}.domain (
              --  domain_id			    VARCHAR(20)		NOT NULL,
              --  domain_name		    VARCHAR(255)	NOT NULL,
              --  domain_concept_id		INTEGER			NOT NULL
              -- )
              -- ;
              --HINT DISTRIBUTE ON RANDOM
              CREATE TABLE {schema}.concept_class (
                concept_class_id			VARCHAR(20)		NOT NULL,
                concept_class_name		VARCHAR(255)	NOT NULL,
                concept_class_concept_id	INTEGER
              )
              ;
              --HINT DISTRIBUTE ON RANDOM
              CREATE TABLE {schema}.concept_relationship (
                concept_id_1			INTEGER			NOT NULL,
                concept_id_2			INTEGER			NOT NULL,
                relationship_id		VARCHAR(255)		NOT NULL,
                valid_start_date		DATE			NOT NULL,
                valid_end_date		DATE			NOT NULL,
                invalid_reason		VARCHAR(1)		NULL
              )
              ;
              --HINT DISTRIBUTE ON RANDOM
              CREATE TABLE {schema}.relationship (
                relationship_id			VARCHAR(255)		NOT NULL,
                relationship_name			VARCHAR(255)	NOT NULL,
                is_hierarchical			VARCHAR(1)		NOT NULL,
                defines_ancestry			VARCHAR(1)		NOT NULL,
                reverse_relationship_id	VARCHAR(20)	,
                relationship_concept_id	INTEGER
              )
              ;
              --HINT DISTRIBUTE ON RANDOM
              CREATE TABLE {schema}.concept_synonym (
                concept_id			INTEGER			NOT NULL,
                concept_synonym_name	VARCHAR(1000)	NOT NULL,
                language_concept_id	INTEGER			NOT NULL
              )
              ;
              --HINT DISTRIBUTE ON RANDOM
              CREATE TABLE {schema}.concept_ancestor (
                ancestor_concept_id		INTEGER		NOT NULL,
                descendant_concept_id		INTEGER		NOT NULL,
                min_levels_of_separation	INTEGER		NOT NULL,
                max_levels_of_separation	INTEGER		NOT NULL
              )
              ;
              "))

    if (verbose) {
      secretary::typewrite("Tables created.")
    }


    }


    if ("copy" %in% steps) {
      if (verbose) {
        cli::cat_line()
        cli::cat_boxx("Copy",
                      float = "center"
        )

        secretary::typewrite("Copying...")
      }

      vocabulary_files <-
        c(
          'CONCEPT_ANCESTOR.csv',
          'CONCEPT_CLASS.csv',
          'CONCEPT_RELATIONSHIP.csv',
          'CONCEPT_SYNONYM.csv',
          'CONCEPT.csv',
          'RELATIONSHIP.csv',
          'VOCABULARY.csv'
        )

      table_names <-
      xfun::sans_ext(vocabulary_files)

      paths_to_csvs <-
        path.expand(file.path(
          path_to_csvs,
          vocabulary_files
        ))

      errors <- vector()
      for (i in seq_along(paths_to_csvs)) {
        vocabulary_file <- paths_to_csvs[i]
        table_name <- table_names[i]


        sql <- SqlRender::render("COPY @schema.@tableName FROM '@vocabulary_file' CSV HEADER QUOTE E'\\b' NULL AS '';",
                                 schema = target_schema,
                                 tableName = table_name,
                                 vocabulary_file = vocabulary_file
        )

        output <-
          tryCatch(pg13::send(
            conn = conn,
            sql_statement = sql,
            verbose = verbose,
            render_sql = render_sql
          ),
          error = function(e) "Error"
          )

        if ((length(output) == 1) && (output %in% "Error")) {
          errors <-
            c(errors, table_name)
        }
      }

      if (length(errors)) {
        secretary::typewrite(
          secretary::enbold(secretary::redTxt("WARNING:")),
          "The following tables failed to load:"
        )

        errors %>%
          purrr::map(~ secretary::typewrite(.,
                                            tabs = 4,
                                            timepunched = FALSE
          ))
      } else {
        secretary::typewrite("All tables copied successfully:")
        table_names %>%
          purrr::map(~ secretary::typewrite(.,
                                            tabs = 4,
                                            timepunched = FALSE
          ))
      }
    }





  }
