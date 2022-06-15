#' @title
#' Setup OMOP
#'
#' @description
#' Instantiate a Postgres OMOP Vocabulary
#' database from the csvs created by the `process_owl_to_omop`
#' function.
#'
#' @rdname setup_omop
#' @export
#' @import tidyverse
#' @import rlang
#' @import pg13
#' @import cli
#' @import secretary
#' @import glue
#' @importFrom xfun sans_ext
#' @importFrom prettyunits pretty_dt



setup_omop <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           target_schema = "omop_athena_nci",
           steps = c("drop_tables", "copy", "log", "indices", "constraints"),
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE,
           checks = "",
           log_schema = "public",
           log_table = "setup_nci_omop_log") {

    path_to_csvs <-
    system.file(
      package = "setupNCI",
      "data",
      "omop",
      "current"
    )

    current_file <-
      "inst/data/omop/current/log.json"

    current_log <-
      jsonlite::read_json(
        path = current_file,
        simplifyVector = TRUE
      )

    nci_version <- current_log$nci_version
    release_version <- nci_version

    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(pg13::dc(conn = conn, verbose = verbose),
        add = TRUE,
        after = TRUE
      )
    }


    if ("drop_tables" %in% steps) {
      if (verbose) {
        cli::cat_line()
        cli::cat_boxx("Drop Tables", float = "center")
      }
      if (tolower(target_schema) %in% tolower(pg13::ls_schema(
        conn = conn,
        verbose = verbose, render_sql = render_sql
      ))) {
        if (verbose) {
          secretary::typewrite(sprintf(
            "Existing '%s' schema found. Dropping tables...",
            target_schema
          ))
        }
        pg13::drop_cascade(conn = conn, schema = target_schema)
        if (verbose) {
          secretary::typewrite("Tables dropped.")
        }
      }
      pg13::create_schema(conn = conn, schema = target_schema)
      if (verbose) {
        secretary::typewrite(sprintf(
          "'%s' schema created.",
          target_schema
        ))
      }
      if (verbose) {
        secretary::typewrite("Creating tables...")
      }


      schema <- target_schema
      pg13::send(
        conn = conn,
        sql_statement =
          glue::glue(
            "
              --HINT DISTRIBUTE ON RANDOM
              CREATE TABLE {schema}.concept (
                concept_id			BIGINT			NOT NULL ,
                concept_name			text	NOT NULL ,
                domain_id				VARCHAR(20)		NOT NULL ,
                vocabulary_id			VARCHAR(20)		NOT NULL ,
                concept_class_id		VARCHAR(20)		NOT NULL ,
                standard_concept		VARCHAR(1)		NULL ,
                concept_code			VARCHAR(50)		NOT NULL ,
                valid_start_date		DATE			NOT NULL ,
                valid_end_date		DATE			NOT NULL ,
                invalid_reason		VARCHAR(5)		NULL
              )
              ;
              --HINT DISTRIBUTE ON RANDOM
              CREATE TABLE {schema}.vocabulary (
                vocabulary_id			VARCHAR(20)		NOT NULL,
                vocabulary_name		VARCHAR(255)	NOT NULL,
                vocabulary_reference	VARCHAR(255)	NOT NULL,
                vocabulary_version	VARCHAR(255)	NULL,
                vocabulary_concept_id	BIGINT
              )
              ;
              --HINT DISTRIBUTE ON RANDOM
              -- CREATE TABLE {schema}.domain (
              --  domain_id			    VARCHAR(20)		NOT NULL,
              --  domain_name		    VARCHAR(255)	NOT NULL,
              --  domain_concept_id		BIGINT			NOT NULL
              -- )
              -- ;
              --HINT DISTRIBUTE ON RANDOM
              CREATE TABLE {schema}.concept_class (
                concept_class_id			VARCHAR(20)		NOT NULL,
                concept_class_name		VARCHAR(255)	NOT NULL,
                concept_class_concept_id	BIGINT
              )
              ;
              --HINT DISTRIBUTE ON RANDOM
              CREATE TABLE {schema}.concept_relationship (
                concept_id_1			BIGINT			NOT NULL,
                concept_id_2			BIGINT			NOT NULL,
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
                relationship_concept_id	VARCHAR(20)
              )
              ;
              --HINT DISTRIBUTE ON RANDOM
              CREATE TABLE {schema}.concept_synonym (
                concept_id			BIGINT			NOT NULL,
                concept_synonym_name	text	NOT NULL,
                language_concept_id	BIGINT			NOT NULL
              )
              ;
              --HINT DISTRIBUTE ON RANDOM
              CREATE TABLE {schema}.concept_ancestor (
                ancestor_concept_id		BIGINT		NOT NULL,
                descendant_concept_id		BIGINT		NOT NULL,
                min_levels_of_separation	INTEGER		NOT NULL,
                max_levels_of_separation	INTEGER		NOT NULL
              )
              ;
              "
          )
      )

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
          "CONCEPT_ANCESTOR.csv",
          "CONCEPT_CLASS.csv",
          "CONCEPT_RELATIONSHIP.csv",
          "CONCEPT_SYNONYM.csv",
          "CONCEPT.csv",
          "RELATIONSHIP.csv",
          "VOCABULARY.csv"
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


        sql <-
          glue::glue(
            "COPY {target_schema}.{table_name}
            FROM '{vocabulary_file}' CSV HEADER QUOTE E'\"' NULL AS '';"
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


      duplicate_rows <-
        pg13::query(
          conn = conn,
          sql_statement =
            glue::glue(
              "
        select c.*
        from {target_schema}.concept c
        INNER JOIN (
        select concept_code,COUNT(*)
        from {target_schema}.concept
        group by concept_code
        having COUNT(*) > 1
        ) c2
        on c2.concept_code = c.concept_code
        order by c.concept_code
        ;
        "
            )
        )

      if (nrow(duplicate_rows) > 0) {
        secretary::typewrite(
          secretary::enbold(secretary::redTxt("WARNING:")),
          glue::glue(
            "
          Duplicate rows detected!
              select c.*
              from {target_schema}.concept c
              INNER JOIN (
              select concept_code,COUNT(*)
              from {target_schema}.concept
              group by concept_code
              having COUNT(*) > 1
              ) c2
              on c2.concept_code = c.concept_code
              order by c.concept_code
              ;
          "
          )
        )
      } else {
        secretary::typewrite(
          "No duplicates in the CONCEPT table detected. The
          unique `concept_id` count equals the unique
          `concept_code` count.")
      }
    }

    duplicate_rows <-
      pg13::query(
        conn = conn,
        sql_statement =
          glue::glue(
            "
        WITH dupes AS (
        SELECT dupe.ancestor_concept_id,dupe.descendant_concept_id,COUNT(*)
        FROM {target_schema}.concept_ancestor dupe
        GROUP BY dupe.ancestor_concept_id,dupe.descendant_concept_id
        HAVING COUNT(*)>1
        )

        SELECT ca.*, dupes.count
        FROM {target_schema}.concept_ancestor ca
        INNER JOIN dupes
        ON dupes.ancestor_concept_id = ca.ancestor_concept_id
        AND dupes.descendant_concept_id = ca.descendant_concept_id
        ORDER BY ca.ancestor_concept_id,ca.descendant_concept_id
        ;
        "
          )
      )

    if (nrow(duplicate_rows) > 0) {
      secretary::typewrite(
        secretary::enbold(secretary::redTxt("WARNING:")),
        glue::glue(
          "
          Duplicate rows detected!
        WITH dupes AS (
        SELECT dupe.ancestor_concept_id,dupe.descendant_concept_id,COUNT(*)
        FROM {target_schema}.concept_ancestor dupe
        GROUP BY dupe.ancestor_concept_id,dupe.descendant_concept_id
        HAVING COUNT(*)>1
        )

        SELECT ca.*, dupes.count
        FROM {target_schema}.concept_ancestor ca
        INNER JOIN dupes
        ON dupes.ancestor_concept_id = ca.ancestor_concept_id
        AND dupes.descendant_concept_id = ca.descendant_concept_id
        ORDER BY ca.ancestor_concept_id,ca.descendant_concept_id
        ;
          "
        )
      )
    } else {
      secretary::typewrite(
        "No duplicates in the CONCEPT_ANCESTOR table detected. Each `ancestor_concept_id`
        and `descendant_concept_id` combination maps to 1 unique `min_levels_of_separation`
        and `max_levels_of_separation` combination."
        )
    }

    if ("log" %in% steps) {
      if (verbose) {
        cli::cat_line()
        cli::cat_boxx("Log",
          float = "center"
        )
        secretary::typewrite("Logging...")
      }

      new_tables <-
        pg13::ls_tables(
          conn = conn,
          schema = target_schema,
          verbose = verbose,
          render_sql = TRUE
        )

      log_list <-
        vector(
          mode = "list",
          length = length(new_tables)
        ) %>%
        set_names(new_tables)

      for (new_table in new_tables) {

        log_list[[new_table]] <-
          pg13::query(
            conn = conn,
            sql_statement =
              glue::glue("SELECT COUNT(*) FROM {target_schema}.{new_table};"),
            verbose = verbose,
            render_sql = render_sql,
            checks = checks
          ) %>%
          unlist() %>%
          unname()
      }

      log_list2 <-
        log_list %>%
        tibble::enframe(
          name = "Table",
          value = "Row Count"
        ) %>%
        mutate(`Row Count` = unlist(`Row Count`))

      print(log_list2)

      new_log_entry <-
        dplyr::bind_cols(
          tibble(
            so_datetime = as.character(Sys.time()),
            so_schema = target_schema,
            so_nci_version = nci_version
          ),
          log_list2 %>%
            tidyr::pivot_wider(
              names_from = Table,
              values_from = "Row Count"
            )
        )

      table_exists <-
        pg13::table_exists(
          conn = conn,
          schema = log_schema,
          table_name = log_table
        )


      if (!table_exists) {
        pg13::send(
          conn = conn,
          sql_statement =
            glue::glue(
              "
              CREATE TABLE {log_schema}.{log_table} (
                so_datetime TIMESTAMP without time zone,
                so_schema    varchar(25),
                so_nci_version varchar(10),
                concept integer,
                concept_ancestor integer,
                concept_class integer,
                concept_relationship integer,
                concept_synonym integer,
                relationship integer,
                vocabulary integer
              )
              ;
              "
            ),
          verbose = verbose,
          render_sql = render_sql
        )
      }


      new_log_entry <-
        unlist(new_log_entry) %>%
        unname()

      pg13::send(
        conn = conn,
        sql_statement =
          glue::glue(
            "INSERT INTO {log_schema}.{log_table} VALUES({glue::glue_collapse(glue::single_quote(new_log_entry),sep = \",\")});"
          ),
        verbose = verbose,
        render_sql = render_sql
      )
    }

    if ("indices" %in% steps) {
      if (verbose) {
        cli::cat_line()
        cli::cat_boxx("Indices",
          float = "center"
        )
        secretary::typewrite("Executing indexes...")
      }



      sql <-
        glue::glue(
          "
        ALTER TABLE {schema}.concept ADD CONSTRAINT xpk_concept PRIMARY KEY (concept_id);
        ALTER TABLE {schema}.vocabulary ADD CONSTRAINT xpk_vocabulary PRIMARY KEY (vocabulary_id);
        ALTER TABLE {schema}.concept_class ADD CONSTRAINT xpk_concept_class PRIMARY KEY (concept_class_id);
        ALTER TABLE {schema}.concept_relationship ADD CONSTRAINT xpk_concept_relationship PRIMARY KEY (concept_id_1,concept_id_2,relationship_id);
        ALTER TABLE {schema}.relationship ADD CONSTRAINT xpk_relationship PRIMARY KEY (relationship_id);
        -- ALTER TABLE {schema}.concept_ancestor ADD CONSTRAINT xpk_concept_ancestor PRIMARY KEY (ancestor_concept_id,descendant_concept_id);
        CREATE UNIQUE INDEX idx_concept_concept_id ON {schema}.concept (concept_id ASC);
        CLUSTER {schema}.concept USING idx_concept_concept_id ;
        CREATE INDEX idx_concept_code ON {schema}.concept (concept_code ASC);
        CREATE INDEX idx_concept_vocabluary_id ON {schema}.concept (vocabulary_id ASC);
        CREATE INDEX idx_concept_domain_id ON {schema}.concept (domain_id ASC);
        CREATE INDEX idx_concept_class_id ON {schema}.concept (concept_class_id ASC);
        CREATE INDEX idx_concept_id_varchar ON {schema}.concept (CAST(concept_id AS VARCHAR));
        CREATE UNIQUE INDEX idx_vocabulary_vocabulary_id ON {schema}.vocabulary (vocabulary_id ASC);
        CLUSTER {schema}.vocabulary USING idx_vocabulary_vocabulary_id ;
        CREATE UNIQUE INDEX idx_concept_class_class_id ON {schema}.concept_class (concept_class_id ASC);
        CLUSTER {schema}.concept_class USING idx_concept_class_class_id ;
        CREATE INDEX idx_concept_relationship_id_1 ON {schema}.concept_relationship (concept_id_1 ASC);
        CREATE INDEX idx_concept_relationship_id_2 ON {schema}.concept_relationship (concept_id_2 ASC);
        CREATE INDEX idx_concept_relationship_id_3 ON {schema}.concept_relationship (relationship_id ASC);
        CREATE UNIQUE INDEX idx_relationship_rel_id ON {schema}.relationship (relationship_id ASC);
        CLUSTER {schema}.relationship USING idx_relationship_rel_id ;
        CREATE INDEX idx_concept_synonym_id ON {schema}.concept_synonym (concept_id ASC);
        CLUSTER {schema}.concept_synonym USING idx_concept_synonym_id ;
        CREATE INDEX idx_concept_ancestor_id_1 ON {schema}.concept_ancestor (ancestor_concept_id ASC);
        CLUSTER {schema}.concept_ancestor USING idx_concept_ancestor_id_1 ;
        CREATE INDEX idx_concept_ancestor_id_2 ON {schema}.concept_ancestor (descendant_concept_id ASC);
        "
        )


      sql_statements <-
        strsplit(
          x = sql,
          split = ";"
        ) %>%
        unlist() %>%
        trimws(which = "both")

      start_time <- Sys.time()
      i <- 0
      for (sql_statement in sql_statements) {
        i <- i + 1
        Sys.sleep(0.5)
        pg13::send(
          conn = conn,
          checks = "",
          sql_statement = sql_statement,
          render_sql = render_sql,
          verbose = FALSE
        )
        Sys.sleep(0.5)
        indices_time <-
          difftime(
            Sys.time(),
            start_time
          )
        indices_time <-
          prettyunits::pretty_dt(indices_time)

        percent_progress <-
          paste0(
            formatC(round(i / length(sql_statements) * 100, digits = 1),
              format = "f",
              digits = 1
            ), "%"
          )

        secretary::typewrite(glue::glue("{percent_progress} completed..."))
        secretary::typewrite(glue::glue("{indices_time} elapsed..."))
      }

      stop_time <- Sys.time()

      indices_time <-
        difftime(
          stop_time,
          start_time
        )

      indices_time <-
        prettyunits::pretty_dt(indices_time)

      secretary::typewrite(glue::glue("Indices complete! [{indices_time}]"))
    }

    if ("constraints" %in% steps) {
      if (verbose) {
        cli::cat_line()
        cli::cat_boxx("Constraints",
          float = "center"
        )
        secretary::typewrite("Executing constraints...")
      }


      sql <- glue::glue(
        "
        ALTER TABLE {schema}.concept ADD CONSTRAINT fpk_concept_domain FOREIGN KEY (domain_id)  REFERENCES domain (domain_id);
        ALTER TABLE {schema}.concept ADD CONSTRAINT fpk_concept_class FOREIGN KEY (concept_class_id)  REFERENCES concept_class (concept_class_id);
        ALTER TABLE {schema}.concept ADD CONSTRAINT fpk_concept_vocabulary FOREIGN KEY (vocabulary_id)  REFERENCES vocabulary (vocabulary_id);
        ALTER TABLE {schema}.vocabulary ADD CONSTRAINT fpk_vocabulary_concept FOREIGN KEY (vocabulary_concept_id)  REFERENCES concept (concept_id);
        ALTER TABLE {schema}.domain ADD CONSTRAINT fpk_domain_concept FOREIGN KEY (domain_concept_id)  REFERENCES concept (concept_id);
        ALTER TABLE {schema}.concept_class ADD CONSTRAINT fpk_concept_class_concept FOREIGN KEY (concept_class_concept_id)  REFERENCES concept (concept_id);
        ALTER TABLE {schema}.concept_relationship ADD CONSTRAINT fpk_concept_relationship_c_1 FOREIGN KEY (concept_id_1)  REFERENCES concept (concept_id);
        ALTER TABLE {schema}.concept_relationship ADD CONSTRAINT fpk_concept_relationship_c_2 FOREIGN KEY (concept_id_2)  REFERENCES concept (concept_id);
        ALTER TABLE {schema}.concept_relationship ADD CONSTRAINT fpk_concept_relationship_id FOREIGN KEY (relationship_id)  REFERENCES relationship (relationship_id);
        ALTER TABLE {schema}.relationship ADD CONSTRAINT fpk_relationship_concept FOREIGN KEY (relationship_concept_id)  REFERENCES concept (concept_id);
        ALTER TABLE {schema}.relationship ADD CONSTRAINT fpk_relationship_reverse FOREIGN KEY (reverse_relationship_id)  REFERENCES relationship (relationship_id);
        ALTER TABLE {schema}.concept_synonym ADD CONSTRAINT fpk_concept_synonym_concept FOREIGN KEY (concept_id)  REFERENCES concept (concept_id);
        ALTER TABLE {schema}.concept_synonym ADD CONSTRAINT fpk_concept_synonym_language_concept FOREIGN KEY (language_concept_id)  REFERENCES concept (concept_id);
        ALTER TABLE {schema}.concept_ancestor ADD CONSTRAINT fpk_concept_ancestor_concept_1 FOREIGN KEY (ancestor_concept_id)  REFERENCES concept (concept_id);
        ALTER TABLE {schema}.concept_ancestor ADD CONSTRAINT fpk_concept_ancestor_concept_2 FOREIGN KEY (descendant_concept_id)  REFERENCES concept (concept_id);
        ALTER TABLE {schema}.source_to_concept_map ADD CONSTRAINT fpk_source_to_concept_map_v_1 FOREIGN KEY (source_vocabulary_id)  REFERENCES vocabulary (vocabulary_id);
        ALTER TABLE {schema}.source_to_concept_map ADD CONSTRAINT fpk_source_to_concept_map_v_2 FOREIGN KEY (target_vocabulary_id)  REFERENCES vocabulary (vocabulary_id);
        ALTER TABLE {schema}.source_to_concept_map ADD CONSTRAINT fpk_source_to_concept_map_c_1 FOREIGN KEY (target_concept_id)  REFERENCES concept (concept_id);
        ALTER TABLE {schema}.drug_strength ADD CONSTRAINT fpk_drug_strength_concept_1 FOREIGN KEY (drug_concept_id)  REFERENCES concept (concept_id);
        ALTER TABLE {schema}.drug_strength ADD CONSTRAINT fpk_drug_strength_concept_2 FOREIGN KEY (ingredient_concept_id)  REFERENCES concept (concept_id);
        ALTER TABLE {schema}.drug_strength ADD CONSTRAINT fpk_drug_strength_unit_1 FOREIGN KEY (amount_unit_concept_id)  REFERENCES concept (concept_id);
        ALTER TABLE {schema}.drug_strength ADD CONSTRAINT fpk_drug_strength_unit_2 FOREIGN KEY (numerator_unit_concept_id)  REFERENCES concept (concept_id);
        ALTER TABLE {schema}.drug_strength ADD CONSTRAINT fpk_drug_strength_unit_3 FOREIGN KEY (denominator_unit_concept_id)  REFERENCES concept (concept_id);
        ALTER TABLE {schema}.concept_synonym ADD CONSTRAINT uq_concept_synonym UNIQUE (concept_id, concept_synonym_name, language_concept_id);
        ALTER TABLE {schema}.concept ADD CONSTRAINT chk_c_concept_name CHECK (concept_name <> '');
        ALTER TABLE {schema}.concept ADD CONSTRAINT chk_c_standard_concept CHECK (COALESCE(standard_concept,'C') in ('C','S'));
        ALTER TABLE {schema}.concept ADD CONSTRAINT chk_c_concept_code CHECK (concept_code <> '');
        ALTER TABLE {schema}.concept ADD CONSTRAINT chk_c_invalid_reason CHECK (COALESCE(invalid_reason,'D') in ('D','U'));
        ALTER TABLE {schema}.concept_relationship ADD CONSTRAINT chk_cr_invalid_reason CHECK (COALESCE(invalid_reason,'D')='D');
        ALTER TABLE {schema}.concept_synonym ADD CONSTRAINT chk_csyn_concept_synonym_name CHECK (concept_synonym_name <> '');
        "
      )


      sql_statements <-
        strsplit(
          x = sql,
          split = ";"
        ) %>%
        unlist() %>%
        trimws(which = "both")

      start_time <- Sys.time()

      for (i in seq_along(sql_statements)) {
        tryCatch(
          pg13::send(
            conn = conn,
            sql_statement = sql_statements[i],
            verbose = verbose,
            render_sql = render_sql
          ),
          error = function(e) NULL
        )

        constraint_time <-
          difftime(
            Sys.time(),
            start_time
          )
        constraint_time <-
          prettyunits::pretty_dt(constraint_time)

        percent_progress <-
          paste0(
            formatC(round(i / length(sql_statements) * 100, digits = 1),
              format = "f",
              digits = 1
            ), "%"
          )

        secretary::typewrite(glue::glue("{percent_progress} completed..."))
        secretary::typewrite(glue::glue("{constraint_time} elapsed..."))
      }

      stop_time <- Sys.time()

      constraint_time <-
        difftime(
          stop_time,
          start_time
        )

      constraint_time <-
        prettyunits::pretty_dt(constraint_time)

      secretary::typewrite(glue::glue("Constraints complete! [{constraint_time}]"))
    }
  }
