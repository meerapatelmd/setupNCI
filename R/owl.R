#' @title
#' Process NCI Thesaurus
#' OWL Files into Nodes and Edges
#'
#' @param owl_folder Path to the 'Thesaurus.owl' and 'InferredThesaurus.owl' files.
#' @param base_folder Base folder to which the json and final
#' node.csv and edge.csv files are written by `nci_version`.
#'
#' @rdname process_owl_to_neo4j
#' @export
#' @import tidyverse
#' @import reticulate
#' @import cli
#' @importFrom glue glue

process_owl_to_neo4j <-
  function(nci_version,
           output_folder) {
    stopifnot(!missing(nci_version))

    # py_install("pandas")
    # py_install("json")
    # py_install("copy")
    # py_install("xmltodict")

    if (missing(output_folder)) {
      output_folder <-
        pkg_options("output_folder")

      if (is.null(output_folder)) {
        stop(
          "`output_folder` value is required or be globally set using `pkg_options()`.",
          call. = FALSE
        )
      }
    }

    owl_folder <-
      file.path(
        output_folder,
        "owl"
      )

    owl_folder <-
      makedirs(
        folder_path = owl_folder,
        verbose = FALSE
      )


    neo4j_folder <-
      file.path(
        output_folder,
        "neo4j",
        nci_version
      )

    neo4j_folder <-
      makedirs(
        folder_path = neo4j_folder,
        verbose = FALSE
      )

    any_neo4j_file_missing <-
      function(neo4j_folder) {
        any(
          sapply(
            file.path(
              neo4j_folder,
              c("node.csv", "edge.csv")
            ),
            file.exists,
            USE.NAMES = FALSE
          ) == FALSE
        )
      }

    if (any_neo4j_file_missing(neo4j_folder = neo4j_folder)) {
      py_file <-
        system.file(package = "setupNCI", "py", "parse_ncit_owl.py")
      py_file_lines <-
        glue::glue(paste(readLines(py_file), collapse = "\n"),
          .open = "{{{",
          .close = "}}}"
        )

      py_run_string(py_file_lines)

      cli::cli_inform("{cli::symbol$tick} Nodes and Edges available at '{neo4j_folder}'")
    } else {
      cli::cli_inform("{cli::symbol$tick} Nodes and Edges already available at '{neo4j_folder}'. To rerun processing, delete 1 or both files and run function again.")
    }
  }




manual_domain_map <-
  tibble::tribble(
    ~`domain_id_1`, ~`domain_id_2`, ~`rel_type`,
    "Drug", "Drug", "Chemical_Or_Drug_Has_Mechanism_Of_Action",
    "Drug", "Drug", "Chemical_Or_Drug_Has_Physiologic_Effect",
    "Drug", "Drug", "Chemotherapy_Regimen_Has_Component",
    "Drug", "Drug", "Has_Pharmaceutical_Administration_Method",
    "Drug", "Drug", "Has_Pharmaceutical_Intended_Site",
    "Drug", "Drug", "Has_Pharmaceutical_Release_Characteristics",
    "Drug", "Drug", "Has_Pharmaceutical_Transformation",
    "Drug", "Observation", "Chemical_Or_Drug_Affects_Abnormal_Cell",
    "Drug", "Observation", "Chemical_Or_Drug_Affects_Gene_Product",
    "Drug", "Observation", "Chemical_Or_Drug_Is_Metabolized_By_Enzyme",
    "Drug", "Observation", "Chemical_Or_Drug_Plays_Role_In_Biological_Process",
    "Drug", "Observation", "Regimen_Has_Accepted_Use_For_Disease",
    "Observation", "Observation", "Allele_In_Chromosomal_Location",
    "Observation", "Observation", "Allele_Plays_Altered_Role_In_Process",
    "Observation", "Observation", "Anatomic_Structure_Has_Location",
    "Observation", "Observation", "Anatomic_Structure_Is_Physical_Part_Of",
    "Observation", "Observation", "Biological_Process_Has_Associated_Location",
    "Observation", "Observation", "Biological_Process_Has_Initiator_Process",
    "Observation", "Observation", "Biological_Process_Has_Result_Anatomy",
    "Observation", "Observation", "Biological_Process_Has_Result_Biological_Process",
    "Observation", "Observation", "Biological_Process_Is_Part_Of_Process",
    "Observation", "Observation", "Cytogenetic_Abnormality_Involves_Chromosome",
    "Observation", "Observation", "Disease_Excludes_Abnormal_Cell",
    "Observation", "Observation", "Disease_Excludes_Cytogenetic_Abnormality",
    "Observation", "Observation", "Disease_Excludes_Finding",
    "Observation", "Observation", "Disease_Excludes_Metastatic_Anatomic_Site",
    "Observation", "Observation", "Disease_Excludes_Molecular_Abnormality",
    "Observation", "Observation", "Disease_Excludes_Normal_Cell_Origin",
    "Observation", "Observation", "Disease_Excludes_Normal_Tissue_Origin",
    "Observation", "Observation", "Disease_Excludes_Primary_Anatomic_Site",
    "Observation", "Observation", "Disease_Has_Abnormal_Cell",
    "Observation", "Observation", "Disease_Has_Associated_Anatomic_Site",
    "Observation", "Observation", "Disease_Has_Associated_Disease",
    "Observation", "Observation", "Disease_Has_Cytogenetic_Abnormality",
    "Observation", "Observation", "Disease_Has_Finding",
    "Observation", "Observation", "Disease_Has_Metastatic_Anatomic_Site",
    "Observation", "Observation", "Disease_Has_Molecular_Abnormality",
    "Observation", "Observation", "Disease_Has_Normal_Cell_Origin",
    "Observation", "Observation", "Disease_Has_Normal_Tissue_Origin",
    "Observation", "Observation", "Disease_Has_Primary_Anatomic_Site",
    "Observation", "Observation", "Disease_Is_Grade",
    "Observation", "Observation", "Disease_Is_Stage",
    "Observation", "Observation", "Disease_Mapped_To_Chromosome",
    "Observation", "Observation", "Disease_Mapped_To_Gene",
    "Observation", "Observation", "Disease_May_Have_Abnormal_Cell",
    "Observation", "Observation", "Disease_May_Have_Associated_Disease",
    "Observation", "Observation", "Disease_May_Have_Cytogenetic_Abnormality",
    "Observation", "Observation", "Disease_May_Have_Finding",
    "Observation", "Observation", "Disease_May_Have_Molecular_Abnormality",
    "Observation", "Observation", "Disease_May_Have_Normal_Cell_Origin",
    "Observation", "Observation", "Disease_May_Have_Normal_Tissue_Origin",
    "Observation", "Observation", "Gene_Associated_With_Disease",
    "Observation", "Observation", "Gene_Has_Physical_Location",
    "Observation", "Observation", "Gene_In_Chromosomal_Location",
    "Observation", "Observation", "Gene_Involved_In_Pathogenesis_Of_Disease",
    "Observation", "Observation", "Gene_Is_Biomarker_Type",
    "Observation", "Observation", "Gene_Is_Element_In_Pathway",
    "Observation", "Observation", "Gene_Plays_Role_In_Process",
    "Observation", "Observation", "Has_CTCAE_5_Parent",
    "Observation", "Observation", "Has_INC_Parent",
    "Observation", "Observation", "Molecular_Abnormality_Involves_Gene",
    "Observation", "Observation", "Neoplasm_Has_Special_Category",
    "Procedure", "Observation", "Procedure_Has_Completely_Excised_Anatomy",
    "Procedure", "Observation", "Procedure_Has_Excised_Anatomy",
    "Procedure", "Observation", "Procedure_Has_Partially_Excised_Anatomy",
    "Procedure", "Observation", "Procedure_Has_Target_Anatomy",
    "Procedure", "Observation", "Procedure_May_Have_Completely_Excised_Anatomy",
    "Procedure", "Observation", "Procedure_May_Have_Excised_Anatomy",
    "Procedure", "Observation", "Procedure_May_Have_Partially_Excised_Anatomy",
    "Observation", "Observation", "subClassOf"
  )


#' @title
#' Process NCI Thesaurus OWL into OMOP Tables
#'
#' @return
#' CSV files
#'
#' @details
#' The OWL files are first processed into the Neo4j nodes and edges csvs
#' using the `process_owl_to_neo4j` function in this package. These csvs are then
#' processed further into OMOP format Vocabulary table csvs: CONCEPT, CONCEPT_SYNONYM,
#' CONCEPT_RELATIONSHIP, CONCEPT_ANCESTOR. Metadata tables VOCABULARY, RELATIONSHIP,
#' and CONCEPT_CLASS are also created.
#'
#' CONCEPT:
#' Concept ids are generated by row number + 20000. Concept ids are not created for
#' any of the metadata concepts in the VOCABULARY, RELATIONSHIP and CONCEPT_CLASS tables.
#' The `domain_id` is determined by the source `rel_type` value in the Edges file. The
#' `concept_class_id` are dependant on the placement of the hierarchy. If it is
#' the topmost ancestor, it is 'Root' Class. If it is a bottomost descendant, it is
#' a 'Leaf' Non-Standard Concept. Otherwise the concept is a 'SubClass' Non-Standard Concept.
#' If there is any value found within the `Concept_Status` field in the nodes csv,
#' the concept is deprecated in the final concept table.
#'
#' CONCEPT_RELATIONSHIP:
#' All relationships (asserted, inherited, and annotation) are used to generate these csvs.
#' The exact `rel_type` from the source edge csv is lost in this processing.
#'
#' CONCEPT_SYNONYM: All language concepts id are 4180186, only
#' concept synonyms in the `FULL_SYN` source field that did not have a
#' lowercased match to the `Preferred_Name` was used.
#'
#' @rdname process_owl_to_omop
#'
#'
#' @export
#' @import tidyverse

process_owl_to_omop <-
  function(nci_version,
           output_folder) {
    vocabulary_id <- "CAI NCIt"
    vocabulary_name <- "CAI NCI Thesaurus"
    vocabulary_version <- nci_version

    if (missing(output_folder)) {
      output_folder <-
        pkg_options("output_folder")

      if (is.null(output_folder)) {
        stop(
          "`output_folder` value is required or be globally set using `pkg_options()`.",
          call. = FALSE
        )
      }
    }

    owl_folder <-
      file.path(
        output_folder,
        "owl"
      )


    neo4j_folder <-
      file.path(
        output_folder,
        "neo4j",
        nci_version
      )


    omop_folder <-
      file.path(
        output_folder,
        "omop",
        nci_version
      )

    omop_folder <-
      makedirs(
        folder_path = omop_folder,
        verbose = FALSE
      )

    final_files <-
      c(
        "CONCEPT_ANCESTOR.csv",
        "CONCEPT_CLASS.csv",
        "CONCEPT_RELATIONSHIP.csv",
        "CONCEPT_SYNONYM.csv",
        "CONCEPT.csv",
        "RELATIONSHIP.csv",
        "VOCABULARY.csv"
      )
    final_paths <-
      file.path(
        omop_folder,
        final_files
      )


    if (any(!file.exists(final_paths))) {
      process_owl_to_neo4j(
        nci_version = nci_version,
        output_folder = output_folder
      )


      node <-
        readr::read_csv(
          file =
            file.path(
              neo4j_folder,
              "node.csv"
            ),
          show_col_types = FALSE
        )

      edge <-
        readr::read_csv(
          file =
            file.path(
              neo4j_folder,
              "edge.csv"
            ),
          show_col_types = FALSE
        )




      classification <-
        edge %>%
        dplyr::filter(
          rel_type == "subClassOf" # ,
          # rel_cat == 'asserted'
        ) %>%
        transmute(
          concept_code_1 = source,
          relationship_id = "Is a",
          relationship_name = "subClassOf (NCIt)",
          rel_type,
          concept_code_2 = target,
          is_hierarchical = 1,
          defines_ancestry = 1
        )

      classification_b <-
        edge %>%
        dplyr::filter(
          rel_type == "subClassOf" # ,
          # rel_cat == 'asserted'
        ) %>%
        transmute(
          concept_code_1 = target,
          relationship_id = "Subsumes",
          relationship_name = "Subsumes (CAI)",
          rel_type,
          concept_code_2 = source,
          is_hierarchical = 1,
          defines_ancestry = 1
        )

      relationships <-
        edge %>%
        dplyr::filter(
          rel_type != "subClassOf" # ,
          # rel_cat == 'asserted'
        ) %>%
        transmute(
          concept_code_1 = source,
          relationship_id = rel_type,
          relationship_name = rel_type,
          rel_type,
          concept_code_2 = target,
          is_hierarchical = 0,
          defines_ancestry = 0
        )

      concept_relationship_stage <-
        bind_rows(
          classification,
          classification_b,
          relationships
        ) %>%
        left_join(
          node %>%
            select(
              concept_code_1 = code,
              concept_name_1 = Preferred_Name
            ),
          by = "concept_code_1"
        ) %>%
        left_join(
          node %>%
            select(
              concept_code_2 = code,
              concept_name_2 = Preferred_Name
            ),
          by = "concept_code_2"
        ) %>%
        distinct()



      root_class_codes <-
        classification %>%
        dplyr::filter(!(concept_code_2 %in% classification$concept_code_1)) %>%
        distinct(concept_code_2) %>%
        unlist() %>%
        unname()

      leaf_concept_codes <-
        classification %>%
        dplyr::filter(!(concept_code_1 %in% classification$concept_code_2)) %>%
        distinct(concept_code_1) %>%
        unlist() %>%
        unname()


      pre_domain_map <-
        concept_relationship_stage %>%
        mutate(
          concept_class_id_1 =
            case_when(
              concept_code_1 %in% root_class_codes ~ "Root",
              concept_code_1 %in% leaf_concept_codes ~ "Leaf",
              TRUE ~ "SubClass"
            ),
          concept_class_id_2 =
            case_when(
              concept_code_2 %in% root_class_codes ~ "Root",
              concept_code_2 %in% leaf_concept_codes ~ "Leaf",
              TRUE ~ "SubClass"
            )
        ) %>%
        left_join(manual_domain_map, by = "rel_type")


      concepts_staged <-
        bind_rows(
          pre_domain_map %>%
            select(
              concept_code = concept_code_1,
              concept_name = concept_name_1,
              concept_class_id = concept_class_id_1,
              domain_id = domain_id_1
            ),
          pre_domain_map %>%
            select(
              concept_code = concept_code_2,
              concept_name = concept_name_2,
              concept_class_id = concept_class_id_2,
              domain_id = domain_id_2
            )
        ) %>%
        mutate(domain_id = "Observation") %>%
        distinct() %>%
        left_join(
          tibble::tribble(
            ~`concept_class_id`, ~`standard_concept`,
            "Leaf", NA_character_,
            "Root", "C",
            "SubClass", NA_character_
          ),
          by = "concept_class_id"
        ) %>%
        left_join(
          node %>%
            distinct(
              code,
              Concept_Status
            ) %>%
            transmute(
              concept_code = code,
              invalid_reason =
                ifelse(!is.na(Concept_Status),
                  "D",
                  Concept_Status
                ),
              valid_start_date =
                "1970-01-01",
              valid_end_date =
                ifelse(!is.na(Concept_Status),
                  "1970-01-01",
                  "2099-12-31"
                )
            ),
          by = "concept_code"
        ) %>%
        mutate(vocabulary_id = vocabulary_id) %>%
        select(all_of(c(
          "concept_name",
          "domain_id",
          "vocabulary_id",
          "concept_class_id",
          "standard_concept",
          "concept_code",
          "valid_start_date",
          "valid_end_date",
          "invalid_reason"
        ))) %>%
        distinct()

      concept_relationship_stage2 <-
        concept_relationship_stage %>%
        transmute(
          concept_code_1,
          concept_code_2,
          relationship_id,
          valid_start_date = "1970-01-01",
          valid_end_date   = "2099-12-31",
          invalid_reason   = NA_character_
        ) %>%
        distinct() %>%
        select(all_of(
          c(
            "concept_code_1",
            "concept_code_2",
            "relationship_id",
            "valid_start_date",
            "valid_end_date",
            "invalid_reason"
          )
        ))

      relationship_stage <-
        concept_relationship_stage %>%
        transmute(
          relationship_id,
          relationship_name,
          is_hierarchical,
          defines_ancestry,
          reverse_relationship_id = NA_character_,
          relationship_concept_id = NA_integer_
        ) %>%
        select(
          all_of(
            c(
              "relationship_id",
              "relationship_name",
              "is_hierarchical",
              "defines_ancestry",
              "reverse_relationship_id",
              "relationship_concept_id"
            )
          )
        ) %>%
        distinct()


      vocabulary_stage <-
        tibble(
          vocabulary_id,
          vocabulary_name,
          vocabulary_reference = "https://meerapatelmd.github.io/setupNCI/",
          vocabulary_version,
          vocabulary_concept_id = NA_integer_
        )


      concept_class_stage <-
        concepts_staged %>%
        transmute(concept_class_id,
          concept_class_name = concept_class_id,
          concept_class_concept_id = NA_integer_
        ) %>%
        distinct()


      make_concept_id <-
        function(x) {
          2000000 + 1:length(x)
        }

      concepts_staged2 <-
        concepts_staged %>%
        mutate(
          concept_id =
            make_concept_id(concept_code)
        ) %>%
        select(
          concept_id,
          dplyr::everything()
        )

      concept_relationship_stage3 <-
        concept_relationship_stage2 %>%
        left_join(concepts_staged2 %>%
          rename_all(function(x) sprintf("%s_1", x)),
        by = c("concept_code_1")
        ) %>%
        left_join(concepts_staged2 %>%
          rename_all(function(x) sprintf("%s_2", x)),
        by = c("concept_code_2")
        ) %>%
        select(all_of(
          c(
            "concept_id_1",
            "concept_id_2",
            "relationship_id",
            "valid_start_date",
            "valid_end_date",
            "invalid_reason"
          )
        )) %>%
        distinct()

      roots <-
        concepts_staged2 %>%
        dplyr::filter(concept_class_id == "Root") %>%
        select(concept_id) %>%
        unlist() %>%
        unname()

      roots_list <-
        vector(
          mode = "list",
          length = length(roots)
        ) %>%
        set_names(concepts_staged2 %>%
          dplyr::filter(concept_class_id == "Root") %>%
          select(concept_name) %>%
          unlist() %>%
          unname())

      tmp_folder <-
        file.path(
          omop_folder,
          "tmp"
        )

      tmp_folder <-
        makedirs(tmp_folder,
          verbose = FALSE
        )

      j <- 0
      total <- length(roots_list)
      cli::cli_h1("Processing each Root into {.emph Concept Ancestor} format...")

      for (root in roots) {
        jj <- j
        j <- j + 1

        tmp_root_file <-
          file.path(
            tmp_folder,
            xfun::with_ext(names(roots_list)[j], "csv")
          )

        cli::cli_text(
          "[{as.character(Sys.time())}] {.strong {names(roots_list)[j]}} {sprintf('%s/%s',j, total)} ({paste0(signif(((jj/total) * 100),
        digits = 2), '%')})"
        )


        if (!file.exists(tmp_root_file)) {
          output <- list()

          for (i in 1:10) {
            if (i == 1) {
              output[[i]] <-
                concept_relationship_stage3 %>%
                dplyr::filter(
                  relationship_id == "Subsumes",
                  concept_id_1 == root
                ) %>%
                transmute(
                  ancestor_concept_id = root,
                  descendant_concept_id = concept_id_2,
                  parent = concept_id_1,
                  child = concept_id_2
                ) %>%
                distinct()
            } else {
              x <-
                output[[i - 1]] %>%
                select(parent = child) %>%
                inner_join(concept_relationship_stage3 %>%
                  dplyr::filter(relationship_id == "Subsumes"),
                by = c("parent" = "concept_id_1")
                ) %>%
                transmute(
                  ancestor_concept_id = root,
                  descendant_concept_id = concept_id_2,
                  parent,
                  child = concept_id_2
                ) %>%
                distinct()

              output[[i]] <- x
            }
          }

          names(output) <-
            as.character(1:length(output))



          # Full Traversal from Root to Leaf
          y <-
            bind_rows(output,
              .id = "min_levels_of_separation"
            ) %>%
            transmute(ancestor_concept_id,
              descendant_concept_id,
              min_levels_of_separation = as.integer(min_levels_of_separation),
              max_levels_of_separation = as.integer(min_levels_of_separation)
            )

          # Direct Parent to Child Relationships
          z <-
            bind_rows(output,
              .id = "min_levels_of_separation"
            ) %>%
            transmute(
              ancestor_concept_id = parent,
              descendant_concept_id = child,
              min_levels_of_separation = 1,
              max_levels_of_separation = 1
            )

          # SubTraversal: Starts at Level 3
          # At Level 3 the child has a grandparent

          output2 <-
            output %>%
            map(select, parent, child)

          for (i in seq_along(output2)) {
            output2[[i]] <-
              bind_cols(
                output2[[i]] %>%
                  transmute(prior_child = parent) %>%
                  rename_all(function(x) sprintf("child_%s", i - 1)),
                output2[[i]] %>%
                  rename_all(function(x) sprintf("%s_%s", x, i))
              )
          }


          quiet_left_join <-
            function(x,
                     y,
                     by = NULL,
                     copy = FALSE,
                     suffix = c(".x", ".y"),
                     ...,
                     keep = FALSE) {
              suppressMessages(
                left_join(
                  x = x,
                  y = y,
                  by = by,
                  copy = copy,
                  suffix = suffix,
                  keep = keep,
                  ...
                )
              )
            }

          output3 <-
            output2 %>%
            reduce(quiet_left_join) %>%
            select(starts_with("child")) %>%
            distinct() %>%
            rename_all(str_remove_all, "child_") %>%
            rowid_to_column("pathid") %>%
            pivot_longer(
              cols = !pathid,
              names_to = "level",
              values_to = "concept_id",
              values_drop_na = TRUE
            ) %>%
            group_by(pathid) %>%
            mutate(level_value = max(as.integer(level))) %>%
            ungroup()

          output3 <-
            split(
              output3,
              output3$pathid
            )

          subtraverse_path <-
            function(df) {
              level_value <-
                unique(df$level_value)

              if (level_value >= 3) {
                subtraversal_range <-
                  1:(level_value - 2) # Root ancestor (0) and direct relationships already accounted for

                child <-
                  df %>%
                  dplyr::filter(level == level_value) %>%
                  select(concept_id) %>%
                  unlist() %>%
                  unname()

                df %>%
                  filter(level %in% as.character(subtraversal_range)) %>%
                  transmute(
                    ancestor_concept_id = concept_id,
                    descendant_concept_id = child,
                    min_levels_of_separation = level_value - (as.integer(level)),
                    max_levels_of_separation = level_value - (as.integer(level))
                  )
              }
            }

          zz <-
            output3 %>%
            map(subtraverse_path) %>%
            bind_rows()



          roots_list[[j]] <-
            bind_rows(y, z, zz) %>%
            distinct()


          readr::write_csv(
            x = roots_list[[j]],
            file = tmp_root_file,
            na = ""
          )

          cli::cli_text(
            "[{as.character(Sys.time())}] {.strong {names(roots_list)[j]}} {sprintf('%s/%s',j, total)} ({paste0(signif(((j/total) * 100),
        digits = 2), '%')})"
          )
        } else {
          roots_list[[j]] <-
            readr::read_csv(
              file = tmp_root_file,
              col_types = c("iiii"),
              na = character()
            )

          cli::cli_text(
            "[{as.character(Sys.time())}] {.strong {names(roots_list)[j]}} {sprintf('%s/%s',j, total)} ({paste0(signif(((j/total) * 100),
        digits = 2), '%')})"
          )
        }
      }

      concept_ancestor_stage <-
        bind_rows(roots_list) %>%
        distinct()

      # Finding ancestor descendant pairs that
      # have more than 1 level associated to convert to min and maxes
      concept_ancestor_stage_b <-
        concept_ancestor_stage %>%
        count(
          ancestor_concept_id,
          descendant_concept_id
        ) %>%
        dplyr::filter(n > 1) %>%
        select(-n) %>%
        left_join(concept_ancestor_stage,
          by = c("ancestor_concept_id", "descendant_concept_id")
        ) %>%
        pivot_longer(
          cols = c(
            min_levels_of_separation,
            max_levels_of_separation
          ),
          names_to = "level",
          values_to = "value"
        ) %>%
        select(-level) %>%
        group_by(
          ancestor_concept_id,
          descendant_concept_id
        ) %>%
        mutate(
          min_levels_of_separation =
            min(value, na.rm = TRUE),
          max_levels_of_separation =
            max(value, na.rm = TRUE)
        ) %>%
        ungroup() %>%
        select(-value) %>%
        distinct()

      concept_ancestor_stage2 <-
        left_join(
          concept_ancestor_stage,
          concept_ancestor_stage_b,
          by = c("ancestor_concept_id", "descendant_concept_id"),
          suffix = c("", "_updated")
        ) %>%
        mutate(
          min_levels_of_separation =
            coalesce(
              min_levels_of_separation_updated,
              min_levels_of_separation
            ),
          max_levels_of_separation =
            coalesce(
              max_levels_of_separation_updated,
              max_levels_of_separation
            )
        ) %>%
        select(-ends_with("_updated")) %>%
        distinct()


      # Concept Synonym
      concept_synonym_stage <-
        node %>%
        select(
          code,
          Preferred_Name,
          FULL_SYN
        ) %>%
        separate_rows(FULL_SYN,
          sep = "[|]{1}"
        ) %>%
        mutate(
          lc_preferred_name =
            tolower(Preferred_Name)
        ) %>%
        mutate(
          lc_full_syn =
            tolower(FULL_SYN)
        ) %>%
        mutate(
          is_synonym =
            lc_preferred_name != lc_full_syn
        ) %>%
        dplyr::filter(is_synonym == TRUE) %>%
        dplyr::select(
          concept_code = code,
          concept_name = Preferred_Name,
          concept_synonym_name = FULL_SYN
        ) %>%
        distinct()

      concept_synonym_stage2 <-
        concept_synonym_stage %>%
        left_join(concepts_staged2,
          by = c("concept_code", "concept_name")
        ) %>%
        transmute(
          concept_id,
          concept_synonym_name,
          language_concept_id = 4180186
        ) %>%
        distinct()


      output_map <-
        list(
          CONCEPT = concepts_staged2,
          CONCEPT_SYNONYM = concept_synonym_stage2,
          CONCEPT_ANCESTOR = concept_ancestor_stage2,
          CONCEPT_RELATIONSHIP = concept_relationship_stage3,
          VOCABULARY = vocabulary_stage,
          RELATIONSHIP = relationship_stage,
          CONCEPT_CLASS = concept_class_stage
        ) %>%
        map(distinct)


      for (i in seq_along(output_map)) {
        readr::write_csv(
          x = output_map[[i]],
          file = file.path(omop_folder, xfun::with_ext(names(output_map)[i], "csv")),
          na = "",
          quote = "all"
        )
      }

      cli::cli_inform("{cli::symbol$tick} OMOP Tables available at '{omop_folder}'")

      print(
        output_map %>%
          map(nrow) %>%
          enframe(
            name = "Table",
            value = "Rows"
          ) %>%
          mutate(
            Rows =
              unlist(Rows)
          )
      )
    }

    cli::cli_inform("{cli::symbol$tick} OMOP Tables already available at '{omop_folder}'. To rerun processing, delete 1 or all files and run function again.")

    cli::cli_inform("Use `setup_omop()` to load csvs into tables.")
  }
