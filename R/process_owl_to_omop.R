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
#' All concept ids in the output have a pattern of 7-billion.
#'
#' CONCEPT:
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
#' CONCEPT_CROSSWALK: New table that contains select crosswalks explicitly stated in
#' the nodes.csv file.
#'
#' @rdname process_owl_to_omop
#'
#'
#' @export
#' @import tidyverse

process_owl_to_omop <-
  function(owl_folder,
           output_folder = file.path(here::here(), "dev"),
           inst_folder = file.path(here::here(), "inst", "data")) {

    owl_folder <- path.expand(owl_folder)
    output_folder <- path.expand(output_folder)
    inst_folder <- path.expand(inst_folder)

    process_owl_to_neo4j(owl_folder = owl_folder,
                         output_folder = output_folder)

    process_neo4j_to_omop(output_folder = output_folder)

    ### Getting the versions in the README for each
    ### to rename the files in the installation dir
    neo4j_readme_file <- file.path(output_folder, "neo4j", "README.md")
    omop_readme_file  <- file.path(output_folder, "omop" , "README.md")

    get_nci_version <-
      function(readme_file) {

        x <- readLines(readme_file,
                  n = 2)[2]
        trimws(
        stringr::str_remove_all(x,
                                pattern = "Version")
        )

      }

    neo4j_nci_version <- get_nci_version(neo4j_readme_file)
    omop_nci_version  <- get_nci_version(omop_readme_file)

    neo4j_folder <- file.path(output_folder, "neo4j")
    omop_folder <- file.path(output_folder, "omop")

    # If another version of the zip files
    # are already present, they are removed from
    # the folder prior to adding the newest ones
    system(
    glue::glue(
      "rm -rf {inst_folder}",
      "mkdir {inst_folder}",
      .sep = "\n"
    ))


    # Each of the omop and neo4j files are
    # zipped to the inst folder with the name modified with the version
    inst_neo4j_folder <- file.path(inst_folder, glue::glue("neo4j-{neo4j_nci_version}"))
    system(
      glue::glue(
        "rsync {neo4j_folder}/* {inst_neo4j_folder} --recursive",
        "cd {inst_folder}",
        "zip -r {inst_neo4j_folder}.zip {basename(inst_neo4j_folder)}; rm -rf {basename(inst_neo4j_folder)}",
        .sep = "\n"
    ))

    inst_omop_folder <- file.path(inst_folder, glue::glue("omop-{omop_nci_version}"))
    system(
      glue::glue(
        "rsync {omop_folder}/* {inst_omop_folder} --recursive",
        "rm -rf {inst_omop_folder}/tmp",
        "cd {inst_folder}",
        "zip -r {inst_omop_folder}.zip {basename(inst_omop_folder)}; rm -rf {basename(inst_omop_folder)}",
        .sep = "\n"
      ))

    system(
      glue::glue(
      "git add {inst_folder}",
      "git commit -m 'add Neo4j version {neo4j_nci_version} and OMOP version {omop_nci_version} zip'",
      .sep = "\n"
      )
    )

    cli::cli_inform("Use `setup_omop()` to load csvs into tables.")
  }
