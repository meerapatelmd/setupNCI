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
  function(nci_version) {
    stopifnot(!missing(nci_version))

    pkg_options(output_folder = file.path(here::here(), "inst", "data"))

    output_folder <-
      pkg_options("output_folder")


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
