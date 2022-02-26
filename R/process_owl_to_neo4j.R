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

    pkg_options(output_folder = file.path(getwd(), "inst", "data"))

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
