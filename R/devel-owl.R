#' @title
#' Process NCI Thesaurus
#' OWL Files into Nodes and Edges
#'
#' @param owl_folder Path to the 'Thesaurus.owl' and 'InferredThesaurus.owl' files.
#' @param base_folder Base folder to which the json and final
#' node.csv and edge.csv files are written by `nci_version`.
#'
#' @rdname process_owl
#' @export
#' @import tidyverse
#' @import reticulate
#' @import cli
#' @importFrom glue glue

process_owl <-
  function(nci_version   = "21.11e",
           owl_folder    = "/Users/mpatel/terminology/NCIT",
           base_folder   = "/Users/mpatel/Desktop/NCIt") {

    output_folder <-
    file.path(base_folder,
              nci_version)


    any_output_missing <-
      function(output_folder) {

        any(
          sapply(
            file.path(output_folder,
                      c("node.csv", "edge.csv")),
            file.exists,
            USE.NAMES = FALSE)==FALSE)

      }

    if (!dir.exists(output_folder)|
        (dir.exists(output_folder)&any_output_missing(output_folder=output_folder))) {

        py_file <-
          system.file(package = "setupNCI", "py", "parse_ncit_owl.py")
        py_file_lines <-
          glue::glue(paste(readLines(py_file), collapse = "\n"),
                     .open = "{{{",
                     .close = "}}}")

        py_run_string(py_file_lines)

    }

    cli::cli_inform("{cli::symbol$tick} Nodes and Edges available at '{output_folder}'")

  }



process_omop <-
  function(nci_version   = "21.11e",
           owl_folder    = "/Users/mpatel/terminology/NCIT",
           base_folder   = "/Users/mpatel/Desktop/NCIt") {


    process_owl(nci_version = nci_version,
                owl_folder = owl_folder,
                base_folder = base_folder)


    node <-
    readr::read_csv(file =
                      file.path(base_folder,
                                nci_version,
                                "node.csv"))

    # Node to Concept Stage

    concept_stage <-
    node %>%
      transmute(concept_code = code,
             concept_name = Preferred_Name,
             vocabulary_id = 'NCI Thesaurus',
             standard_concept = NA_character_,
             invalid_reason = ifelse(!is.na(Concept_Status),
                                     "D",
                                     NA_character_),
             valid_start_date = "1970-01-01",
             valid_end_date   = ifelse(!is.na(Concept_Status),
                                       "1970-01-01",
                                       "2099-12-31")) %>%
      distinct()



    edge <-
      readr::read_csv(file =
                        file.path(base_folder,
                                  nci_version,
                                  "edge.csv"))


    concept_relationship_stage <-
      edge %>%
      transmute(
        concept_code_1 = target,
        concept_code_2 = source,
        relationship_id = rel_type,
        invalid_reason = NA_character_,
        valid_start_date = "1970-01-01",
        valid_end_date = "2099-12-31") %>%
      distinct()

    list(concept_stage,
         concept_relationship_stage)
#
#     list(node,
#          edge)



  }

staged_tables[[2]] %>%
  select(concept_code_1,
         concept_code_2,
         relationship_id) %>%
  left_join(staged_tables[[1]] %>%
              rename_all(function(x) sprintf("%s_1", x)),
            by = c("concept_code_1" = "concept_code_1")) %>%
  left_join(staged_tables[[1]] %>%
              rename_all(function(x) sprintf("%s_2", x)),
            by = c("concept_code_2" = "concept_code_2")) %>%
  select(concept_name_1,
         relationship_id,
         concept_name_2) %>%
  dplyr::filter(!(relationship_id %in% concept_class_map$relationship_id)) %>%
  dplyr::filter(!(relationship_id %in% c("subClassOf", "Concept_In_Subset"))) %>%
  rubix::filter_at_grepl(col = relationship_id,
                         grepl_phrase = "Has_GDC_Value|Has_Salt|Has_Free_Acid|Has_Data_Element|Role_Has|Conceptual_Part_Of|Has_PCDC_Data_Type|Is_Value_For_GDC_Property|Value_Set_Is_Paired_With|Gene_Found_In_Organism|Has_Pharmaceutical_Basic_Dose_Form|Gene_Product|State_Of_Matter|Has_Target|Permissible_Value_For_Variable",
                         evaluates_to = FALSE) %>%
  group_by(relationship_id) %>%
  mutate(count = n()) %>%
  ungroup() %>%
  arrange(desc(count))


concept_class_map <-
  tribble(
    ~domain_id_1, ~concept_class_1, ~relationship_id                      , ~is_hierarchical, ~domain_id_2, ~concept_class_2,
    'Drug'      , 'Component'     , 'Chemotherapy_Regimen_Has_Component'  , 0               ,  'Drug'     , 'Regimen'       ,
    'Observation', 'Anatomy'      , 'Anatomic_Structure_Is_Physical_Part_Of'        , 0               ,  'Observation',  'Anatomy'    ,
    'Observation', 'Anatomy'      , 'Procedure_Has_Target_Anatomy'        , 0               ,  'Procedure',  'Procedure'    ,
    'Observation', 'Anatomy'      , 'Procedure_Has_Excised_Anatomy'        , 0               ,  'Procedure',  'Procedure'    ,
    'Observation', 'Anatomy'      , 'Procedure_Has_Partially_Excised_Anatomy'        , 0               ,  'Procedure',  'Procedure'    ,
    'Observation', 'Disease'      , 'Disease_Has_Associated_Disease', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Disease'      , 'Disease_May_Have_Associated_Disease', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Finding'      , 'Disease_Has_Finding', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Finding'      , 'Disease_May_Have_Finding', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Finding'      , 'Disease_Excludes_Finding', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Tissue'      , 'Disease_Has_Normal_Tissue_Origin', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Tissue'      , 'Disease_Has_Excludes_Normal_Tissue_Origin', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Cell'      , 'Disease_Has_Normal_Cell_Origin', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Cell'      , 'Disease_Excludes_Normal_Cell_Origin', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Cell'      , 'Disease_Has_Abnormal_Cell', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Cell'      , 'Disease_Excludes_Abnormal_Cell', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Anatomy'      , 'Disease_Has_Associated_Anatomic_Site', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Anatomy'      , 'Disease_Has_Primary_Anatomic_Site', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Anatomy'      , 'Disease_Excludes_Primary_Anatomic_Site', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Gene'      , 'Disease_Mapped_To_Gene', 0               ,  'Observation',  'Disease'    ,
    'Drug', 'Physiologic Effect'      , 'Chemical_Or_Drug_Has_Physiologic_Effect', 0               ,  'Drug',  'Substance'    ,
    'Observation', 'Enzyme'      , 'Chemical_Or_Drug_Is_Metabolized_By_Enzyme', 0               ,  'Drug',  'Substance'    ,
    'Observation', 'Gene Product'      , 'Chemical_Or_Drug_Affects_Gene_Product', 0               ,  'Drug',  'Substance'    ,
    'Observation', 'Genetic Biomarker'      , 'Related_To_Genetic_Biomarker', 0               ,  'Measurement',  'Measurement'    ,
    'Observation', 'Chromosomal Location'      , 'Gene_In_Chromosomal_Location', 0               ,  'Observation',  'Gene'    ,
    'Observation', 'Biochemical Pathway'      , 'Gene_Is_Element_In_Pathway', 0               ,  'Observation',  'Gene'    ,
    'Observation', 'Molecular Process'      , 'Gene_Plays_Role_In_Process', 0               ,  'Observation',  'Gene'    ,
    'Observation', 'Disease'      , 'Gene_Associated_With_Disease', 0               ,  'Observation',  'Gene'    ,
    'Observation', 'Gene'      , 'Molecular_Abnormality_Involves_Gene', 0               ,  'Observation',  'Molecular Abnormality'    ,
    'Observation', 'Disease Pathogenesis'      , 'Gene_Involved_In_Pathogenesis_Of_Disease', 0               ,  'Observation',  'Gene'    ,
    'Drug', 'Mechanism of Action'      , 'Chemical_Or_Drug_Has_Mechanism_Of_Action', 0               ,  'Drug',  'Substance'    ,
    'Observation', 'Disease Stage'      , 'Disease_Is_Stage', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Cytogenetic Abnormality'      , 'Disease_May_Have_Cytogenetic_Abnormality', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Molecular Abnormality'      , 'Disease_May_Have_Molecular_Abnormality', 0               ,  'Observation',  'Disease'    ,
    'Observation', 'Chromosomal Location'      , 'Allele_In_Chromosomal_Location', 0               ,  'Observation',  'Allele'    ,
    'Observation', 'CTCAE 5 Parent'      , 'Has_CTCAE_5_Parent', 0               ,  'Observation',  'CTCAE 5 Child'
  )

