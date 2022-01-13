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

    vocabulary_id <- 'CAI NCIt'
    vocabulary_name <- 'CAI NCI Thesaurus'
    vocabulary_version <- nci_version

    process_owl(nci_version = nci_version,
                owl_folder = owl_folder,
                base_folder = base_folder)


    node <<-
    readr::read_csv(file =
                      file.path(base_folder,
                                nci_version,
                                "node.csv"))

    edge <<-
      readr::read_csv(file =
                        file.path(base_folder,
                                  nci_version,
                                  "edge.csv"))



    classification <-
      edge %>%
      dplyr::filter(rel_type == 'subClassOf',
                    rel_cat == 'asserted') %>%
      transmute(
        concept_code_1 = source,
        relationship_id = 'Is a',
        relationship_name = 'subClassOf (NCIt)',
        rel_type,
        concept_code_2 = target,
        is_hierarchical = 1,
        defines_ancestry = 1
      )

    classification_b <-
      edge %>%
      dplyr::filter(rel_type == 'subClassOf',
                    rel_cat == 'asserted') %>%
      transmute(
        concept_code_1 = target,
        relationship_id ='Subsumes',
        relationship_name = 'Subsumes (CAI)',
        rel_type,
        concept_code_2 = source,
        is_hierarchical = 1,
        defines_ancestry = 1
      )

    relationships <-
      edge %>%
      dplyr::filter(rel_type != 'subClassOf',
                    rel_cat == 'asserted') %>%
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
        relationships) %>%
      left_join(
        node %>%
          select(
            concept_code_1 = code,
            concept_name_1 = Preferred_Name),
        by = "concept_code_1") %>%
      left_join(
        node %>%
          select(
            concept_code_2 = code,
            concept_name_2 = Preferred_Name),
        by = "concept_code_2") %>%
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

    manual_domain_map <-
    tibble::tribble(
      ~`domain_id_1`, ~`domain_id_2`, ~`rel_type`,
      'Drug','Drug','Chemical_Or_Drug_Has_Mechanism_Of_Action',
      'Drug','Drug','Chemical_Or_Drug_Has_Physiologic_Effect',
      'Drug','Drug','Chemotherapy_Regimen_Has_Component',
      'Drug','Drug','Has_Pharmaceutical_Administration_Method',
      'Drug','Drug','Has_Pharmaceutical_Intended_Site',
      'Drug','Drug','Has_Pharmaceutical_Release_Characteristics',
      'Drug','Drug','Has_Pharmaceutical_Transformation',
      'Observation','Drug','Chemical_Or_Drug_Affects_Abnormal_Cell',
      'Observation','Drug','Chemical_Or_Drug_Affects_Gene_Product',
      'Observation','Drug','Chemical_Or_Drug_Is_Metabolized_By_Enzyme',
      'Observation','Drug','Chemical_Or_Drug_Plays_Role_In_Biological_Process',
      'Observation','Drug','Regimen_Has_Accepted_Use_For_Disease',
      'Observation','Observation','Allele_In_Chromosomal_Location',
      'Observation','Observation','Allele_Plays_Altered_Role_In_Process',
      'Observation','Observation','Anatomic_Structure_Has_Location',
      'Observation','Observation','Anatomic_Structure_Is_Physical_Part_Of',
      'Observation','Observation','Biological_Process_Has_Associated_Location',
      'Observation','Observation','Biological_Process_Has_Initiator_Process',
      'Observation','Observation','Biological_Process_Has_Result_Anatomy',
      'Observation','Observation','Biological_Process_Has_Result_Biological_Process',
      'Observation','Observation','Biological_Process_Is_Part_Of_Process',
      'Observation','Observation','Cytogenetic_Abnormality_Involves_Chromosome',
      'Observation','Observation','Disease_Excludes_Abnormal_Cell',
      'Observation','Observation','Disease_Excludes_Cytogenetic_Abnormality',
      'Observation','Observation','Disease_Excludes_Finding',
      'Observation','Observation','Disease_Excludes_Metastatic_Anatomic_Site',
      'Observation','Observation','Disease_Excludes_Molecular_Abnormality',
      'Observation','Observation','Disease_Excludes_Normal_Cell_Origin',
      'Observation','Observation','Disease_Excludes_Normal_Tissue_Origin',
      'Observation','Observation','Disease_Excludes_Primary_Anatomic_Site',
      'Observation','Observation','Disease_Has_Abnormal_Cell',
      'Observation','Observation','Disease_Has_Associated_Anatomic_Site',
      'Observation','Observation','Disease_Has_Associated_Disease',
      'Observation','Observation','Disease_Has_Cytogenetic_Abnormality',
      'Observation','Observation','Disease_Has_Finding',
      'Observation','Observation','Disease_Has_Metastatic_Anatomic_Site',
      'Observation','Observation','Disease_Has_Molecular_Abnormality',
      'Observation','Observation','Disease_Has_Normal_Cell_Origin',
      'Observation','Observation','Disease_Has_Normal_Tissue_Origin',
      'Observation','Observation','Disease_Has_Primary_Anatomic_Site',
      'Observation','Observation','Disease_Is_Grade',
      'Observation','Observation','Disease_Is_Stage',
      'Observation','Observation','Disease_Mapped_To_Chromosome',
      'Observation','Observation','Disease_Mapped_To_Gene',
      'Observation','Observation','Disease_May_Have_Abnormal_Cell',
      'Observation','Observation','Disease_May_Have_Associated_Disease',
      'Observation','Observation','Disease_May_Have_Cytogenetic_Abnormality',
      'Observation','Observation','Disease_May_Have_Finding',
      'Observation','Observation','Disease_May_Have_Molecular_Abnormality',
      'Observation','Observation','Disease_May_Have_Normal_Cell_Origin',
      'Observation','Observation','Disease_May_Have_Normal_Tissue_Origin',
      'Observation','Observation','Gene_Associated_With_Disease',
      'Observation','Observation','Gene_Has_Physical_Location',
      'Observation','Observation','Gene_In_Chromosomal_Location',
      'Observation','Observation','Gene_Involved_In_Pathogenesis_Of_Disease',
      'Observation','Observation','Gene_Is_Biomarker_Type',
      'Observation','Observation','Gene_Is_Element_In_Pathway',
      'Observation','Observation','Gene_Plays_Role_In_Process',
      'Observation','Observation','Has_CTCAE_5_Parent',
      'Observation','Observation','Has_INC_Parent',
      'Observation','Observation','Molecular_Abnormality_Involves_Gene',
      'Observation','Observation','Neoplasm_Has_Special_Category',
      'Observation','Procedure','Procedure_Has_Completely_Excised_Anatomy',
      'Observation','Procedure','Procedure_Has_Excised_Anatomy',
      'Observation','Procedure','Procedure_Has_Partially_Excised_Anatomy',
      'Observation','Procedure','Procedure_Has_Target_Anatomy',
      'Observation','Procedure','Procedure_May_Have_Completely_Excised_Anatomy',
      'Observation','Procedure','Procedure_May_Have_Excised_Anatomy',
      'Observation','Procedure','Procedure_May_Have_Partially_Excised_Anatomy',
      'Observation','Observation', 'subClassOf'
    )

    pre_domain_map <-
    concept_relationship_stage %>%
      mutate(
        concept_class_id_1 =
          case_when(concept_code_1 %in% root_class_codes   ~ 'Root Class',
                    concept_code_1 %in% leaf_concept_codes ~ 'Leaf',
                    TRUE ~ 'SubClass'),
        concept_class_id_2 =
          case_when(concept_code_2 %in% root_class_codes   ~ 'Root Class',
                    concept_code_2 %in% leaf_concept_codes ~ 'Leaf',
                    TRUE ~ 'SubClass')) %>%
      left_join(manual_domain_map, by = "rel_type")


    concepts_staged <-
      bind_rows(
        pre_domain_map %>%
          select(concept_code = concept_code_1,
                 concept_name = concept_name_1,
                 concept_class_id = concept_class_id_1,
                 domain_id = domain_id_1),
        pre_domain_map %>%
          select(concept_code = concept_code_2,
                 concept_name = concept_name_2,
                 concept_class_id = concept_class_id_2,
                 domain_id = domain_id_2)) %>%
      distinct() %>%
      mutate(domain_id =
               ifelse(is.na(domain_id),
                      "Observation",
                      domain_id)) %>%
    left_join(
      tibble::tribble(
        ~`concept_class_id`, ~`standard_concept`,
        'Leaf',NA_character_,
        'Root Class','C',
        'SubClass','C'),
      by = "concept_class_id") %>%
      left_join(
        node %>%
          distinct(code,
                   Concept_Status) %>%
          transmute(concept_code = code,
                    invalid_reason =
                      ifelse(!is.na(Concept_Status),
                             'D',
                             Concept_Status),
                    valid_start_date =
                      '1970-01-01',
                    valid_end_date =
                      ifelse(!is.na(Concept_Status),
                             '1970-01-01',
                             '2099-12-31')),
        by = "concept_code") %>%
      mutate(vocabulary_id = 'CAI NCIt') %>%
      select(all_of(    c(
        'concept_name',
        'domain_id',
        'vocabulary_id',
        'concept_class_id',
        'standard_concept',
        'concept_code',
        'valid_start_date',
        'valid_end_date',
        'invalid_reason'))) %>%
      distinct()

    concept_relationship_stage2 <-
      concept_relationship_stage %>%
      transmute(
        concept_code_1,
        concept_code_2,
        relationship_id,
        valid_start_date = '1970-01-01',
        valid_end_date   = '2099-12-31',
        invalid_reason   = NA_character_) %>%
      distinct() %>%
      select(all_of(
    c(
      'concept_code_1',
      'concept_code_2',
      'relationship_id',
      'valid_start_date',
      'valid_end_date',
      'invalid_reason'
    )))

    relationship_stage <-
      concept_relationship_stage %>%
      transmute(
        relationship_id,
        relationship_name,
        is_hierarchical,
        defines_ancestry,
        reverse_relationship_id = NA_character_,
        relationship_concept_id = NA_integer_) %>%
      select(
        all_of(
          c(
            'relationship_id',
            'relationship_name',
            'is_hierarchical',
            'defines_ancestry',
            'reverse_relationship_id',
            'relationship_concept_id'
          )
        )
      )


    vocabulary_stage <-
      tibble(
        vocabulary_id,
        vocabulary_name,
        vocabulary_reference = NA_character_,
        vocabulary_version,
        vocabulary_concept_id = NA_integer_
      )


    concept_class_stage <-
      concepts_staged %>%
      transmute(concept_class_id,
                concept_class_name = concept_class_id,
                concept_class_concept_id = NA_integer_) %>%
      distinct()


    make_concept_id <-
      function(x) {
        2000000 + 1:length(x)
      }

    concepts_staged2 <-
    concepts_staged %>%
      mutate(concept_id =
               make_concept_id(concept_code)) %>%
      select(concept_id,
             dplyr::everything())

    concept_relationship_stage3 <-
    concept_relationship_stage2 %>%
      left_join(concepts_staged2 %>%
                  rename_all(function(x) sprintf("%s_1", x)),
                by = c("concept_code_1")) %>%
      left_join(concepts_staged2 %>%
                  rename_all(function(x) sprintf("%s_2", x)),
                by = c("concept_code_2")) %>%
      select(all_of(
              c(
                'concept_id_1',
                'concept_id_2',
                'relationship_id',
                'valid_start_date',
                'valid_end_date',
                'invalid_reason'
              ))) %>%
      distinct()

    roots <-
    concepts_staged2 %>%
      dplyr::filter(concept_class_id == 'Root Class') %>%
      select(concept_id) %>%
      unlist() %>%
      unname()

    roots_list <-
      vector(mode = "list",
             length = length(roots)) %>%
      set_names(    concepts_staged2 %>%
                      dplyr::filter(concept_class_id == 'Root Class') %>%
                      select(concept_name) %>%
                      unlist() %>%
                      unname())

    j <- 0
    for (root in roots) {

      j <- j+1

      output <- list()

      for (i in 1:10) {

        if (i == 1) {

          output[[i]] <-
          concept_relationship_stage3 %>%
          dplyr::filter(relationship_id == 'Subsumes',
                        concept_id_1 == root) %>%
            transmute(
              ancestor_concept_id = root,
              descendant_concept_id = concept_id_2,
              parent = concept_id_1,
              child =  concept_id_2) %>%
            distinct()

        } else {


          x <-
            output[[i-1]] %>%
            select(parent = child) %>%
            inner_join(concept_relationship_stage3 %>%
                         dplyr::filter(relationship_id == 'Subsumes'),
                       by = c("parent" = "concept_id_1")) %>%
            transmute(
              ancestor_concept_id = root,
              descendant_concept_id = concept_id_2,
              parent,
              child = concept_id_2) %>%
            distinct()

          output[[i]] <- x

        }


      }

      names(output) <-
        as.character(1:length(output))



      y <-
        bind_rows(output,
                  .id = "min_levels_of_separation") %>%
        transmute(ancestor_concept_id,
                  descendant_concept_id,
                  min_levels_of_separation = as.integer(min_levels_of_separation),
                  max_levels_of_separation = as.integer(min_levels_of_separation))

      z <-
        bind_rows(output,
                  .id = "min_levels_of_separation") %>%
        transmute(ancestor_concept_id = parent,
                  descendant_concept_id = child,
                  min_levels_of_separation = 1,
                  max_levels_of_separation = 1)

      roots_list[[j]] <-
        bind_rows(y,z) %>%
        distinct()
    }
  }

concept_ancestor_stage <-
  bind_rows(roots_list) %>%
  distinct()



# Use Case

output0 <-
concepts_staged2 %>%
  dplyr::filter(concept_class_id == 'Root Class') %>%
  select(concept_id,
         concept_code,
         concept_name) %>%
  inner_join(concept_ancestor_stage,
             by = c("concept_id" = "ancestor_concept_id")) %>%
  select(ancestor_id =   concept_id,
         ancestor_code = concept_code,
         ancestor_name = concept_name,
         descendant_concept_id,
         min_levels_of_separation,
         max_levels_of_separation) %>%
  inner_join(concepts_staged2,
             by = c("descendant_concept_id" = "concept_id")) %>%
  select(ancestor_id,
         ancestor_code,
         ancestor_name,
         descendant_id = descendant_concept_id,
         descendant_code = concept_code,
         descendant_name = concept_name,
         level = min_levels_of_separation) %>%
  tidyr::pivot_wider(
    id_cols = c(ancestor_id,
                ancestor_code,
                ancestor_name),
    names_from = level,
    values_from = descendant_name,
    values_fn   = list)

output <- output0
for (i in 1:10) {

  output <-
    output %>%
    unnest(all_of(i))


}

}

#
#     leafs <-
#       classification %>%
#       dplyr::filter(!(source %in% classification$target)) %>%
#       distinct(source)
#
#     # Node to Concept Stage
#
#     concept_stage <-
#     node %>%
#       transmute(concept_code = code,
#              concept_name = Preferred_Name,
#              vocabulary_id = 'NCI Thesaurus',
#              standard_concept = '',
#              invalid_reason = ifelse(!is.na(Concept_Status),
#                                      "D",
#                                      NA_character_),
#              valid_start_date = "1970-01-01",
#              valid_end_date   = ifelse(!is.na(Concept_Status),
#                                        "1970-01-01",
#                                        "2099-12-31")) %>%
#       distinct()
#
#
#
#     edge <-
#       readr::read_csv(file =
#                         file.path(base_folder,
#                                   nci_version,
#                                   "edge.csv"))
#
#
#     concept_relationship_stage <-
#       edge %>%
#       transmute(
#         concept_code_1 = target,
#         concept_code_2 = source,
#         relationship_id = rel_type,
#         rel_invalid_reason = NA_character_,
#         rel_valid_start_date = "1970-01-01",
#         rel_valid_end_date = "2099-12-31") %>%
#       distinct()
#
#     concept_class_map <-
#     concept_relationship_stage %>%
#       left_join(concept_stage %>%
#                   rename_all(function(x) sprintf("%s_1", x)),
#                 by = c("concept_code_1" = "concept_code_1")) %>%
#       left_join(concept_stage %>%
#                   rename_all(function(x) sprintf("%s_2", x)),
#                 by = c("concept_code_2" = "concept_code_2")) %>%
#       inner_join(ncit_to_omop_map_1,
#                 by = c("relationship_id", "standard_concept_1", "standard_concept_2"))
#
#     out_map <-
#     bind_rows(
#     concept_class_map %>%
#       select(concept_code =
#                concept_code_1,
#              concept_name =
#                concept_name_1,
#              concept_class_id =
#                concept_class_id_1),
#     concept_class_map %>%
#       select(concept_code =
#                concept_code_2,
#              concept_name =
#                concept_name_2,
#              concept_class_id =
#                concept_class_id_2)) %>%
#      distinct()
#
#
#     out_map
#
#   }
#
# staged_tables[[2]] %>%
#   select(concept_code_1,
#          concept_code_2,
#          relationship_id) %>%
#   left_join(staged_tables[[1]] %>%
#               rename_all(function(x) sprintf("%s_1", x)),
#             by = c("concept_code_1" = "concept_code_1")) %>%
#   left_join(staged_tables[[1]] %>%
#               rename_all(function(x) sprintf("%s_2", x)),
#             by = c("concept_code_2" = "concept_code_2")) %>%
#   dplyr::filter(concept_code_2 == 'C13208') %>%
#   select(concept_name_1,
#          relationship_id,
#          concept_name_2) %>%
#   inner_join(ncit_to_omop_map_1)
#   # dplyr::filter(!(relationship_id %in% concept_class_map$relationship_id)) %>%
#   #dplyr::filter(relationship_id == 'Related_To_Genetic_Biomarker')
#
#
#   dplyr::filter(!(relationship_id %in% c("subClassOf", "Concept_In_Subset")))
#
#   rubix::filter_at_grepl(col = relationship_id,
#                          grepl_phrase = "Gene_Is_Biomarker_Of|Is_Related_To_Endogenous_Product|Biological_Process_Has_Result_Chemical_Or_Drug|Allele_Has_Activity|Has_ICDC_Value|Biological_Process_Has_Initiator_Chemical_Or_Drug|disjointWith|Allele_Has_Abnormality|Has_CTDC_Value|Gene_Has_Abnormality|Chemical_Or_Drug_Affects_Cell_Type_Or_Tissue|EO_Disease|Has_GDC_Value|Has_Salt|Has_Free_Acid|Has_Data_Element|Role_Has|Conceptual_Part_Of|Has_PCDC_Data_Type|Is_Value_For_GDC_Property|Value_Set_Is_Paired_With|Gene_Found_In_Organism|Has_Pharmaceutical_Basic_Dose_Form|Gene_Product|State_Of_Matter|Has_Target|Permissible_Value",
#                          evaluates_to = FALSE) %>%
#   group_by(relationship_id) %>%
#   mutate(count = n()) %>%
#   ungroup() %>%
#   arrange(desc(count))
#
#
# ncit_to_omop_map_1 <-
#   tibble::tribble(
#     ~`domain_id_1`, ~`concept_class_id_1`, ~`relationship_id`, ~`is_hierarchical`, ~`domain_id_2`, ~`concept_class_id_2`, ~`standard_concept_1`, ~`standard_concept_2`,
#     'Drug','Substance','Chemotherapy_Regimen_Has_Component',0,'Drug','Regimen','','',
#     'Observation','Disease','Regimen_Has_Accepted_Use_For_Disease',0,'Drug','Regimen','','',
#     'Drug','Administration Method','Has_Pharmaceutical_Administration_Method',0,'Drug','Dosage Form','','',
#     'Drug','Intended Site','Has_Pharmaceutical_Intended_Site',0,'Drug','Dosage Form','','',
#     'Drug','Transformation','Has_Pharmaceutical_Transformation',0,'Drug','Dosage Form','','',
#     'Drug','Release','Has_Pharmaceutical_Release_Characteristics',0,'Drug','Dosage Form','','',
#     'Observation','Anatomic Structure','Biological_Process_Has_Result_Anatomy',0,'Observation','Process','','',
#     'Observation','Anatomic Structure','Anatomic_Structure_Has_Location',0,'Observation','Anatomic Structure','','',
#     'Observation','Anatomic Structure','Biological_Process_Has_Associated_Location',0,'Observation','Process','','',
#     'Observation','Anatomic Structure','Anatomic_Structure_Is_Physical_Part_Of',0,'Observation','Anatomic Structure','','',
#     'Observation','Anatomic Structure','Procedure_Has_Target_Anatomy',0,'Procedure','Procedure','','',
#     'Observation','Anatomic Structure','Procedure_Has_Excised_Anatomy',0,'Procedure','Procedure','','',
#     'Observation','Anatomic Structure','Procedure_May_Have_Excised_Anatomy',0,'Procedure','Procedure','','',
#     'Observation','Anatomic Structure','Procedure_May_Have_Partially_Excised_Anatomy',0,'Procedure','Procedure','','',
#     'Observation','Anatomic Structure','Procedure_Has_Completely_Excised_Anatomy',0,'Procedure','Procedure','','',
#     'Observation','Anatomic Structure','Procedure_May_Have_Completely_Excised_Anatomy',0,'Procedure','Procedure','','',
#     'Observation','Anatomic Structure','Procedure_Has_Partially_Excised_Anatomy',0,'Procedure','Procedure','','',
#     'Observation','Disease','Disease_Has_Associated_Disease',0,'Observation','Disease','','',
#     'Observation','Disease','Disease_May_Have_Associated_Disease',0,'Observation','Disease','','',
#     'Observation','Finding','Disease_Has_Finding',0,'Observation','Disease','','',
#     'Observation','Finding','Disease_May_Have_Finding',0,'Observation','Disease','','',
#     'Observation','Finding','Disease_Excludes_Finding',0,'Observation','Disease','','',
#     'Observation','Tissue','Disease_Has_Normal_Tissue_Origin',0,'Observation','Disease','','',
#     'Observation','Tissue','Disease_May_Have_Normal_Tissue_Origin',0,'Observation','Disease','','',
#     'Observation','Tissue','Disease_Excludes_Normal_Tissue_Origin',0,'Observation','Disease','','',
#     'Observation','Cell','Disease_Has_Normal_Cell_Origin',0,'Observation','Disease','','',
#     'Observation','Cell','Disease_May_Have_Normal_Cell_Origin',0,'Observation','Disease','','',
#     'Observation','Cell','Disease_Excludes_Normal_Cell_Origin',0,'Observation','Disease','','',
#     'Observation','Cell','Disease_Has_Abnormal_Cell',0,'Observation','Disease','','',
#     'Observation','Cell','Disease_May_Have_Abnormal_Cell',0,'Observation','Disease','','',
#     'Observation','Cell','Disease_Excludes_Abnormal_Cell',0,'Observation','Disease','','',
#     'Observation','Cell','Chemical_Or_Drug_Affects_Abnormal_Cell',0,'Drug','Substance','','',
#     'Observation','Anatomic Structure','Disease_Has_Metastatic_Anatomic_Site',0,'Observation','Disease','','',
#     'Observation','Anatomic Structure','Disease_Excludes_Metastatic_Anatomic_Site',0,'Observation','Disease','','',
#     'Observation','Anatomic Structure','Disease_Has_Associated_Anatomic_Site',0,'Observation','Disease','','',
#     'Observation','Anatomic Structure','Disease_Has_Primary_Anatomic_Site',0,'Observation','Disease','','',
#     'Observation','Anatomic Structure','Disease_Excludes_Primary_Anatomic_Site',0,'Observation','Disease','','',
#     'Observation','Gene','Disease_Mapped_To_Gene',0,'Observation','Disease','','',
#     'Drug','Process','Chemical_Or_Drug_Has_Physiologic_Effect',0,'Drug','Substance','','',
#     'Observation','Enzyme','Chemical_Or_Drug_Is_Metabolized_By_Enzyme',0,'Drug','Substance','','',
#     'Observation','Gene Product','Chemical_Or_Drug_Affects_Gene_Product',0,'Drug','Substance','','',
#     # 'Observation','Genetic Biomarker','Related_To_Genetic_Biomarker',0,'Measurement','Measurement','','',
#     'Observation','Chromosomal Location','Gene_In_Chromosomal_Location',0,'Observation','Gene','','',
#     'Observation','Biochemical Pathway','Gene_Is_Element_In_Pathway',0,'Observation','Gene','','',
#     'Observation','Process','Gene_Plays_Role_In_Process',0,'Observation','Gene','','',
#     'Observation','Disease','Gene_Associated_With_Disease',0,'Observation','Gene','','',
#     'Observation','Gene','Molecular_Abnormality_Involves_Gene',0,'Observation','Molecular Abnormality','','',
#     'Observation','Disease','Gene_Involved_In_Pathogenesis_Of_Disease',0,'Observation','Gene','','',
#     'Drug','Process','Chemical_Or_Drug_Has_Mechanism_Of_Action',0,'Drug','Substance','','',
#     'Observation','Process','Chemical_Or_Drug_Plays_Role_In_Biological_Process',0,'Drug','Substance','','',
#     'Observation','Disease Stage','Disease_Is_Stage',0,'Observation','Disease','','',
#     'Observation','Disease Grade','Disease_Is_Grade',0,'Observation','Disease','','',
#     'Observation','Cytogenetic Abnormality','Disease_Has_Cytogenetic_Abnormality',0,'Observation','Disease','','',
#     'Observation','Cytogenetic Abnormality','Disease_May_Have_Cytogenetic_Abnormality',0,'Observation','Disease','','',
#     'Observation','Cytogenetic Abnormality','Disease_Excludes_Cytogenetic_Abnormality',0,'Observation','Disease','','',
#     'Observation','Molecular Abnormality','Disease_May_Have_Molecular_Abnormality',0,'Observation','Disease','','',
#     'Observation','Molecular Abnormality','Disease_Has_Molecular_Abnormality',0,'Observation','Disease','','',
#     'Observation','Molecular Abnormality','Disease_Excludes_Molecular_Abnormality',0,'Observation','Disease','','',
#     'Observation','Chromosome','Disease_Mapped_To_Chromosome',0,'Observation','Disease','','',
#     'Observation','Chromosomal Location','Allele_In_Chromosomal_Location',0,'Observation','Allele','','',
#     'Observation','Process','Allele_Plays_Altered_Role_In_Process',0,'Observation','Allele','','',
#     'Observation','CTCAE 5 Parent','Has_CTCAE_5_Parent',1,'Observation','CTCAE 5 Child','C','C',
#     'Observation','Physical Location','Gene_Has_Physical_Location',0,'Observation','Gene','','',
#     'Observation','Chromosome','Cytogenetic_Abnormality_Involves_Chromosome',0,'Observation','Cytogenetic Abnormality','','',
#     'Observation','Process','Biological_Process_Is_Part_Of_Process',0,'Observation','Process','','',
#     'Observation','Process','Biological_Process_Has_Initiator_Process',0,'Observation','Process','','',
#     'Observation','Process','Biological_Process_Has_Result_Biological_Process',0,'Observation','Process','','',
#     'Observation','Adverse Event Parent','Has_INC_Parent',1,'Observation','Adverse Event Child','C','C',
#     'Observation','Neoplasm Category','Neoplasm_Has_Special_Category',1,'Observation','Neoplasm','C','',
#     'Observation','Biomarker Type','Gene_Is_Biomarker_Type',0,'Observation','Gene','C',''
#   )
#
