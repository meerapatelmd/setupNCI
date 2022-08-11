#' @title
#' Process Neo4j files into OMOP Tables
#'
#' @rdname process_neo4j_to_omop
#'
#'
#' @export
#' @import tidyverse

process_neo4j_to_omop <-
  function(output_folder = file.path(here::here(), "dev")) {

    cli::cli_h1("process_neo4j_to_omop")

    ############### Variables #######################
    vocabulary_id   <- "CAI NCIt"
    vocabulary_name <- "CAI NCI Thesaurus"

    neo4j_folder <-
      file.path(
        output_folder,
        "neo4j"
      )

    omop_folder <-
      file.path(
        output_folder,
        "omop"
      )

    # Getting the version from neo4j which is what the omop
    # files are updated to
    neo4j_nci_version     <- readLines(file.path(neo4j_folder, "README.md"),
                                    n = 2)[2]
    neo4j_nci_version <-
      trimws(
      stringr::str_remove_all(string = neo4j_nci_version,
                              pattern = "Version")
      )

    # Getting the version from omop which is what it is being replaced with
    omop_nci_version     <- readLines(file.path(omop_folder, "README.md"),
                                       n = 2)[2]
    omop_nci_version <-
      trimws(
        stringr::str_remove_all(string = omop_nci_version,
                                pattern = "Version")
      )

    #################################################################
    ############### Converting Node and Edges #######################
    #################################################################
      node <-
        readr::read_csv(
          file =
            file.path(
              neo4j_folder,
              "node.csv"
            ),
          col_types = readr::cols(.default = "c"),
          show_col_types = FALSE
        )

    cli::cli_inform("There are {nrow(node)} nodes in version {neo4j_nci_version}.")
      # > node
      # # A tibble: 166,403 × 64
      # concept_id               code  Semantic_Type Preferred_Name UMLS_CUI FDA_UNII_Code Contributing_So… `Legacy Concep…` FULL_SYN DEFINITION label Concept_Status ALT_DEFINITION NCI_META_CUI
      # <chr>                    <chr> <chr>         <chr>          <chr>    <chr>         <chr>            <chr>            <chr>    <chr>      <chr> <chr>          <chr>          <chr>
      #   1 http://ncicb.nci.nih.go… C1000 Amino Acid, … Recombinant A… C1514768 7MGE0HPM2H    FDA              Recombinant_Amp… AMPHIRE… A recombi… Reco… NA             NA             NA
      # 2 http://ncicb.nci.nih.go… C100… Therapeutic … Cyclophospham… C0279323 NA            NA               Cyclophosphamid… CD(P)TH… NA         Cycl… Obsolete_Conc… NA             NA
      # 3 http://ncicb.nci.nih.go… C100… Therapeutic … Percutaneous … C3272245 NA            CDISC            NA               PERCUTA… A percuta… Perc… NA             A percutaneou… NA
      # 4 http://ncicb.nci.nih.go… C100… Therapeutic … Percutaneous … C3272246 NA            CDISC            NA               PERCUTA… A percuta… Perc… NA             A percutaneou… NA
      # 5 http://ncicb.nci.nih.go… C100… Therapeutic … Percutaneous … C3272247 NA            CDISC            NA               PERCUTA… A percuta… Perc… NA             A percutaneou… NA
      # 6 http://ncicb.nci.nih.go… C100… Therapeutic … Percutaneous … C3272248 NA            CDISC            NA               PERCUTA… Invasive … Perc… NA             Invasive proc… NA
      # 7 http://ncicb.nci.nih.go… C100… Therapeutic … Pericardial S… C3272249 NA            CDISC            NA               PERICAR… Removal o… Peri… NA             Removal or re… NA
      # 8 http://ncicb.nci.nih.go… C100… Health Care … Post-Cardiac … C3272250 NA            CDISC            NA               POST-CA… A procedu… Post… NA             A procedure t… NA
      # 9 http://ncicb.nci.nih.go… C100… Health Care … Pre-Operative… C3272251 NA            CDISC            NA               PRE-OPE… A procedu… Pre-… NA             A procedure t… NA
      # 10 http://ncicb.nci.nih.go… C100… Finding       Previously Im… C3272252 NA            CDISC            NA               PREVIOU… The coron… Prev… NA             The coronary … NA
      # # … with 166,393 more rows, and 50 more variables: PDQ_Open_Trial_Search_ID <chr>, PDQ_Closed_Trial_Search_ID <chr>, NCI_Drug_Dictionary_ID <chr>, Display_Name <chr>,
      # #   Neoplastic_Status <chr>, PubMedID_Primary_Reference <chr>, CAS_Registry <chr>, Maps_To <chr>, OMIM_Number <chr>, Swiss_Prot <chr>, FDA_Table <chr>, Chemical_Formula <chr>,
      # #   `NSC Number` <chr>, Publish_Value_Set <chr>, Term_Browser_Value_Set_Description <chr>, Value_Set_Pair <chr>, Extensible_List <chr>, CHEBI_ID <chr>, Accepted_Therapeutic_Use_For <chr>,
      # #   DesignNote <chr>, GenBank_Accession_Number <chr>, HGNC_ID <chr>, EntrezGene_ID <chr>, `oboInOwl:hasDbXref` <chr>, INFOODS <chr>, USDA_ID <chr>, Unit <chr>, miRBase_ID <chr>,
      # #   NCBI_Taxon_ID <chr>, SNP_ID <chr>, ClinVar_Variation_ID <chr>, Gene_Encodes_Product <chr>, `ICD-O-3_Code` <chr>, Subsource <chr>, US_Recommended_Intake <chr>, Tolerable_Level <chr>,
      # #   Nutrient <chr>, Micronutrient <chr>, MGI_Accession_ID <chr>, Use_For <chr>, Homologous_Gene <chr>, PID_ID <chr>, Essential_Amino_Acid <chr>, KEGG_ID <chr>, GO_Annotation <chr>,
      # #   Macronutrient <chr>, Essential_Fatty_Acid <chr>, BioCarta_ID <chr>, Relative_Enzyme_Activity <chr>, `:LABEL` <chr>

      edge <-
        readr::read_csv(
          file =
            file.path(
              neo4j_folder,
              "edge.csv"
            ),
          col_types = readr::cols(.default = "c"),
          show_col_types = FALSE
        )
      # > edge
      # # A tibble: 1,104,754 × 4
      # source  rel_type                           target rel_cat
      # <chr>   <chr>                              <chr>  <chr>
      #   1 C1000   subClassOf                         C1504  asserted
      # 2 C1000   Concept_In_Subset                  C63923 annotation
      # 3 C10000  subClassOf                         C61007 asserted
      # 4 C10000  Chemotherapy_Regimen_Has_Component C405   asserted
      # 5 C10000  Chemotherapy_Regimen_Has_Component C507   asserted
      # 6 C10000  Chemotherapy_Regimen_Has_Component C662   asserted
      # 7 C10000  Chemotherapy_Regimen_Has_Component C770   asserted
      # 8 C10000  Chemotherapy_Regimen_Has_Component C855   asserted
      # 9 C100000 subClassOf                         C99521 asserted
      # 10 C100000 Procedure_Has_Target_Anatomy       C12686 inherited
      # # … with 1,104,744 more rows
      cli::cli_inform("There are {nrow(edge)} edges in version {neo4j_nci_version}.")



      # Edges are subset into classification based on the
      # 'subClassOf' or relationships (any other relationship)
      # The classification subset is then doubled by generating the
      # inverse 'Subsumes (CAI)' relationship

      ######### Edges is filtered for classifications ##############
      classification <-
        edge %>%
        dplyr::filter(
          rel_type == "subClassOf" # ,
          # rel_cat == 'asserted'
        ) %>%
        dplyr::transmute(
          concept_code_1 = source,
          relationship_id = "Is a",
          relationship_name = "subClassOf (NCIt)",
          rel_type,
          concept_code_2 = target,
          is_hierarchical = 1,
          defines_ancestry = 1
        )
      # > classification
      # # A tibble: 190,404 × 7
      # concept_code_1 relationship_id relationship_name rel_type   concept_code_2 is_hierarchical defines_ancestry
      # <chr>          <chr>           <chr>             <chr>      <chr>                    <dbl>            <dbl>
      #   1 C1000          Is a            subClassOf (NCIt) subClassOf C1504                        1                1
      # 2 C10000         Is a            subClassOf (NCIt) subClassOf C61007                       1                1
      # 3 C100000        Is a            subClassOf (NCIt) subClassOf C99521                       1                1
      # 4 C100001        Is a            subClassOf (NCIt) subClassOf C99521                       1                1
      # 5 C100002        Is a            subClassOf (NCIt) subClassOf C99521                       1                1
      # 6 C100003        Is a            subClassOf (NCIt) subClassOf C80449                       1                1
      # 7 C100004        Is a            subClassOf (NCIt) subClassOf C80430                       1                1
      # 8 C100005        Is a            subClassOf (NCIt) subClassOf C15839                       1                1
      # 9 C100006        Is a            subClassOf (NCIt) subClassOf C139982                      1                1
      # 10 C100007        Is a            subClassOf (NCIt) subClassOf C99896                       1                1
      # # … with 190,394 more rows

      ######### Classification inverse is generated from the classification subset ###########
      classification_b <-
        edge %>%
        dplyr::filter(
          rel_type == "subClassOf" # ,
          # rel_cat == 'asserted'
        ) %>%
        dplyr::transmute(
          concept_code_1 = target,
          relationship_id = "Subsumes",
          relationship_name = "Subsumes (CAI)",
          rel_type,
          concept_code_2 = source,
          is_hierarchical = 1,
          defines_ancestry = 1
        )
      # > classification_b
      # # A tibble: 190,404 × 7
      # concept_code_1 relationship_id relationship_name rel_type   concept_code_2 is_hierarchical defines_ancestry
      # <chr>          <chr>           <chr>             <chr>      <chr>                    <dbl>            <dbl>
      #   1 C1504          Subsumes        Subsumes (CAI)    subClassOf C1000                        1                1
      # 2 C61007         Subsumes        Subsumes (CAI)    subClassOf C10000                       1                1
      # 3 C99521         Subsumes        Subsumes (CAI)    subClassOf C100000                      1                1
      # 4 C99521         Subsumes        Subsumes (CAI)    subClassOf C100001                      1                1
      # 5 C99521         Subsumes        Subsumes (CAI)    subClassOf C100002                      1                1
      # 6 C80449         Subsumes        Subsumes (CAI)    subClassOf C100003                      1                1
      # 7 C80430         Subsumes        Subsumes (CAI)    subClassOf C100004                      1                1
      # 8 C15839         Subsumes        Subsumes (CAI)    subClassOf C100005                      1                1
      # 9 C139982        Subsumes        Subsumes (CAI)    subClassOf C100006                      1                1
      # 10 C99896         Subsumes        Subsumes (CAI)    subClassOf C100007                      1                1
      # # … with 190,394 more rows

      ################## edges to all other relationships ####################
      ################## NOTE: inverse of relationship is not added as in the classifications #########
      relationships <-
        edge %>%
        dplyr::filter(
          rel_type != "subClassOf" # ,
          # rel_cat == 'asserted'
        ) %>%
        dplyr::transmute(
          concept_code_1 = source,
          relationship_id = rel_type,
          relationship_name = rel_type,
          rel_type,
          concept_code_2 = target,
          is_hierarchical = 0,
          defines_ancestry = 0
        )
      # > relationships
      # # A tibble: 914,350 × 7
      # concept_code_1 relationship_id                    relationship_name                  rel_type                           concept_code_2 is_hierarchical defines_ancestry
      # <chr>          <chr>                              <chr>                              <chr>                              <chr>                    <dbl>            <dbl>
      #   1 C1000          Concept_In_Subset                  Concept_In_Subset                  Concept_In_Subset                  C63923                       0                0
      # 2 C10000         Chemotherapy_Regimen_Has_Component Chemotherapy_Regimen_Has_Component Chemotherapy_Regimen_Has_Component C405                         0                0
      # 3 C10000         Chemotherapy_Regimen_Has_Component Chemotherapy_Regimen_Has_Component Chemotherapy_Regimen_Has_Component C507                         0                0
      # 4 C10000         Chemotherapy_Regimen_Has_Component Chemotherapy_Regimen_Has_Component Chemotherapy_Regimen_Has_Component C662                         0                0
      # 5 C10000         Chemotherapy_Regimen_Has_Component Chemotherapy_Regimen_Has_Component Chemotherapy_Regimen_Has_Component C770                         0                0
      # 6 C10000         Chemotherapy_Regimen_Has_Component Chemotherapy_Regimen_Has_Component Chemotherapy_Regimen_Has_Component C855                         0                0
      # 7 C100000        Procedure_Has_Target_Anatomy       Procedure_Has_Target_Anatomy       Procedure_Has_Target_Anatomy       C12686                       0                0
      # 8 C100000        Concept_In_Subset                  Concept_In_Subset                  Concept_In_Subset                  C101859                      0                0
      # 9 C100000        Concept_In_Subset                  Concept_In_Subset                  Concept_In_Subset                  C61410                       0                0
      # 10 C100000        Concept_In_Subset                  Concept_In_Subset                  Concept_In_Subset                  C66830                       0                0
      # # … with 914,340 more rows


      ########## Classifications + Inverse Classifications + Relationships
      ########## and converted into a staged Concept Relationship table
      concept_relationship_stage <-
        dplyr::bind_rows(
          classification,
          classification_b,
          relationships
        ) %>%
        dplyr::full_join(
          node %>%
            dplyr::select(
              concept_code_1 = code,
              concept_name_1 = Preferred_Name
            ),
          by = "concept_code_1"
        ) %>%
        dplyr::full_join(
          node %>%
            dplyr::select(
              concept_code_2 = code,
              concept_name_2 = Preferred_Name
            ),
          by = "concept_code_2"
        ) %>%
        dplyr::distinct()


      # > concept_relationship_stage
      # # A tibble: 1,295,158 × 9
      # concept_code_1 relationship_id relationship_name rel_type   concept_code_2 is_hierarchical defines_ancestry concept_name_1                                                 concept_name_2
      # <chr>          <chr>           <chr>             <chr>      <chr>                    <dbl>            <dbl> <chr>                                                          <chr>
      #   1 C1000          Is a            subClassOf (NCIt) subClassOf C1504                        1                1 Recombinant Amphiregulin                                       Recombinant E…
      # 2 C10000         Is a            subClassOf (NCIt) subClassOf C61007                       1                1 Cyclophosphamide/Fluoxymesterone/Mitolactol/Prednisone/Tamoxi… Agent Combina…
      # 3 C100000        Is a            subClassOf (NCIt) subClassOf C99521                       1                1 Percutaneous Coronary Intervention for ST Elevation Myocardia… Percutaneous …
      # 4 C100001        Is a            subClassOf (NCIt) subClassOf C99521                       1                1 Percutaneous Coronary Intervention for ST Elevation Myocardia… Percutaneous …
      # 5 C100002        Is a            subClassOf (NCIt) subClassOf C99521                       1                1 Percutaneous Coronary Intervention for ST Elevation Myocardia… Percutaneous …
      # 6 C100003        Is a            subClassOf (NCIt) subClassOf C80449                       1                1 Percutaneous Mitral Valve Repair                               Mitral Valve …
      # 7 C100004        Is a            subClassOf (NCIt) subClassOf C80430                       1                1 Pericardial Stripping                                          Cardiac Thera…
      # 8 C100005        Is a            subClassOf (NCIt) subClassOf C15839                       1                1 Post-Cardiac Transplant Evaluation                             Prevention an…
      # 9 C100006        Is a            subClassOf (NCIt) subClassOf C139982                      1                1 Pre-Operative Evaluation for Non-Cardiovascular Surgery        Pre-operative…
      # 10 C100007        Is a            subClassOf (NCIt) subClassOf C99896                       1                1 Previously Implanted Cardiac Lead                              Cardiac Lead …
      # # … with 1,295,148 more rows



      # In the classification subset derived from 'Is a' and
      # 'subClassOf' relationships, I find the root and leaf
      # class codes. These will be used to define the
      # 'concept_class_id' of Root or Leaf
      root_class_codes <-
        classification %>%
        dplyr::filter(!(concept_code_2 %in% classification$concept_code_1)) %>%
        dplyr::distinct(concept_code_2) %>%
        unlist() %>%
        unname()
      # [1] "C20189" "C20181" "C20047" "C22188" "C26548" "C12913" "C12219" "C22187" "C14250" "C1908"  "C43431" "C17828" "C97325" "C20633" "C16612" "C3910"  "C7057"  "C12218"

      leaf_concept_codes <-
        classification %>%
        dplyr::filter(!(concept_code_1 %in% classification$concept_code_2)) %>%
        dplyr::distinct(concept_code_1) %>%
        unlist() %>%
        unname()
      # [1] "C1000"   "C10000"  "C100000" "C100001" "C100002" "C100003" [ reached getOption("max.print") -- omitted 135203 entries ]

      ######### Concept Relationships are appended with the Root and Leaf concept classes
      ######### at concept 1 and concept 2 which will then be selected for
      ######### into the final Concept Table
      concept_map <-
        concept_relationship_stage %>%
        dplyr::mutate(
          concept_class_id_1 =
            dplyr::case_when(
              concept_code_1 %in% root_class_codes ~ "Root",
              concept_code_1 %in% leaf_concept_codes ~ "Leaf",
              TRUE ~ "SubClass"
            ),
          concept_class_id_2 =
            dplyr::case_when(
              concept_code_2 %in% root_class_codes ~ "Root",
              concept_code_2 %in% leaf_concept_codes ~ "Leaf",
              TRUE ~ "SubClass"
            )
        ) %>%
        dplyr::distinct()

      # > concept_map
      # # A tibble: 1,295,158 × 11
      # concept_code_1 relationship_id relationship_name rel_type   concept_code_2 is_hierarchical defines_ancestry concept_name_1               concept_name_2 concept_class_i… concept_class_i…
      # <chr>          <chr>           <chr>             <chr>      <chr>                    <dbl>            <dbl> <chr>                        <chr>          <chr>            <chr>
      #   1 C1000          Is a            subClassOf (NCIt) subClassOf C1504                        1                1 Recombinant Amphiregulin     Recombinant E… Leaf             SubClass
      # 2 C10000         Is a            subClassOf (NCIt) subClassOf C61007                       1                1 Cyclophosphamide/Fluoxymest… Agent Combina… Leaf             SubClass
      # 3 C100000        Is a            subClassOf (NCIt) subClassOf C99521                       1                1 Percutaneous Coronary Inter… Percutaneous … Leaf             SubClass
      # 4 C100001        Is a            subClassOf (NCIt) subClassOf C99521                       1                1 Percutaneous Coronary Inter… Percutaneous … Leaf             SubClass
      # 5 C100002        Is a            subClassOf (NCIt) subClassOf C99521                       1                1 Percutaneous Coronary Inter… Percutaneous … Leaf             SubClass
      # 6 C100003        Is a            subClassOf (NCIt) subClassOf C80449                       1                1 Percutaneous Mitral Valve R… Mitral Valve … Leaf             SubClass
      # 7 C100004        Is a            subClassOf (NCIt) subClassOf C80430                       1                1 Pericardial Stripping        Cardiac Thera… Leaf             SubClass
      # 8 C100005        Is a            subClassOf (NCIt) subClassOf C15839                       1                1 Post-Cardiac Transplant Eva… Prevention an… Leaf             SubClass
      # 9 C100006        Is a            subClassOf (NCIt) subClassOf C139982                      1                1 Pre-Operative Evaluation fo… Pre-operative… Leaf             SubClass
      # 10 C100007        Is a            subClassOf (NCIt) subClassOf C99896                       1                1 Previously Implanted Cardia… Cardiac Lead … Leaf             SubClass
      # # … with 1,295,148 more rows

      # Concepts on both sides of the Concept Relationship
      # table are united into a staged Concepts table
      concepts_staged <-
        dplyr::bind_rows(
          concept_map %>%
            dplyr::transmute(
              concept_code = concept_code_1,
              concept_name = concept_name_1,
              concept_class_id = concept_class_id_1,
              domain_id = "Observation"
            ),
          concept_map %>%
            dplyr::transmute(
              concept_code = concept_code_2,
              concept_name = concept_name_2,
              concept_class_id = concept_class_id_2,
              domain_id = "Observation")) %>%
        dplyr::distinct() %>%
        dplyr::left_join(
          tibble::tribble(
            ~`concept_class_id`, ~`standard_concept`,
            "Leaf", NA_character_,
            "Root", "C",
            "SubClass", NA_character_
          ),
          by = "concept_class_id"
        ) %>%
        dplyr::left_join(
          node %>%
            dplyr::distinct(
              code,
              Concept_Status
            ) %>%
            dplyr::transmute(
              concept_code = code,
              invalid_reason =
                ifelse(
                  grepl(pattern = "Obsolete", Concept_Status),
                  "D",
                  NA_character_
                ),
              valid_start_date =
                "1970-01-01",
              valid_end_date =
                ifelse(
                  grepl(pattern = "Obsolete", Concept_Status),
                  valid_start_date,
                  "2099-12-31"
                )
            ),
          by = "concept_code"
        ) %>%
        dplyr::mutate(vocabulary_id = vocabulary_id) %>%
        dplyr::select(dplyr::all_of(c(
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
        dplyr::distinct()
      # > concepts_staged
      # # A tibble: 166,403 × 9
      # concept_name                                                        domain_id vocabulary_id concept_class_id standard_concept concept_code valid_start_date valid_end_date invalid_reason
      # <chr>                                                               <chr>     <chr>         <chr>            <chr>            <chr>        <chr>            <chr>          <chr>
      #   1 Recombinant Amphiregulin                                            Observat… CAI NCIt      Leaf             NA               C1000        1970-01-01       2099-12-31     NA
      # 2 Cyclophosphamide/Fluoxymesterone/Mitolactol/Prednisone/Tamoxifen    Observat… CAI NCIt      Leaf             NA               C10000       1970-01-01       1970-01-01     D
      # 3 Percutaneous Coronary Intervention for ST Elevation Myocardial Inf… Observat… CAI NCIt      Leaf             NA               C100000      1970-01-01       2099-12-31     NA
      # 4 Percutaneous Coronary Intervention for ST Elevation Myocardial Inf… Observat… CAI NCIt      Leaf             NA               C100001      1970-01-01       2099-12-31     NA
      # 5 Percutaneous Coronary Intervention for ST Elevation Myocardial Inf… Observat… CAI NCIt      Leaf             NA               C100002      1970-01-01       2099-12-31     NA
      # 6 Percutaneous Mitral Valve Repair                                    Observat… CAI NCIt      Leaf             NA               C100003      1970-01-01       2099-12-31     NA
      # 7 Pericardial Stripping                                               Observat… CAI NCIt      Leaf             NA               C100004      1970-01-01       2099-12-31     NA
      # 8 Post-Cardiac Transplant Evaluation                                  Observat… CAI NCIt      Leaf             NA               C100005      1970-01-01       2099-12-31     NA
      # 9 Pre-Operative Evaluation for Non-Cardiovascular Surgery             Observat… CAI NCIt      Leaf             NA               C100006      1970-01-01       2099-12-31     NA
      # 10 Previously Implanted Cardiac Lead                                   Observat… CAI NCIt      Leaf             NA               C100007      1970-01-01       2099-12-31     NA
      # # … with 166,393 more rows

      ## Adding the date data to the staged concept relationships table
      concept_relationship_stage2 <-
        concept_relationship_stage %>%
        dplyr::transmute(
          concept_code_1,
          concept_code_2,
          relationship_id,
          valid_start_date = "1970-01-01",
          valid_end_date   = "2099-12-31",
          invalid_reason   = NA_character_
        ) %>%
        dplyr::distinct() %>%
        dplyr::select(dplyr::all_of(
          c(
            "concept_code_1",
            "concept_code_2",
            "relationship_id",
            "valid_start_date",
            "valid_end_date",
            "invalid_reason"
          )
        ))
      # > concept_relationship_stage2
      # # A tibble: 1,295,158 × 6
      # concept_code_1 concept_code_2 relationship_id valid_start_date valid_end_date invalid_reason
      # <chr>          <chr>          <chr>           <chr>            <chr>          <chr>
      #   1 C1000          C1504          Is a            1970-01-01       2099-12-31     NA
      # 2 C10000         C61007         Is a            1970-01-01       2099-12-31     NA
      # 3 C100000        C99521         Is a            1970-01-01       2099-12-31     NA
      # 4 C100001        C99521         Is a            1970-01-01       2099-12-31     NA
      # 5 C100002        C99521         Is a            1970-01-01       2099-12-31     NA
      # 6 C100003        C80449         Is a            1970-01-01       2099-12-31     NA
      # 7 C100004        C80430         Is a            1970-01-01       2099-12-31     NA
      # 8 C100005        C15839         Is a            1970-01-01       2099-12-31     NA
      # 9 C100006        C139982        Is a            1970-01-01       2099-12-31     NA
      # 10 C100007        C99896         Is a            1970-01-01       2099-12-31     NA
      # # … with 1,295,148 more rows

      ############################ Diffs from Previous Version #############################
      # Reading prior relationship file
      current_file <-
        file.path(omop_folder, "RELATIONSHIP.csv")

      current_relationship <-
        readr::read_csv(
          file = current_file,
          col_types = readr::cols(.default = "c"))


      # > current_relationship
      # # A tibble: 134 × 6
      # relationship_id                      relationship_name                    is_hierarchical defines_ancestry reverse_relationship_id relationship_concept_id
      # <chr>                                <chr>                                <chr>           <chr>            <chr>                   <chr>
      #   1 Is a                                 subClassOf (NCIt)                    1               1                NA                      7001000001
      # 2 Subsumes                             Subsumes (CAI)                       1               1                NA                      7001000002
      # 3 Concept_In_Subset                    Concept_In_Subset                    0               0                NA                      7001000003
      # 4 Chemotherapy_Regimen_Has_Component   Chemotherapy_Regimen_Has_Component   0               0                NA                      7001000004
      # 5 Procedure_Has_Target_Anatomy         Procedure_Has_Target_Anatomy         0               0                NA                      7001000005
      # 6 Disease_Has_Associated_Anatomic_Site Disease_Has_Associated_Anatomic_Site 0               0                NA                      7001000006
      # 7 Disease_Has_Associated_Disease       Disease_Has_Associated_Disease       0               0                NA                      7001000007
      # 8 Disease_Has_Primary_Anatomic_Site    Disease_Has_Primary_Anatomic_Site    0               0                NA                      7001000008
      # 9 Disease_Has_Normal_Tissue_Origin     Disease_Has_Normal_Tissue_Origin     0               0                NA                      7001000009
      # 10 Disease_Has_Normal_Cell_Origin       Disease_Has_Normal_Cell_Origin       0               0                NA                      7001000010
      # # … with 124 more rows

      # Table is slimmed down for upcoming join
      # with newest relationships
      current_relationship <-
        current_relationship %>%
        dplyr::distinct(relationship_id, relationship_name, relationship_concept_id)
      # > current_relationship
      # # A tibble: 134 × 3
      # relationship_id                      relationship_name                    relationship_concept_id
      # <chr>                                <chr>                                <chr>
      #   1 Is a                                 subClassOf (NCIt)                    7001000001
      # 2 Subsumes                             Subsumes (CAI)                       7001000002
      # 3 Concept_In_Subset                    Concept_In_Subset                    7001000003
      # 4 Chemotherapy_Regimen_Has_Component   Chemotherapy_Regimen_Has_Component   7001000004
      # 5 Procedure_Has_Target_Anatomy         Procedure_Has_Target_Anatomy         7001000005
      # 6 Disease_Has_Associated_Anatomic_Site Disease_Has_Associated_Anatomic_Site 7001000006
      # 7 Disease_Has_Associated_Disease       Disease_Has_Associated_Disease       7001000007
      # 8 Disease_Has_Primary_Anatomic_Site    Disease_Has_Primary_Anatomic_Site    7001000008
      # 9 Disease_Has_Normal_Tissue_Origin     Disease_Has_Normal_Tissue_Origin     7001000009
      # 10 Disease_Has_Normal_Cell_Origin       Disease_Has_Normal_Cell_Origin       7001000010
      # # … with 124 more rows

      # Current relationship rows are
      # isolated in OMOP format to be compared to
      # prior relationship file. Any diffs are
      # assigned a new concept id
      relationship_stage <-
        concept_relationship_stage %>% # concept_relationship_stage2 isn't used because it is missing `relationship_name` field
        dplyr::transmute(
          relationship_id,
          relationship_name,
          is_hierarchical,
          defines_ancestry
        ) %>%
        dplyr::distinct()

      # Assigning new concept ids to new
      # relationships while retaining the old ones
      # from prior versions
      relationship_stage2 <-
      relationship_stage %>%
        dplyr::left_join(current_relationship,
                         by = c("relationship_id",
                                "relationship_name"))
      # > relationship_stage2
      # # A tibble: 134 × 5
      # relationship_id                      relationship_name                    is_hierarchical defines_ancestry relationship_concept_id
      # <chr>                                <chr>                                          <dbl>            <dbl> <chr>
      #   1 Is a                                 subClassOf (NCIt)                                  1                1 7001000001
      # 2 Subsumes                             Subsumes (CAI)                                     1                1 7001000002
      # 3 Concept_In_Subset                    Concept_In_Subset                                  0                0 7001000003
      # 4 Chemotherapy_Regimen_Has_Component   Chemotherapy_Regimen_Has_Component                 0                0 7001000004
      # 5 Procedure_Has_Target_Anatomy         Procedure_Has_Target_Anatomy                       0                0 7001000005
      # 6 Disease_Has_Associated_Anatomic_Site Disease_Has_Associated_Anatomic_Site               0                0 7001000006
      # 7 Disease_Has_Associated_Disease       Disease_Has_Associated_Disease                     0                0 7001000007
      # 8 Disease_Has_Primary_Anatomic_Site    Disease_Has_Primary_Anatomic_Site                  0                0 7001000008
      # 9 Disease_Has_Normal_Tissue_Origin     Disease_Has_Normal_Tissue_Origin                   0                0 7001000009
      # 10 Disease_Has_Normal_Cell_Origin       Disease_Has_Normal_Cell_Origin                     0                0 7001000010
      # # … with 124 more rows

      # If there are any new relationships in the newest version of NCIt,
      # a new concept id is assigned.
      # The last relationship_id is taken from current relationships based on the max value. Integer cannot be
      # used because the value is too large to be treated as one so it is first converted to double
      # and then to character
      last_relationship_id <- as.character(max(as.double(current_relationship$relationship_concept_id)))
      # > last_relationship_id
      # [1] "7001000136"

      # Any new relationships are subset
      # and if there are any rows, new relationship_concept_id
      # is generated for each
      relationship_stage3a <-
        relationship_stage2 %>%
        dplyr::filter(is.na(relationship_concept_id))

      if (nrow(relationship_stage3a)>0) {

        cli::cli_inform("{nrow(relationship_stage3a)} new relationship{?s} found:")
        huxtable::print_screen(
        huxtable::hux(relationship_stage3a) %>%
          huxtable::theme_article(),
        colnames = FALSE
        )

        relationship_stage3a <-
          relationship_stage3a %>%
          dplyr::distinct() %>%
          tibble::rowid_to_column("rowid") %>%
          dplyr::mutate(relationship_concept_id =
                          as.character(as.double(last_relationship_id)+rowid)) %>%
          dplyr::select(-rowid) %>%
          dplyr::distinct()

        cli::cli_inform("{.var relationship_concept_id} {as.character(min(as.double(relationship_stage3a$relationship_concept_id)))} to {as.character(max(as.double(relationship_stage3a$relationship_concept_id)))} assigned.")


      } else {

        cli::cli_inform("No new relationships found in Neo4j version {neo4j_nci_version} compared to OMOP version {omop_nci_version}.")

      }


      ########### New Relationships (if any) are combined with the old relationships ################
      relationship_stage3b <-
        relationship_stage2 %>%
        dplyr::filter(!is.na(relationship_concept_id))

      relationship_stage4 <-
        dplyr::bind_rows(
          relationship_stage3a,
          relationship_stage3b
        )


      # Getting existing concept_ids
      current_file <-
        file.path(omop_folder, "CONCEPT.csv")

      current_concept <-
        readr::read_csv(
          file = current_file,
          col_types = readr::cols(.default = "c")) %>%
        dplyr::distinct(concept_code,
                        concept_id,
                        valid_start_date,
                        valid_end_date,
                        invalid_reason)


      # Joining to previous CONCEPT by code to determine
      # new concept_id assignment
      concepts_staged1 <-
      concepts_staged %>%
        dplyr::left_join(current_concept,
                         by =  c("concept_code"),
                         suffix = c(".new", ".current")) %>%
        # If a previously valid concept has been deprecated, the
        # `valid_end_date` is updated to current date
        dplyr::transmute(
          concept_id,
          concept_name,
          domain_id,
          vocabulary_id,
          concept_class_id,
          standard_concept,
          concept_code,
          valid_start_date = dplyr::coalesce(valid_start_date.current, valid_start_date.new),
          valid_end_date_update = ifelse(invalid_reason.new == 'D' & is.na(invalid_reason.current), as.character(Sys.Date()), NA_character_),
          valid_end_date.new,
          valid_end_date.current,
          invalid_reason = invalid_reason.new) %>%
        dplyr::transmute(
          concept_id,
          concept_name,
          domain_id,
          vocabulary_id,
          concept_class_id,
          standard_concept,
          concept_code,
          valid_start_date,
          valid_end_date = dplyr::coalesce(valid_end_date_update, valid_end_date.new, valid_end_date.current),
          invalid_reason) %>%
        dplyr::mutate(
          valid_end_date = ifelse(valid_end_date == "1970-01-01" & valid_start_date > valid_end_date, valid_start_date, valid_end_date)) %>%
        dplyr::distinct()

      concepts_staged2a <-
        concepts_staged1 %>%
        dplyr::filter(is.na(concept_id))

      if (nrow(concepts_staged2a)>0) {

        cli::cli_inform("{nrow(concepts_staged2a)} new concept{?s} found. Sample:")
        huxtable::print_screen(
          huxtable::hux(head(concepts_staged2a)) %>%
            huxtable::theme_article(),
          colnames = FALSE
        )

        concepts_staged2a <-
          concepts_staged2a %>%
          dplyr::mutate(concept_id =
                          as.character(70000000000+as.double(stringr::str_remove_all(concept_code, pattern = "^C"))),
                        valid_start_date = as.character(Sys.Date()))

        cli::cli_inform("{nrow(concepts_staged2a)} new concept{?s} found. Update sample:")
        huxtable::print_screen(
          huxtable::hux(head(concepts_staged2a)) %>%
            huxtable::theme_article(),
          colnames = FALSE
        )



        cli::cli_inform("{.var concept_id} {min(concepts_staged2a$concept_id)} to {max(concepts_staged2a$concept_id)} assigned.")


      } else {

        cli::cli_inform("No new concepts found in Neo4j version {neo4j_nci_version} compared to OMOP version {omop_nci_version}.")


      }

      concepts_staged2b <-
        concepts_staged1 %>%
        dplyr::filter(!is.na(concept_id))

      concepts_staged3 <-
        dplyr::bind_rows(
          concepts_staged2a,
          concepts_staged2b) %>%
        dplyr::distinct()

      concept_relationship_stage3 <-
        concept_relationship_stage2 %>%
        dplyr::left_join(concepts_staged3 %>%
          dplyr::rename_all(function(x) sprintf("%s_1", x)),
        by = c("concept_code_1")
        ) %>%
        dplyr::left_join(concepts_staged3 %>%
          dplyr::rename_all(function(x) sprintf("%s_2", x)),
        by = c("concept_code_2")
        ) %>%
        dplyr::select(dplyr::all_of(
          c(
            "concept_id_1",
            "concept_id_2",
            "relationship_id",
            "valid_start_date",
            "valid_end_date",
            "invalid_reason"
          )
        )) %>%
        dplyr::mutate(valid_start_date =
                        ifelse(relationship_id %in% relationship_stage3a$relationship_id,
                               as.character(Sys.Date()),
                               valid_start_date)) %>%
        dplyr::distinct()

      roots <-
        concepts_staged3 %>%
        dplyr::filter(concept_class_id == "Root") %>%
        dplyr::select(concept_id) %>%
        unlist() %>%
        unname()

      roots_list <-
        vector(
          mode = "list",
          length = length(roots)
        ) %>%
        set_names(concepts_staged3 %>%
          dplyr::filter(concept_class_id == "Root") %>%
          dplyr::select(concept_name) %>%
          unlist() %>%
          unname())

      tmp_folder <-
        file.path(
          omop_folder,
          "tmp"
        )

      if (dir.exists(tmp_folder)) {

        unlink(tmp_folder,
               recursive = TRUE)

      }

      dir.create(tmp_folder)

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
          continue <- TRUE
          i <- 0

          while (continue == TRUE) {

            i <- i + 1

            if (i == 1) {
              output[[i]] <-
                concept_relationship_stage3 %>%
                dplyr::filter(
                  relationship_id == "Subsumes",
                  concept_id_1 == root
                ) %>%
                dplyr::transmute(
                  ancestor_concept_id_1 = root,
                  descendant_concept_id_1 = concept_id_2,
                  ancestor_concept_id_2 = concept_id_2
                ) %>%
                dplyr::distinct()
            } else {

              x <-
                output[[i - 1]] %>%
                dplyr::select_at(dplyr::vars(3)) %>%
                dplyr::rename_all(function(x) str_replace_all(x, pattern = "^.*$", replacement = "concept_id_1")) %>%
                dplyr::inner_join(
                  concept_relationship_stage3 %>%
                  dplyr::filter(relationship_id == "Subsumes"),
                  by = "concept_id_1") %>%
                dplyr::transmute(
                  ancestor_concept_id = concept_id_1,
                  descendant_concept_id = concept_id_2,
                  next_ancestor_concept_id = concept_id_2) %>%
                dplyr::distinct()

              if (nrow(x)==0) {

                continue <- FALSE

              } else {

                colnames(x) <-
                  c(
                    sprintf("ancestor_concept_id_%s", i),
                    sprintf("descendant_concept_id_%s", i),
                    sprintf("ancestor_concept_id_%s", i+1)
                  )

              output[[i]] <- x

              }
            }
          }


          quietly_left_join <-
            function(x,
                     y,
                     by = NULL,
                     copy = FALSE,
                     suffix = c(".x", ".y"),
                     ...,
                     keep = FALSE) {

              suppressMessages(
                dplyr::left_join(
                  x = x,
                  y = y,
                  by = by,
                  copy = copy,
                  suffix = suffix,
                  ...,
                  keep = keep
                )
              )


            }
          output2 <-
            output %>%
            purrr::reduce(quietly_left_join) %>%
            dplyr::select(starts_with("ancestor_concept_id_")) %>%
            dplyr::rename_all(str_remove_all, "ancestor_concept_id_") %>%
            dplyr::distinct() %>%
            tibble::rowid_to_column("rowid")


          paths_df <-
            output2 %>%
            tidyr::pivot_longer(
              cols = !rowid,
              names_to = "levels_of_separation",
              values_to = "concept_id",
              values_drop_na = TRUE,
              names_transform = as.integer
            )


          max_level <- max(paths_df$levels_of_separation)

          tmp_ca <- list()

          for (level in 1:max_level) {

            tmp_ca[[level]] <-
            paths_df %>%
              dplyr::group_by(rowid) %>%
              dplyr::filter(levels_of_separation == level) %>%
              dplyr::ungroup() %>%
              dplyr::transmute(rowid,
                        ancestor_concept_id = concept_id) %>%
              dplyr::left_join(paths_df %>%
                          dplyr::transmute(
                            rowid,
                            descendant_concept_id = concept_id,
                            levels_of_separation = levels_of_separation - level),
                        by = "rowid") %>%
              dplyr::filter(levels_of_separation >= 0)




          }

          tmp_ca2 <-
            dplyr::bind_rows(tmp_ca) %>%
            dplyr::select(-rowid) %>%
            dplyr::distinct()


          tmp_ca3 <-
            tmp_ca2 %>%
            dplyr::group_by(ancestor_concept_id,
                     descendant_concept_id) %>%
            dplyr::summarize(min_levels_of_separation =
                        min(levels_of_separation),
                      max_levels_of_separation =
                        max(levels_of_separation),
                      .groups = "drop") %>%
            dplyr::ungroup() %>%
            dplyr::distinct()

          readr::write_csv(
            x = tmp_ca3,
            file = tmp_root_file,
            na = ""
          )

          cli::cli_text(
            "[{as.character(Sys.time())}] {.strong {names(roots_list)[j]}} {sprintf('%s/%s',j, total)} ({paste0(signif(((j/total) * 100),
        digits = 2), '%')})"
          )
        }
      }


      j <- 0
      roots_list2 <- list()
      for (root in roots) {
        jj <- j
        j <- j + 1

        tmp_root_file <-
          file.path(
            tmp_folder,
            xfun::with_ext(names(roots_list)[j], "csv")
          )

        roots_list2[[length(roots_list2)+1]] <-
        readr::read_csv(file = tmp_root_file,
                        show_col_types = FALSE)

      }

      concept_ancestor_stage <-
        dplyr::bind_rows(roots_list2) %>%
        dplyr::distinct()

      # Finding ancestor descendant pairs that
      # have more than 1 level associated to convert to min and maxes.
      # This data frame will likely be 0 rows, but it is
      # just a QA measure to ensure that there are no duplicates
      concept_ancestor_stage_b <-
        concept_ancestor_stage %>%
        dplyr::count(
          ancestor_concept_id,
          descendant_concept_id
        ) %>%
        dplyr::filter(n > 1) %>% # Filter will likely result in 0 rows
        dplyr::select(-n) %>%
        dplyr::left_join(concept_ancestor_stage,
          by = c("ancestor_concept_id", "descendant_concept_id")
        ) %>%
        tidyr::pivot_longer(
          cols = c(
            min_levels_of_separation,
            max_levels_of_separation
          ),
          names_to = "level",
          values_to = "value"
        ) %>%
        dplyr::select(-level) %>%
        dplyr::group_by(
          ancestor_concept_id,
          descendant_concept_id
        ) %>%
        dplyr::mutate(
          min_levels_of_separation =
            min(value, na.rm = TRUE),
          max_levels_of_separation =
            max(value, na.rm = TRUE)
        ) %>%
        dplyr::ungroup() %>%
        dplyr::select(-value) %>%
        dplyr::distinct()

      concept_ancestor_stage2 <-
        dplyr::left_join(
          concept_ancestor_stage,
          concept_ancestor_stage_b,
          by = c("ancestor_concept_id", "descendant_concept_id"),
          suffix = c("", "_updated")
        ) %>%
        dplyr::mutate(
          min_levels_of_separation =
            dplyr::coalesce(
              min_levels_of_separation_updated,
              min_levels_of_separation
            ),
          max_levels_of_separation =
            dplyr::coalesce(
              max_levels_of_separation_updated,
              max_levels_of_separation
            )
        ) %>%
        dplyr::select(-ends_with("_updated")) %>%
        dplyr::distinct()


      # Concept Synonym
      concept_synonym_stage <-
        node %>%
        dplyr::select(
          code,
          Preferred_Name,
          FULL_SYN
        ) %>%
        tidyr::separate_rows(FULL_SYN,
          sep = "[|]{1}"
        ) %>%
        dplyr::mutate(
          lc_preferred_name =
            tolower(Preferred_Name)
        ) %>%
        dplyr::mutate(
          lc_full_syn =
            tolower(FULL_SYN)
        ) %>%
        dplyr::mutate(
          is_synonym =
            lc_preferred_name != lc_full_syn
        ) %>%
        dplyr::filter(is_synonym == TRUE) %>%
        dplyr::select(
          concept_code = code,
          concept_name = Preferred_Name,
          concept_synonym_name = FULL_SYN
        ) %>%
        dplyr::distinct()

      concept_synonym_stage2 <-
        concept_synonym_stage %>%
        dplyr::left_join(concepts_staged3,
          by = c("concept_code", "concept_name")
        ) %>%
        dplyr::transmute(
          concept_id,
          concept_synonym_name,
          language_concept_id = 4180186
        ) %>%
        dplyr::distinct()

      concept_synonym_stage2_b <-
        concepts_staged3 %>%
        dplyr::transmute(
          concept_id,
          concept_synonym_name = concept_name,
          language_concept_id = 4180186
        ) %>%
        dplyr::distinct()


      concept_synonym_stage3 <-
        dplyr::bind_rows(
          concept_synonym_stage2,
          concept_synonym_stage2_b) %>%
        dplyr::distinct() %>%
        arrange(concept_id)


      ############## METADATA TABLES ####################
      # 1. Concept Class
      # 2. Vocabulary
      concept_class_stage <-
        concepts_staged3 %>%
        dplyr::transmute(concept_class_id,
                         concept_class_name = concept_class_id,
                         concept_class_concept_id = NA_integer_
        ) %>%
        dplyr::distinct() %>%
        tibble::rowid_to_column("rowid") %>%
        dplyr::mutate(concept_class_concept_id = 7000000001 + rowid) %>%
        dplyr::select(-rowid)

      # Metadata Tables VOCABULARY requires
      # no processing and is given 7000000000 (7-billion prefix)
      # Other metadata tables with 7000000000: CONCEPT_CLASS (produced from
      # final CONCEPTS table)
      vocabulary_stage <-
        tibble(
          vocabulary_id,
          vocabulary_name,
          vocabulary_reference = "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/",
          vocabulary_version = neo4j_nci_version,
          vocabulary_concept_id = 7000000001
        )


      ############ Writing Output
      output_map <-
        list(
          CONCEPT = concepts_staged3,
          CONCEPT_SYNONYM = concept_synonym_stage3,
          CONCEPT_ANCESTOR = concept_ancestor_stage2,
          CONCEPT_RELATIONSHIP = concept_relationship_stage3,
          VOCABULARY = vocabulary_stage,
          RELATIONSHIP = relationship_stage4,
          CONCEPT_CLASS = concept_class_stage
        ) %>%
        map(dplyr::distinct)


      ##### Tests ####

      if (!all(output_map$CONCEPT_RELATIONSHIP$concept_id_1 %in% output_map$CONCEPT$concept_id)) {


        cli::cli_warn("Invalid concept_id_1s present.")


      }

      if (!all(output_map$CONCEPT_RELATIONSHIP$concept_id_2 %in% output_map$CONCEPT$concept_id)) {


        cli::cli_warn("Invalid concept_id_2s present.")


      }

      if (!all(output_map$CONCEPT_SYNONYM$concept_id %in% output_map$CONCEPT$concept_id)) {


        cli::cli_warn("Invalid concept_ids present in CONCEPT_SYNONYM.")


      }

      if (!all(output_map$CONCEPT_ANCESTOR$ancestor_concept_id %in% output_map$CONCEPT$concept_id)) {


        cli::cli_warn("Invalid ancestor_concept_ids present.")


      }

      if (!all(output_map$CONCEPT_ANCESTOR$descendant_concept_id %in% output_map$CONCEPT$concept_id)) {


        cli::cli_warn("Invalid descendant_concept_ids present.")


      }


      for (i in seq_along(output_map)) {
        readr::write_csv(
          x = output_map[[i]],
          file = file.path(omop_folder, xfun::with_ext(names(output_map)[i], "csv")),
          na = "",
          quote = "all"
        )
      }

      #### CROSSWALK ####
      # Getting the crosswalks from the newest nodes file
      omop_crosswalk_folder <-
        file.path(omop_folder,
                  "crosswalk")
      if (dir.exists(omop_crosswalk_folder)) {
        unlink(omop_crosswalk_folder,
               recursive = TRUE)
      }
      dir.create(omop_crosswalk_folder)

      # Crosswalk between NCIt code and UMLS CUI
      crosswalk <-
      node %>%
        dplyr::transmute(
            ncit_code = code,
            target_code = UMLS_CUI,
            target_vocabulary = "UMLS") %>%
        dplyr::distinct()

      readr::write_csv(
        x = crosswalk,
        file = file.path(omop_crosswalk_folder, "ncit_to_umls.csv"),
        na = "",
        quote = "all"
      )

      # Crosswalk between NCIt code and FDA UNII
      crosswalk <-
        node %>%
        dplyr::transmute(ncit_code = code,
                      target_code  = FDA_UNII_Code,
                      target_vocabulary = "FDA_UNII") %>%
        dplyr::distinct()

      readr::write_csv(
        x = crosswalk,
        file = file.path(omop_crosswalk_folder, "ncit_to_fda_unii.csv"),
        na = "",
        quote = "all"
      )

      # Crosswalk between NCIt code and NCI Metathesaurus CUI
      crosswalk <-
        node %>%
        dplyr::transmute(ncit_code = code,
                      target_code  = NCI_META_CUI,
                      target_vocabulary = "NCIm") %>%
        dplyr::distinct()

      readr::write_csv(
        x = crosswalk,
        file = file.path(omop_crosswalk_folder, "ncit_to_ncim.csv"),
        na = "",
        quote = "all"
      )

      # Crosswalk between NCIt code and OMIM
      crosswalk <-
        node %>%
        dplyr::transmute(ncit_code = code,
                      target_code  = OMIM_Number,
                      target_vocabulary = "OMIM") %>%
        dplyr::distinct()

      readr::write_csv(
        x = crosswalk,
        file = file.path(omop_crosswalk_folder, "ncit_to_omim.csv"),
        na = "",
        quote = "all"
      )

      # Crosswalk between NCIt code and HGNC
      crosswalk <-
        node %>%
        dplyr::transmute(ncit_code = code,
                      target_code  = HGNC_ID,
                      target_vocabulary = "HGNC") %>%
        dplyr::distinct()

      readr::write_csv(
        x = crosswalk,
        file = file.path(omop_crosswalk_folder, "ncit_to_hgnc.csv"),
        na = "",
        quote = "all"
      )

      # Crosswalk between NCIt code and icdo3
      crosswalk <-
        node %>%
        dplyr::transmute(ncit_code = code,
                      target_code  = `ICD-O-3_Code`,
                      target_vocabulary = "ICDO3") %>%
        dplyr::distinct()

      readr::write_csv(
        x = crosswalk,
        file = file.path(omop_crosswalk_folder, "ncit_to_icdo3.csv"),
        na = "",
        quote = "all"
      )

      # Aggregating all the crosswalks into the CONCEPT_CROSSWALK table
      concept_crosswalk <-
      list.files(omop_crosswalk_folder,
                 full.names = TRUE,
                 pattern = "csv") %>%
        purrr::map(readr::read_csv,
                   col_types = readr::cols(.default = "c")) %>%
        dplyr::bind_rows() %>%
        # some values are in pipe-separated strings
        tidyr::separate_rows(target_code, sep = "[|]{1}") %>%
        dplyr::filter(!is.na(target_code)) %>%
        dplyr::distinct() %>%
        dplyr::mutate(target_code =
                        ifelse(target_code == 'OMIM_Number "617823"',
                               '617823',
                               target_code))

      # qa
      qa <-
      concept_crosswalk %>%
        mutate(
          validity_check =
            case_when(target_vocabulary == 'FDA_UNII' & grepl(pattern = "^[0-9A-Z]{1,}$", target_code) ~ TRUE,
                      target_vocabulary == 'HGNC' & grepl(pattern = "^HGNC[:]{1}[0-9]{1,}$", target_code) ~ TRUE,
                      target_vocabulary == 'ICDO3' & grepl(pattern = "^[0-9]{1,}[/]{1}[0-9]{1,}$|^[0-9]{1,}[-]{1}[0-9]{1,}$", target_code) ~ TRUE,
                      target_vocabulary == 'NCIm' & grepl(pattern = "^CL[0-9]{1,}$", target_code) ~ TRUE,
                      target_vocabulary == 'OMIM' & grepl(pattern = "^[P]{0,1}[0-9]{1,}$", target_code) ~ TRUE,
                      target_vocabulary == 'UMLS' & grepl(pattern = "^C[0-9]{1,}$", target_code) ~ TRUE,
                      TRUE ~ FALSE)) %>%
        dplyr::filter(validity_check == FALSE)

      if (nrow(qa)>0) {

        cli::cli_warn("{nrow(qa)} invalid codes found in CONCEPT_CROSSWALK:")
        print(qa)

      }

      readr::write_csv(
        x = concept_crosswalk,
        file = file.path(omop_folder, "CONCEPT_CROSSWALK.csv"),
        na = "",
        quote = "all"
      )

      unlink(omop_crosswalk_folder,
             recursive = TRUE)


      ### README ####
      readme_file <- file.path(omop_folder, "README.md")
      cat(
        "# NCI Thesaurus (OMOP Format)  ",
        glue::glue("Version {neo4j_nci_version}  "),
        "---  ",
        "  ",
        file = readme_file,
        append = FALSE,
        sep = "\n"
      )

      csv_files <-
      list.files(omop_folder,
                 full.names = TRUE,
                 pattern = "csv")
      omop_data <-
        csv_files %>%
        purrr::map(readr::read_csv,
                   col_types = readr::cols(.default = "c")) %>%
        purrr::set_names(xfun::sans_ext(basename(csv_files)))

      for (i in seq_along(omop_data)) {

        omop_table <- omop_data[[i]]
        omop_table_name <- names(omop_data)[i]

        cat(
          glue::glue("## {omop_table_name}  "),
          glue::glue("Rows  : {nrow(omop_table)}  "),
          glue::glue("Fields: {ncol(omop_table)}  "),
          file = readme_file,
          append = TRUE,
          sep = "\n"
        )

        cat("\n",
            file = readme_file,
            append = TRUE)

        omop_table_list <- as.list(omop_table)
        omop_table_list_sm <-
          omop_table_list %>%
          purrr::map(
            function(x) forcats::fct_count(x, sort = TRUE, prop = TRUE))

        for (j in seq_along(omop_table_list_sm)) {

          cat(
            c(glue::glue("\n### {names(omop_table_list_sm)[j]}  "),
              glue::glue("{nrow(omop_table_list_sm[[j]])} x {ncol(omop_table_list_sm[[j]])}  \n"),
              "  \n",
              tbl_to_gfm(omop_table_list_sm[[j]] %>% dplyr::slice(1:10)),
              "\n"),
            file = readme_file,
            sep = "  \n",
            append = TRUE)

          cat("\n",
              file = readme_file,
              sep = "  \n",
              append = TRUE)


        }




      }

      cli::cli_inform("{cli::symbol$tick} OMOP Tables available at '{omop_folder}'")

      # Add and commit newest version to repo
      system(glue::glue("git add {omop_folder}"))
      system(glue::glue("git commit -m 'bump Neo4j version {neo4j_nci_version}'"))

  }
