# Data QC
dima_data_list <- readRDS("QC/all_dima_pks.Rdata")
primarykey_qc<-read.csv("QC/primarykey_resolve_2025-06-27.csv")
dima_data_qc <-function(dima_data_list, primarykey_qc){
  # we've already identified a few plots as problematic while generating the PrimaryKey, let's remove those
  problem_pk <- primarykey_qc$PrimaryKey[primarykey_qc$Action=="Delete"]

  # check lat/longs
  coord_qc <- dima_data_list[["tblPlots"]] |> subset(is.na(Latitude)|is.na(Longitude)|Latitude==0|Longitude==0) |>
    # remove previously identified PrimaryKeys
    subset(!PrimaryKey %in% problem_pk) |>
    dplyr::select(project, dbname, PlotKey, PlotID, PrimaryKey, Latitude, Longitude) |>
    tidyr::pivot_longer(cols = -c(project, dbname, PlotKey, PlotID, PrimaryKey,),
                        names_to = "Field",
                        values_to = "n_missing") |>
    dplyr::mutate(n_missing = 1,
                  Notes = "Coordinates missing 0 or missing",
                  Action = "Populate or delete plot")

  # check for NAs in observations
  missingness <- do.call(rbind,lapply(X = names(dima_data_list),

                                      function(X){
                                        data <- dima_data_list[[X]]

                                        # for tables with PrimaryKeys, check for NAs in columns
                                        if("PrimaryKey" %in% colnames(data)){
                                          # remove previously identified PrimaryKeys
                                          data <- data|>
                                            subset(!PrimaryKey %in% problem_pk)

                                          # identify number of missing rows per field
                                          missingness <- data|>
                                            dplyr::group_by(project, dbname, PrimaryKey) |>
                                            dplyr::summarise(dplyr::across(dplyr::everything(), ~ sum(is.na(.x))))|>
                                            dplyr::ungroup()

                                          # pivot longer so we can summarize
                                          missingness_tall <-  missingness |>
                                            tidyr::pivot_longer(cols = -c("project", "dbname", "PrimaryKey"),
                                                                names_to = "Field",
                                                                values_to = "n_missing")

                                          missingness_summary <- missingness_tall |>
                                            dplyr::group_by(project, dbname,Field) |>
                                            dplyr::summarise(
                                              avg_missing = mean(n_missing),
                                              n_records = dplyr::n()

                                            ) |> dplyr::ungroup()

                                          # join back to tall table
                                          missingness_tall <- missingness_tall |>
                                            dplyr::left_join(missingness_summary) |>
                                            # add interpreation. If the number missing > standard deviation, we'll flag that
                                            dplyr::mutate(
                                              anomaly = (n_missing-avg_missing),
                                              prop_missing = n_missing/n
                                            ) |>

                                            # add table identifier
                                            dplyr::mutate(table = X)
                                        }

                                      })
  )

  # Add notes based on the importance of fields
  missingness_notes <- missingness |>
    # add in PlotID info
    dplyr::left_join(dima_data_list[["tblPlots"]] |> dplyr::select(PrimaryKey, PlotID, PlotKey)) |>
    # subset where this is no anomaly
    subset(anomaly!=0) |>
    # subset where there are no missing values
    subset(n_missing>0) |>
    dplyr::left_join(read.csv("table_fields_importance.csv",
                              na.strings = c("", "NA"))) |>
    # join in the coord_qc table for a comprehensive report
    dplyr::bind_rows(coord_qc)|>
    dplyr::arrange(Notes, Action) |>

    # rearrange for readability
    dplyr::relocate(project, dbname, table, PlotKey, PlotID, PrimaryKey) |>

    # add a data owner response column
    dplyr::mutate(DataOwnerResponse = NA)
}

SWBC_check <- dima_data_qc(dima_data_list = dima_data_list,
                           primarykey_qc = primarykey_qc)
write.csv(SWBC_check, "QC/SWBC_DIMA_check_all.csv")
write.csv(SWBC_check |> subset(!is.na(Action)), "QC/SWBC_DIMA_check_resolve.csv")
