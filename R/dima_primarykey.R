# TODO functionalize

options(scipen = 999)

# set up directories for project
if(!dir.exists("original_files")) dir.create("original_files")
if(!dir.exists("dima_exports")) dir.create("dima_exports")
if(!dir.exists("QC")) dir.create("QC")

# read in DIMA export files
##path_github <- "C:/Users/Brandi.Wheeler/Box/Docs for data prep/Data/" # path where species list and schema are stored
path_project <-"dima_exports/" # path where data for preparation are stored

# get list of all export files
dima_export_files <- data.frame(file_path = list.files(path = path_project,
                                                       pattern = ".csv",
                                                       recursive = T,
                                                       include.dirs = T)) |>
  tidyr::separate_wider_delim(
    file_path,
    "/",
    names = c("project", "dbname", "table"),
    cols_remove = FALSE)

# read all DIMA types and append
all_dimas <- lapply(X = unique(dima_export_files$table),
                    FUN = function(X) {
                      # read each file associated with a data_type
                      file_list <- dima_export_files$file_path[dima_export_files$table==X]
                      # read the all files associated with a particular DIMA table type and append
                      data <- do.call(rbind, lapply(X = file_list,
                                                    FUN = function(X) {
                                                      print(X)
                                                      data <- read.csv(paste0(path_project,X),
                                                                       # change blanks to NA
                                                                       na.strings = c("", "NA")) |>
                                                        # add file path
                                                        dplyr::mutate(file_path = X) |>
                                                        # join to dima_export file to get project, dbname, and table name
                                                        dplyr::left_join(dima_export_files) |>
                                                        dplyr::select(-c(file_path, table))
                                                    })
                      )
                    })

#name all of the tables in the all_dimas list
names(all_dimas) <- unique(dima_export_files$table) |> stringr::str_remove(".csv")

# create PrimaryKeys by joining PlotKey to DateVisited. We first have to join to the header tables, then the detail tables
header_tables <- lapply(X = all_dimas[names(all_dimas) |> stringr::str_detect("Header")],
                        function(X){
                          # if there is already a PlotKey, no need to do anything, otherwise we need to join PlotKey to the table via tblLines
                          if(!"PlotKey" %in% names(X)){
                            data_pk <- dplyr::left_join(
                              X,
                              all_dimas$tblLines |>
                                dplyr::select(PlotKey, LineKey, project, dbname)|> dplyr::distinct(),
                              relationship = "many-to-one")
                          }else{
                            data_pk <- X
                          }
                          # Now generate Primarykey based on PlotKey and FormDate
                          data_pk <- data_pk |>
                            dplyr::mutate(
                              #Format FormDate
                              DateVisited = FormDate |> lubridate::mdy_hms(),
                              PrimaryKey = paste0(PlotKey, DateVisited))
                        })


# join header and detail tables to add PrimaryKey
detail_list <- names(all_dimas)[names(all_dimas) |> stringr::str_detect("Detail")]
detail_tables <- lapply(
  # we will work one method at a time through the list
  X = detail_list,
  function(X){
    # we need to find the associated header table
    tblDetail <- all_dimas[[X]]
    tblHeader <- header_tables[[X |> stringr::str_replace(pattern = "Detail",
                                                          replacement = "Header")]]

    # if tblHeader exists, proceed with join
    if(!is.null(tblHeader)){
      data_pk <- dplyr::left_join(
        # join detail table to header
        tblDetail,
        tblHeader |>
          dplyr::select_if(names(tblHeader) %in% c("PlotKey", "LineKey", "RecKey", "FormDate", "PrimaryKey", "DateVisited", "project", "dbname")),
        relationship = "many-to-one")
    }else{
      print(paste("No header for table", X, "No join performed. Check that this is expected"))
      all_dimas[[X]]
    }
  })

names(detail_tables) <- detail_list

# merge the detail and header tables together
detail_header <- c(detail_tables, header_tables)

# we also need to get PrimaryKey information into the non-Line based data
no_lines_tables <- all_dimas[!names(all_dimas) |> stringr::str_detect("Header|Detail")] |> names()
data_no_lines <- lapply(X = no_lines_tables,
                        function(X){
                          # For tblPlotsNotes, create a PrimaryKey from PlotKey and NoteDate
                          if(X=="tblPlotNotes"){
                            data <- all_dimas[[X]] |> dplyr::mutate(
                              DateVisited = NoteDate |> lubridate::mdy_hms(),
                              PrimaryKey = paste0(PlotKey, DateVisited))
                          }else
                            # For tblPlotHistory, create a PrimaryKey from PlotKey and DateRecorded
                            if(X=="tblPlotHistory"){
                              data <- all_dimas[[X]] |> dplyr::mutate(
                                DateVisited = DateRecorded |> lubridate::mdy_hms(),
                                PrimaryKey = paste0(PlotKey, DateVisited))
                            }else
                              # For tblSoilPits, create add a PlotKey and DateVisited. We'll join PrimaryKey later for all plots
                              if(X=="tblSoilPits"){
                                data <- all_dimas[[X]] |> dplyr::mutate(
                                  DateRecorded = DateRecorded |> lubridate::mdy_hms()
                                )
                              }else
                                # For tblSoilPitHorizons, first join with tblSoilPits, then
                                # add PlotKey and DateVisited
                                if(X=="tblSoilPitHorizons"){
                                  data <- dplyr::left_join(all_dimas[[X]],
                                                           all_dimas$tblSoilPits |>
                                                             dplyr::select(PlotKey, DateRecorded, SoilKey, project, dbname))|>
                                    dplyr::mutate(DateRecorded = DateRecorded |> lubridate::mdy_hms())
                                }else{
                                  all_dimas[[X]]
                                }
                        })

names(data_no_lines) <- no_lines_tables

# Plots, Lines, SoilPits, and SoilPit Horizons all need PrimaryKeys that correspond with visit of the PlotKey
table_plots<- c("tblPlots", "tblLines", "tblSoilPits", "tblSoilPitHorizons")

# get all of the unique method PrimaryKeys
unique_pks <- do.call(rbind,
                      lapply(X = names(detail_header),
                             FUN = function(X){
                               print(X)
                               # If PlotKey exists, we'll merge
                               if("PlotKey" %in% names(detail_header[[X]])){

                                 data <-detail_header[[X]] |>
                                   dplyr::select(PlotKey, PrimaryKey, DateVisited, project, dbname) |>
                                   dplyr::mutate(method = X) |>
                                   dplyr::distinct()
                               }else{
                                 message(paste("No PlotKeys found in table", X, ". This table will be dropped from output"))
                               }
                             })
) |>
  # make sure the methods are distinct, regardless of Header or Detail
  dplyr::mutate(method = method |> stringr::str_remove_all(
    pattern = "Detail|Header|tbl"
  )) |> dplyr::distinct()

# join to table_plots
plots_pks <- lapply(X = table_plots,
                    function(X){
                      print(X)
                      data <- data_no_lines[[X]] |>
                        dplyr::left_join(unique_pks |>
                                           # remove method
                                           dplyr::select(-method) |>
                                           dplyr::distinct(),
                                         relationship = "many-to-many")
                    })
names(plots_pks) <- table_plots


# put all the tables together
all_dimas_pks <- c(plots_pks, data_no_lines[!names(data_no_lines) %in% table_plots], detail_header)

# QC
# First, check that all tables that should have a PrimaryKey and DateVisited assigned
primarykey_check <- do.call(
  rbind,lapply(X = names(all_dimas_pks),
               function(X){
                 data <- all_dimas_pks[[X]]
                 data <- data.frame(table = X) |>
                   dplyr::mutate(primarykey_check = dplyr::if_else(
                     "PrimaryKey" %in% colnames(all_dimas_pks[[X]]),
                     "Yes", "No")
                   )
               })
)

# Print out the problem tables
if(nrow(primarykey_check[primarykey_check$primarykey_check=="No"&!primarykey_check$table %in%
                         c("tblSites", "tblSpecies", "tblSpeciesGeneric", "tblNestedFreqSpeciesSummary",
                           "tblNestedFreqSpeciesDetail"),])>0){
  primarykey_check[primarykey_check$primarykey_check=="No"&!primarykey_check$table %in%
                     c("tblSites", "tblSpecies", "tblSpeciesGeneric", "tblNestedFreqSpeciesSummary",
                       "tblNestedFreqSpeciesDetail"),]
}else{
  print("All PrimaryKeys assigned")
}


# QC PrimaryKeys and DateVisited
# First we'll see how identify any PrimaryKey issues (e.g., NA, orphaned records)
pk_date_check <- all_dimas_pks$tblPlots |>
  dplyr::select(PlotKey, PrimaryKey, DateVisited, dbname, project) |>
  # add method
  dplyr::mutate(method = "tblPlots")|>
  dplyr::distinct()|>
  # join to transect data observations
  dplyr::bind_rows(unique_pks)|>
  # make wider so we can compare by PrimaryKey
  # add a value row
  dplyr::mutate(values = "yes") |>
  tidyr::pivot_wider(names_from = method,
                     values_from = values,
                     values_fill = "no")


# Identify PrimaryKeys where date visits are close to each other--this could mean that unique plots are improperly assigned
pk_date_check <- pk_date_check |> dplyr::group_by(PlotKey) |>
  dplyr::arrange(desc(DateVisited)) |>
  dplyr::mutate(ClosestDateVisited = dplyr::lead(DateVisited))|>
  dplyr::mutate(DaysDiff = difftime(DateVisited, ClosestDateVisited, units = "days") |>
                  # convert to numeric days
                  stringr::str_remove(" days") |> as.numeric()) |>
  dplyr::ungroup()|>

  # add Notes and Action
  dplyr::mutate(Notes = dplyr::case_when( DaysDiff<=7 ~ "Visit within 7 days",
                                          DaysDiff>7 & DaysDiff<=30 ~ "Visit within 7-30 days",
                                          DaysDiff>7 & DaysDiff<=30 ~ "Visit within 7-30 days",
                                          DaysDiff>30 & DaysDiff<=60 ~ "Visit within 30-60 days",
                                          DaysDiff>60 & DaysDiff<=275 ~ "Visit within 30-275 days"),
                # recommend action
                Action = dplyr::case_when(DaysDiff>7 & DaysDiff<=275 ~ "Confirm date visited",
                                          DaysDiff<=7 ~ "Consider grouping date visits"))|>
  # add PlotID information back in to help users trouble shoot
  dplyr::left_join(all_dimas_pks[["tblPlots"]]|> dplyr::select(PrimaryKey, PlotKey, PlotID) |> dplyr::distinct() |> subset(!is.na(PrimaryKey)))

# Flag generic plots and orphaned records for deletion
pk_date_check <- pk_date_check |>
  # Make a note of the issue
  dplyr::mutate(Notes = dplyr::case_when(is.na(PlotKey) ~ "Orphan records",
                                         PlotKey %in% c("123123123", "999999999") ~ "Generic plots",
                                         .default = Notes),
                # recommend action
                Action = dplyr::case_when(is.na(PlotKey) ~ "Delete",
                                          PlotKey %in% c("123123123", "999999999") ~ "Delete",
                                          .default = Action),
                DataOwnerResponse = NA)
# Save files for QC
saveRDS(all_dimas, "QC/all_dimas.Rdata")
saveRDS(all_dimas_pks, "QC/all_dima_pks.Rdata")
write.csv(pk_date_check, "QC/primarykey_date_check.csv", row.names=FALSE)
write.csv(pk_date_check |> subset(!is.na(Action)),
          paste0("QC/primarykey_resolve_", Sys.Date(), ".csv"), row.names=FALSE)
