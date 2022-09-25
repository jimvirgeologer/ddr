library(purrr)
library(tidyverse)
library(readxl)
library(writexl)
library(adagio)
library(dplyr)
library(MASS)
library(visdat)
library(Rmpfr)
library(janitor)
library(data.table)



  setwd("~/Current Work/R_Projects/ddr/DDR_EDA")
  file.list_all <- list.files(pattern = '.xls', recursive = TRUE, full.names = TRUE)
  file.list_all <- file.list_all [!grepl("analysis", file.list_all)]
  file.list_all <- file.list_all [!grepl("Status", file.list_all)]
  file.list_all
  
  df_read <- function(i) {
    x = read_xls(i,sheet = 1, col_types = "text") %>% as.data.frame()
    y <- as.numeric(which(x[,2] %like% "SHIFT"))
    unit <- x[y-1,1]
    x <- row_to_names(x,y[1])
   file <- i[1]
   colnames(x)[1] <- "DATE"
   # names(x)[3] <- 'DATE'
    x <- cbind( file,unit, x)%>% clean_names()

    x
  }
  

  
  
  df <- lapply(file.list_all, df_read) %>% bind_rows %>% as.data.frame()
  

  
  
  
  
  df_01 <- df  %>% transmute( FILE = as.character(file),
                              UNIT = as.character(unit),
                              DATE = as.numeric(date),
                              DATE = as.Date(DATE, origin = "1899-12-30"),
                              SHIFT = as.character(shift),
                              DRILLER = as.character(ifelse(is.na(driller), core_checker, driller)),
                              HOLE_ID = as.character(hole_id),
                              FROM = as.numeric(depth_of_hole_from),
                              TO = as.numeric(depth_of_hole_to),
                              INTERVAL = as.numeric(meters_per_shift),
                              INTERVAL = ifelse(INTERVAL <=0,0,INTERVAL),
                              # INTERVAL = if(INTERVAL <= 0) {0} else {INTERVAL},
                              # TO = if(max(TO) == 0) {FROM} else {max(TO)},
                              RECOVERED = as.numeric(recovered),
                              PERCENTAGE_REC = as.numeric(percentage_of_core_recovery),
                              CORE_REC = as.numeric(core_recovered_in_meters),
                              TOTAL_HOURS = as.numeric(total_hours),
                              TRAVEL_TIME = as.numeric(travel_time),
                              SERVICING = as.numeric(servicing),
                              DRILLING = as.numeric(drilling),
                              BLASTING_HRS = as.numeric(blasting_hours),
                              REAMING = as.numeric(reaming),
                              CASING = as.numeric(casing),
                              MEETINGS = as.numeric(meetings),
                              PULLOUT_PULLDOWN = as.numeric(pull_out_pull_down),
                              WASHOUT_CONDITIONING = as.numeric(wash_out),
                              RIG_MOVING = as.numeric(rig_moving),
                              RIG_SETTING_UP = as.numeric(rig_setting_up),
                              CEMENTING = as.numeric(cementing),
                              PLUGGING = as.numeric(plugging),
                              BIT_CHANGE = as.numeric(bit_change),
                              HOLE_SURVEY = as.numeric(hole_survey),
                              JACK_CASING = as.numeric(jack_casing),
                              CEMENTING = as.numeric(cement),
                              REDRILL = as.numeric(redrill),
                              TESTING = as.numeric(testing),
                              SPT = as.numeric(spt),
                              ORIENTATION = as.numeric(orientation),
                              MIXING_MUD= as.numeric(mixing_mug),
                              CEMENT_STANDBY = as.numeric(cement_standby),
                              STANDBY = as.numeric(standby),
                              BREAKDOWN = as.numeric(breakdown),
                              OTHERS = as.numeric(others),
                              HOUSEKEEPING_MOVEOUT  = as.numeric(housekeeping_moveout),
                              CONTROL_NO = as.numeric(control_no),
                              REMARKS = as.character(remarks),
                              FILTER = paste(DATE,FROM,TO,REMARKS)) 
  
  
  
  df_02 <- df_01 %>%
    filter(FILTER != 'NA 0 NA NA') %>%
    filter(FILTER != 'NA NA NA NA') %>%
    tidyr::fill(DATE,SHIFT,DRILLER,TOTAL_HOURS)%>%
    filter(!is.na(HOLE_ID),!is.na(DATE)) %>%
    mutate(TO = replace_na(TO,0)) %>% 
    arrange(DATE,desc(x = SHIFT))
  
  remove <- with(df_02,(FROM == 0) & (INTERVAL >= 150))
  df_02 <- df_02[!remove,]
  
  df_03 <- df_02 %>% group_by(FILE,UNIT,HOLE_ID,DATE,SHIFT,DRILLER) %>%
    summarize(FROM = min(FROM),
              TO = ifelse(max(TO) == 0, FROM,max(TO)),
              INTERVAL = (TO-FROM),
              RECOVERED = sum(RECOVERED),
              PERCENTAGE_REC = (RECOVERED / INTERVAL),
              CORE_REC = sum(CORE_REC),
              TOTAL_HOURS = sum(TOTAL_HOURS),
              TRAVEL_TIME = sum(TRAVEL_TIME),
              SERVICING = sum(SERVICING),
              DRILLING = sum(DRILLING),
              BLASTING_HRS = sum(BLASTING_HRS),
              REAMING = sum(REAMING),
              CASING = sum(CASING),
              MEETINGS = sum(MEETINGS),
              PULLOUT_PULLDOWN = sum(PULLOUT_PULLDOWN),
              WASHOUT_CONDITIONING = sum(WASHOUT_CONDITIONING),
              RIG_MOVING = sum(RIG_MOVING),
              RIG_SETTING_UP = sum(RIG_SETTING_UP),
              CEMENTING = sum(CEMENTING),
              PLUGGING = sum(PLUGGING),
              BIT_CHANGE = sum (BIT_CHANGE),
              HOLE_SURVEY  = sum(HOLE_SURVEY),
              REDRILL = sum(REDRILL),
              MIXING_MUD = sum(MIXING_MUD),
              STANDBY = sum(STANDBY),
              BREAKDOWN = sum(BREAKDOWN),
              REMARKS = str_c(REMARKS, collapse = "")) %>%
    arrange(HOLE_ID,DATE,FROM)
  
  write_xlsx(df,"D:\\16_Exploration\\10_DDR\\trial_df.xlsx")
  write_xlsx(df_01,"D:\\16_Exploration\\10_DDR\\trial_df_01.xlsx")
  write_xlsx(df_02,"D:\\16_Exploration\\10_DDR\\trial_df_02.xlsx")
  write_xlsx(df_03,"D:\\16_Exploration\\10_DDR\\trial_df_03.xlsx")



