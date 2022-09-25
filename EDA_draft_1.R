library(purrr)
library(tidyverse)
library(readxl)
library(writexl)
library(adagio)
library(dplyr)
library(MASS)
library(visdat)
library(Rmpfr)
library(vroom)
library(janitor)


###########DATA BASE###############

setwd("~/Current Work/R_Projects/ddr/DDR_EDA")
file.list_all <- list.files( pattern = '.xls', recursive = TRUE, full.names = TRUE)
file.list_all <- file.list_all [!grepl("xlsx", file.list_all)]
file.list_all


setwd("~/Current Work/R_Projects/ddr/DDR_EDA")
file.list_2022 <- list.files(path  = './2022', pattern = '.xls', recursive = TRUE, full.names = TRUE)

setwd("~/Current Work/R_Projects/ddr/DDR_EDA")
file.list_2021 <- list.files(path  = './2021', pattern = '.xls', recursive = TRUE, full.names = TRUE)
file.list_2021 <- file.list_2021[!grepl("xlsx", file.list_2021)]
file.list_2021 

setwd("~/Current Work/R_Projects/ddr/DDR_EDA")
file.list_2020 <- list.files(path  = './2020', pattern = '.xls', recursive = TRUE, full.names = TRUE)

DDR_READ<- function(i) {
  x = read_xls(i,sheet = 1)
  
  x[, 72] <- x[3, 1]
  
  colnames(x) <-
    c(
      "c1",
      "c2",
      "c3",
      "c4",
      "c5",
      "c6",
      "c7",
      "c8",
      "c9",
      "c10",
      "c11",
      "c12",
      "c13",
      "c14",
      "c15",
      "c16",
      "c17",
      "c18",
      "c19",
      "c20",
      "c21",
      "c22",
      "c23",
      "c24",
      "c25",
      "c26",
      "c27",
      "c28",
      "c29",
      "c30",
      "c31",
      "c32",
      "c33",
      "c34",
      "c35",
      "c36",
      "c37",
      "c38",
      "c39",
      "c40",
      "c41",
      "c42",
      "c43",
      "c44",
      "c45",
      "c46",
      "c47",
      "c48",
      "c49",
      "c50",
      "c51",
      "c52",
      "c53",
      "c54",
      "c55",
      "c56",
      "c57",
      "c58",
      "c59",
      "c60",
      "c61",
      "c62",
      "c63",
      "c64",
      "c65",
      "c66",
      "c67",
      "c68",
      "c69",
      "c70",
      "c71",
      "c72"
    )

  x <- x %>% transmute(
    UNIT = as.character(c72),
    DATE = as.numeric(c1),
    DATE = as.Date(DATE, origin = "1899-12-30"),
    SHIFT = as.character(c2),
  DRILLER = as.character(c3),
    HOLE_ID = as.character(c5),
    FROM = as.numeric(c6),
    TO = as.numeric(c7),
    INTERVAL = as.numeric(c8),
    RECOVERED = as.numeric(c9),
    PERCENTAGE_REC = as.numeric(c10),
    CORE_REC = as.numeric(c11),
    TOTAL_LENGTH = as.numeric(c13),
    RQD = as.numeric(c14),
    TOTAL_HOURS = as.numeric(c15),
    HQ_0_to_100 = as.numeric(c21),
    HQ_100_to_200 = as.numeric(c22),
    HQ_200_to_300 = as.numeric(c23),
    HQ_300_to_400 = as.numeric(c24),
    HQ_400_to_500 = as.numeric(c25),
    NQ_0_to_100 = as.numeric(c28),
    NQ_100_to_200 = as.numeric(c29),
    NQ_200_to_300 = as.numeric(c30),
    NQ_300_to_400 = as.numeric(c31),
    NQ_400_to_500 = as.numeric(c32),
    TRAVEL_TIME = as.numeric(c41),
    SERVICING = as.numeric(c42),
    DRILLING = as.numeric(c43),
    BLASTING_HRS = as.numeric(c44),
    REAMING = as.numeric(c45),
    CASING = as.numeric(c46),
    MEETINGS = as.numeric(c47),
    PULLOUT_PULLDOWN = as.numeric(c48),
    WASHOUT_CONDITIONING = as.numeric(c49),
    RIG_MOVING = as.numeric(c50),
    RIG_SETTING_UP = as.numeric(c51),
    CEMENTING = as.numeric(c52),
    PLUGGING =as.numeric(c53),
    BIT_CHANGE = as.numeric(c54),
    HOLE_SURVEY = as.numeric(c55),
    JACK_CASING = as.numeric(c56),
    CEMENTING = as.numeric(c57),
    REDRILL = as.numeric(c58),
    TESTING = as.numeric(c59),
    SPT = as.numeric(c60),
    ORIENTATION = as.numeric(c61),
    JACK_CASING = as.numeric(c62),
    MIXING_MUD= as.numeric(c63),
    CEMENT_STANDBY = as.numeric(c64),
    STANDBY = as.numeric(c65),
    BREAKDOWN = as.numeric(c66),
    OTHERS = as.numeric(c67),
    HOUSEKEEPING_MOVEOUT  =as.numeric(c68),
    CONTROL_NO = as.numeric(c69),
    REMARKS = as.character(c71)
  )





  x <- x %>%
    tidyr::fill(DATE,SHIFT,DRILLER,TOTAL_HOURS)%>%
    filter(!is.na(HOLE_ID),!is.na(DATE)) %>% 
    mutate(TO = replace_na(TO,0))

 
 HOLE_DURATION = as.numeric(max(x$DATE)) - as.numeric(min(x$DATE)) 
  TOTAL_LENGTH = max(x$TO)
 RATE = TOTAL_LENGTH / HOLE_DURATION
 x[,53]<-  HOLE_DURATION
 x[,54]<-  as.numeric(TOTAL_LENGTH)
 x[,55]<-   as.numeric(RATE)
  
  x
}
DDR_READ_2021<- function(i) {
  x = read_xls(i,sheet = 1)
  
  x[, 70] <- x[3, 1]
  
  colnames(x) <-
    c(
      "c1",
      "c2",
      "c3",
      "c4",
      "c5",
      "c6",
      "c7",
      "c8",
      "c9",
      "c10",
      "c11",
      "c12",
      "c13",
      "c14",
      "c15",
      "c16",
      "c17",
      "c18",
      "c19",
      "c20",
      "c21",
      "c22",
      "c23",
      "c24",
      "c25",
      "c26",
      "c27",
      "c28",
      "c29",
      "c30",
      "c31",
      "c32",
      "c33",
      "c34",
      "c35",
      "c36",
      "c37",
      "c38",
      "c39",
      "c40",
      "c41",
      "c42",
      "c43",
      "c44",
      "c45",
      "c46",
      "c47",
      "c48",
      "c49",
      "c50",
      "c51",
      "c52",
      "c53",
      "c54",
      "c55",
      "c56",
      "c57",
      "c58",
      "c59",
      "c60",
      "c61",
      "c62",
      "c63",
      "c64",
      "c65",
      "c66",
      "c67",
      "c68",
      "c69",
      "c70"
    )
  
  x <- x %>% transmute(
    UNIT = as.character(c70),
    DATE = as.numeric(c1),
    DATE = as.Date(DATE, origin = "1899-12-30"),
    SHIFT = as.character(c2),
    DRILLER = as.character(c3),
    HOLE_ID = as.character(c5),
    FROM = as.numeric(c6),
    TO = as.numeric(c7),
    INTERVAL = as.numeric(c8),
    RECOVERED = as.numeric(c9),
    PERCENTAGE_REC = as.numeric(c10),
    CORE_REC = as.numeric(c11),
    RQD = as.numeric(c14),
    TOTAL_HOURS = as.numeric(c13),
    HQ_0_to_100 = as.numeric(c19),
    HQ_100_to_200 = as.numeric(c20),
    HQ_200_to_300 = as.numeric(c21),
    HQ_300_to_400 = as.numeric(c22),
    HQ_400_to_500 = as.numeric(c23),
    NQ_0_to_100 = as.numeric(c26),
    NQ_100_to_200 = as.numeric(c27),
    NQ_200_to_300 = as.numeric(c28),
    NQ_300_to_400 = as.numeric(c29),
    NQ_400_to_500 = as.numeric(c30),
    TRAVEL_TIME = as.numeric(c39),
    SERVICING = as.numeric(c40),
    DRILLING = as.numeric(c41),
    BLASTING_HRS = as.numeric(c42),
    REAMING = as.numeric(c43),
    CASING = as.numeric(c44),
    MEETINGS = as.numeric(c45),
    PULLOUT_PULLDOWN = as.numeric(c46),
    WASHOUT_CONDITIONING = as.numeric(c47),
    RIG_MOVING = as.numeric(c48),
    RIG_SETTING_UP = as.numeric(c49),
    CEMENTING = as.numeric(c50),
    PLUGGING = as.numeric(c51),
    BIT_CHANGE = as.numeric(c52),
    HOLE_SURVEY = as.numeric(c53),
    JACK_CASING = as.numeric(c54),
    CEMENTING = as.numeric(c55),
    REDRILL = as.numeric(c56),
    TESTING = as.numeric(c57),
    SPT = as.numeric(c58),
    ORIENTATION = as.numeric(c59),
    JACK_CASING = as.numeric(c60),
    MIXING_MUD= as.numeric(c61),
    CEMENT_STANDBY = as.numeric(c62),
    STANDBY = as.numeric(c63),
    BREAKDOWN = as.numeric(c64),
    OTHERS = as.numeric(c65),
    HOUSEKEEPING_MOVEOUT  = as.numeric(c66),
    CONTROL_NO = as.numeric(c67),
    REMARKS = as.character(c69)
  )
  
  
  
  
  
  # x <- x %>%
  #   tidyr::fill(DATE,SHIFT,DRILLER,TOTAL_HOURS)%>%
  #   filter(!is.na(HOLE_ID),!is.na(DATE)) %>% 
  #   mutate(TO = replace_na(TO,0))
  # 
  # 
  # HOLE_DURATION = as.numeric(max(x$DATE)) - as.numeric(min(x$DATE)) 
  # TOTAL_LENGTH = max(x$TO)
  # RATE = TOTAL_LENGTH / HOLE_DURATION
  # x[,51]<-  HOLE_DURATION
  # x[,52]<-  as.numeric(TOTAL_LENGTH)
  # x[,53]<-   as.numeric(RATE)
  # 
  x
}

DDR_NCOL_1<- function(i) {
  x = read_xls(i,sheet = 1)
  y <- ncol(x)
  z <- data.frame(ncols = y)
  z }



DDR_NCOL<- function(i) {
  x = read_xls(i,sheet = 1)
  y <- ncol(x)

  y2 == 0
  if (y == 70) {
    y2 == 1
  }

else { y2 = 0}
  
  z <- data.frame(ncoles = y2)
  z
  }

######## Pawer ########
if (y == 69) {
  x = read_xls(i,sheet = 1)
  
  x[, 70] <- x[3, 1]
  
  colnames(x) <-
    c(
      "c1",
      "c2",
      "c3",
      "c4",
      "c5",
      "c6",
      "c7",
      "c8",
      "c9",
      "c10",
      "c11",
      "c12",
      "c13",
      "c14",
      "c15",
      "c16",
      "c17",
      "c18",
      "c19",
      "c20",
      "c21",
      "c22",
      "c23",
      "c24",
      "c25",
      "c26",
      "c27",
      "c28",
      "c29",
      "c30",
      "c31",
      "c32",
      "c33",
      "c34",
      "c35",
      "c36",
      "c37",
      "c38",
      "c39",
      "c40",
      "c41",
      "c42",
      "c43",
      "c44",
      "c45",
      "c46",
      "c47",
      "c48",
      "c49",
      "c50",
      "c51",
      "c52",
      "c53",
      "c54",
      "c55",
      "c56",
      "c57",
      "c58",
      "c59",
      "c60",
      "c61",
      "c62",
      "c63",
      "c64",
      "c65",
      "c66",
      "c67",
      "c68",
      "c69",
      "c70"
    )
  
  x <- x %>% transmute(
    UNIT = as.character(c70),
    DATE = as.numeric(c1),
    DATE = as.Date(DATE, origin = "1899-12-30"),
    SHIFT = as.character(c2),
    DRILLER = as.character(c3),
    HOLE_ID = as.character(c5),
    FROM = as.numeric(c6),
    TO = as.numeric(c7),
    INTERVAL = as.numeric(c8),
    RECOVERED = as.numeric(c9),
    PERCENTAGE_REC = as.numeric(c10),
    CORE_REC = as.numeric(c11),
    RQD = as.numeric(c14),
    TOTAL_HOURS = as.numeric(c13),
    HQ_0_to_100 = as.numeric(c19),
    HQ_100_to_200 = as.numeric(c20),
    HQ_200_to_300 = as.numeric(c21),
    HQ_300_to_400 = as.numeric(c22),
    HQ_400_to_500 = as.numeric(c23),
    NQ_0_to_100 = as.numeric(c26),
    NQ_100_to_200 = as.numeric(c27),
    NQ_200_to_300 = as.numeric(c28),
    NQ_300_to_400 = as.numeric(c29),
    NQ_400_to_500 = as.numeric(c30),
    TRAVEL_TIME = as.numeric(c39),
    SERVICING = as.numeric(c40),
    DRILLING = as.numeric(c41),
    BLASTING_HRS = as.numeric(c42),
    REAMING = as.numeric(c43),
    CASING = as.numeric(c44),
    MEETINGS = as.numeric(c45),
    PULLOUT_PULLDOWN = as.numeric(c46),
    WASHOUT_CONDITIONING = as.numeric(c47),
    RIG_MOVING = as.numeric(c48),
    RIG_SETTING_UP = as.numeric(c49),
    CEMENTING = as.numeric(c50),
    PLUGGING = as.numeric(c51),
    BIT_CHANGE = as.numeric(c52),
    HOLE_SURVEY = as.numeric(c53),
    JACK_CASING = as.numeric(c54),
    CEMENTING = as.numeric(c55),
    REDRILL = as.numeric(c56),
    TESTING = as.numeric(c57),
    SPT = as.numeric(c58),
    ORIENTATION = as.numeric(c59),
    JACK_CASING = as.numeric(c60),
    MIXING_MUD= as.numeric(c61),
    CEMENT_STANDBY = as.numeric(c62),
    STANDBY = as.numeric(c63),
    BREAKDOWN = as.numeric(c64),
    OTHERS = as.numeric(c65),
    HOUSEKEEPING_MOVEOUT  = as.numeric(c66),
    CONTROL_NO = as.numeric(c67),
    REMARKS = as.character(c69)
  )
  
  
  
  
  x <- x %>%
    tidyr::fill(DATE,SHIFT,DRILLER,TOTAL_HOURS)%>%
    filter(!is.na(HOLE_ID),!is.na(DATE)) %>%
    mutate(TO = replace_na(TO,0))
  
  
  HOLE_DURATION = as.numeric(max(x$DATE)) - as.numeric(min(x$DATE))
  TOTAL_LENGTH = max(x$TO)
  RATE = TOTAL_LENGTH / HOLE_DURATION
  x[,51]<-  HOLE_DURATION
  x[,52]<-  as.numeric(TOTAL_LENGTH)
  x[,53]<-   as.numeric(RATE)
  
  x
}

if (y == 71) {
  x = read_xls(i,sheet = 1)
  
  x[, 72] <- x[3, 1]
  
  colnames(x) <-
    c(
      "c1",
      "c2",
      "c3",
      "c4",
      "c5",
      "c6",
      "c7",
      "c8",
      "c9",
      "c10",
      "c11",
      "c12",
      "c13",
      "c14",
      "c15",
      "c16",
      "c17",
      "c18",
      "c19",
      "c20",
      "c21",
      "c22",
      "c23",
      "c24",
      "c25",
      "c26",
      "c27",
      "c28",
      "c29",
      "c30",
      "c31",
      "c32",
      "c33",
      "c34",
      "c35",
      "c36",
      "c37",
      "c38",
      "c39",
      "c40",
      "c41",
      "c42",
      "c43",
      "c44",
      "c45",
      "c46",
      "c47",
      "c48",
      "c49",
      "c50",
      "c51",
      "c52",
      "c53",
      "c54",
      "c55",
      "c56",
      "c57",
      "c58",
      "c59",
      "c60",
      "c61",
      "c62",
      "c63",
      "c64",
      "c65",
      "c66",
      "c67",
      "c68",
      "c69",
      "c70",
      "c71",
      "c72"
    )
  
  x <- x %>% transmute(
    UNIT = as.character(c72),
    DATE = as.numeric(c1),
    DATE = as.Date(DATE, origin = "1899-12-30"),
    SHIFT = as.character(c2),
    DRILLER = as.character(c3),
    HOLE_ID = as.character(c5),
    FROM = as.numeric(c6),
    TO = as.numeric(c7),
    INTERVAL = as.numeric(c8),
    RECOVERED = as.numeric(c9),
    PERCENTAGE_REC = as.numeric(c10),
    CORE_REC = as.numeric(c11),
    TOTAL_LENGTH = as.numeric(c13),
    RQD = as.numeric(c14),
    TOTAL_HOURS = as.numeric(c15),
    HQ_0_to_100 = as.numeric(c21),
    HQ_100_to_200 = as.numeric(c22),
    HQ_200_to_300 = as.numeric(c23),
    HQ_300_to_400 = as.numeric(c24),
    HQ_400_to_500 = as.numeric(c25),
    NQ_0_to_100 = as.numeric(c28),
    NQ_100_to_200 = as.numeric(c29),
    NQ_200_to_300 = as.numeric(c30),
    NQ_300_to_400 = as.numeric(c31),
    NQ_400_to_500 = as.numeric(c32),
    TRAVEL_TIME = as.numeric(c41),
    SERVICING = as.numeric(c42),
    DRILLING = as.numeric(c43),
    BLASTING_HRS = as.numeric(c44),
    REAMING = as.numeric(c45),
    CASING = as.numeric(c46),
    MEETINGS = as.numeric(c47),
    PULLOUT_PULLDOWN = as.numeric(c48),
    WASHOUT_CONDITIONING = as.numeric(c49),
    RIG_MOVING = as.numeric(c50),
    RIG_SETTING_UP = as.numeric(c51),
    CEMENTING = as.numeric(c52),
    PLUGGING =as.numeric(c53),
    BIT_CHANGE = as.numeric(c54),
    HOLE_SURVEY = as.numeric(c55),
    JACK_CASING = as.numeric(c56),
    CEMENTING = as.numeric(c57),
    REDRILL = as.numeric(c58),
    TESTING = as.numeric(c59),
    SPT = as.numeric(c60),
    ORIENTATION = as.numeric(c61),
    JACK_CASING = as.numeric(c62),
    MIXING_MUD= as.numeric(c63),
    CEMENT_STANDBY = as.numeric(c64),
    STANDBY = as.numeric(c65),
    BREAKDOWN = as.numeric(c66),
    OTHERS = as.numeric(c67),
    HOUSEKEEPING_MOVEOUT  =as.numeric(c68),
    CONTROL_NO = as.numeric(c69),
    REMARKS = as.character(c71)
  )
  
  
  
  
  
  x <- x %>%
    tidyr::fill(DATE,SHIFT,DRILLER,TOTAL_HOURS)%>%
    filter(!is.na(HOLE_ID),!is.na(DATE)) %>% 
    mutate(TO = replace_na(TO,0))
  
  
  HOLE_DURATION = as.numeric(max(x$DATE)) - as.numeric(min(x$DATE)) 
  TOTAL_LENGTH = max(x$TO)
  RATE = TOTAL_LENGTH / HOLE_DURATION
  x[,53]<-  HOLE_DURATION
  x[,54]<-  as.numeric(TOTAL_LENGTH)
  x[,55]<-   as.numeric(RATE)
  
  x
}
if (y == 72) {
  x = read_xls(i,sheet = 1)
  
  x[, 73] <- x[3, 1]
  
  colnames(x) <-
    c(
      "c1",
      "c2",
      "c3",
      "c4",
      "c5",
      "c6",
      "c7",
      "c8",
      "c9",
      "c10",
      "c11",
      "c12",
      "c13",
      "c14",
      "c15",
      "c16",
      "c17",
      "c18",
      "c19",
      "c20",
      "c21",
      "c22",
      "c23",
      "c24",
      "c25",
      "c26",
      "c27",
      "c28",
      "c29",
      "c30",
      "c31",
      "c32",
      "c33",
      "c34",
      "c35",
      "c36",
      "c37",
      "c38",
      "c39",
      "c40",
      "c41",
      "c42",
      "c43",
      "c44",
      "c45",
      "c46",
      "c47",
      "c48",
      "c49",
      "c50",
      "c51",
      "c52",
      "c53",
      "c54",
      "c55",
      "c56",
      "c57",
      "c58",
      "c59",
      "c60",
      "c61",
      "c62",
      "c63",
      "c64",
      "c65",
      "c66",
      "c67",
      "c68",
      "c69",
      "c70",
      "c71",
      "c72",
      "c73"
    )
  
  x <- x %>% transmute(
    UNIT = as.character(c72),
    DATE = as.numeric(c1),
    DATE = as.Date(DATE, origin = "1899-12-30"),
    SHIFT = as.character(c2),
    DRILLER = as.character(c3),
    HOLE_ID = as.character(c5),
    FROM = as.numeric(c6),
    TO = as.numeric(c7),
    INTERVAL = as.numeric(c8),
    RECOVERED = as.numeric(c10),
    PERCENTAGE_REC = as.numeric(c11),
    CORE_REC = as.numeric(c12),
    TOTAL_LENGTH = as.numeric(c14),
    RQD = as.numeric(c15),
    TOTAL_HOURS = as.numeric(c16),
    HQ_0_to_100 = as.numeric(c22),
    HQ_100_to_200 = as.numeric(c23),
    HQ_200_to_300 = as.numeric(c24),
    HQ_300_to_400 = as.numeric(c25),
    HQ_400_to_500 = as.numeric(c26),
    NQ_0_to_100 = as.numeric(c29),
    NQ_100_to_200 = as.numeric(c30),
    NQ_200_to_300 = as.numeric(c31),
    NQ_300_to_400 = as.numeric(c32),
    NQ_400_to_500 = as.numeric(c33),
    TRAVEL_TIME = as.numeric(c42),
    SERVICING = as.numeric(c43),
    DRILLING = as.numeric(c44),
    BLASTING_HRS = as.numeric(c45),
    REAMING = as.numeric(c46),
    CASING = as.numeric(c47),
    MEETINGS = as.numeric(c48),
    PULLOUT_PULLDOWN = as.numeric(c49),
    WASHOUT_CONDITIONING = as.numeric(c50),
    RIG_MOVING = as.numeric(c51),
    RIG_SETTING_UP = as.numeric(c52),
    CEMENTING = as.numeric(c53),
    PLUGGING =as.numeric(c54),
    BIT_CHANGE = as.numeric(c55),
    HOLE_SURVEY = as.numeric(c56),
    JACK_CASING = as.numeric(c57),
    CEMENTING = as.numeric(c58),
    REDRILL = as.numeric(c59),
    TESTING = as.numeric(c60),
    SPT = as.numeric(c61),
    ORIENTATION = as.numeric(c62),
    JACK_CASING = as.numeric(c63),
    MIXING_MUD= as.numeric(c64),
    CEMENT_STANDBY = as.numeric(c65),
    STANDBY = as.numeric(c66),
    BREAKDOWN = as.numeric(c67),
    OTHERS = as.numeric(c68),
    HOUSEKEEPING_MOVEOUT  =as.numeric(c69),
    CONTROL_NO = as.numeric(c70),
    REMARKS = as.character(c72)
  )
  
  
  
  
  
  x <- x %>%
    tidyr::fill(DATE,SHIFT,DRILLER,TOTAL_HOURS)%>%
    filter(!is.na(HOLE_ID),!is.na(DATE)) %>% 
    mutate(TO = replace_na(TO,0))
  
  
  HOLE_DURATION = as.numeric(max(x$DATE)) - as.numeric(min(x$DATE)) 
  TOTAL_LENGTH = max(x$TO)
  RATE = TOTAL_LENGTH / HOLE_DURATION
  x[,53]<-  HOLE_DURATION
  x[,54]<-  as.numeric(TOTAL_LENGTH)
  x[,55]<-   as.numeric(RATE)
  
  x
}

if (y == 100) {
  x = read_xls(i,sheet = 1)
  
  x[, 72] <- x[3, 1]
  
  colnames(x) <-
    c(
      "c1",
      "c2",
      "c3",
      "c4",
      "c5",
      "c6",
      "c7",
      "c8",
      "c9",
      "c10",
      "c11",
      "c12",
      "c13",
      "c14",
      "c15",
      "c16",
      "c17",
      "c18",
      "c19",
      "c20",
      "c21",
      "c22",
      "c23",
      "c24",
      "c25",
      "c26",
      "c27",
      "c28",
      "c29",
      "c30",
      "c31",
      "c32",
      "c33",
      "c34",
      "c35",
      "c36",
      "c37",
      "c38",
      "c39",
      "c40",
      "c41",
      "c42",
      "c43",
      "c44",
      "c45",
      "c46",
      "c47",
      "c48",
      "c49",
      "c50",
      "c51",
      "c52",
      "c53",
      "c54",
      "c55",
      "c56",
      "c57",
      "c58",
      "c59",
      "c60",
      "c61",
      "c62",
      "c63",
      "c64",
      "c65",
      "c66",
      "c67",
      "c68",
      "c69",
      "c70",
      "c71",
      "c72"
    )
  
  x <- x %>% transmute(
    UNIT = as.character(c72),
    DATE = as.numeric(c1),
    DATE = as.Date(DATE, origin = "1899-12-30"),
    SHIFT = as.character(c2),
    DRILLER = as.character(c3),
    HOLE_ID = as.character(c5),
    FROM = as.numeric(c6),
    TO = as.numeric(c7),
    INTERVAL = as.numeric(c8),
    RECOVERED = as.numeric(c9),
    PERCENTAGE_REC = as.numeric(c10),
    CORE_REC = as.numeric(c11),
    TOTAL_LENGTH = as.numeric(c13),
    RQD = as.numeric(c14),
    TOTAL_HOURS = as.numeric(c15),
    HQ_0_to_100 = as.numeric(c21),
    HQ_100_to_200 = as.numeric(c22),
    HQ_200_to_300 = as.numeric(c23),
    HQ_300_to_400 = as.numeric(c24),
    HQ_400_to_500 = as.numeric(c25),
    NQ_0_to_100 = as.numeric(c28),
    NQ_100_to_200 = as.numeric(c29),
    NQ_200_to_300 = as.numeric(c30),
    NQ_300_to_400 = as.numeric(c31),
    NQ_400_to_500 = as.numeric(c32),
    TRAVEL_TIME = as.numeric(c41),
    SERVICING = as.numeric(c42),
    DRILLING = as.numeric(c43),
    BLASTING_HRS = as.numeric(c44),
    REAMING = as.numeric(c45),
    CASING = as.numeric(c46),
    MEETINGS = as.numeric(c47),
    PULLOUT_PULLDOWN = as.numeric(c48),
    WASHOUT_CONDITIONING = as.numeric(c49),
    RIG_MOVING = as.numeric(c50),
    RIG_SETTING_UP = as.numeric(c51),
    CEMENTING = as.numeric(c52),
    PLUGGING =as.numeric(c53),
    BIT_CHANGE = as.numeric(c54),
    HOLE_SURVEY = as.numeric(c55),
    JACK_CASING = as.numeric(c56),
    CEMENTING = as.numeric(c57),
    REDRILL = as.numeric(c58),
    TESTING = as.numeric(c59),
    SPT = as.numeric(c60),
    ORIENTATION = as.numeric(c61),
    JACK_CASING = as.numeric(c62),
    MIXING_MUD= as.numeric(c63),
    CEMENT_STANDBY = as.numeric(c64),
    STANDBY = as.numeric(c65),
    BREAKDOWN = as.numeric(c66),
    OTHERS = as.numeric(c67),
    HOUSEKEEPING_MOVEOUT  =as.numeric(c68),
    CONTROL_NO = as.numeric(c69),
    REMARKS = as.character(c71)
  )
  
  
  
  
  
  x <- x %>%
    tidyr::fill(DATE,SHIFT,DRILLER,TOTAL_HOURS)%>%
    filter(!is.na(HOLE_ID),!is.na(DATE)) %>% 
    mutate(TO = replace_na(TO,0))
  
  
  HOLE_DURATION = as.numeric(max(x$DATE)) - as.numeric(min(x$DATE)) 
  TOTAL_LENGTH = max(x$TO)
  RATE = TOTAL_LENGTH / HOLE_DURATION
  x[,53]<-  HOLE_DURATION
  x[,54]<-  as.numeric(TOTAL_LENGTH)
  x[,55]<-   as.numeric(RATE)
  
  x
}
if (y == 100) {
  x = read_xls(i,sheet = 1)
  
  x[, 72] <- x[3, 1]
  
  colnames(x) <-
    c(
      "c1",
      "c2",
      "c3",
      "c4",
      "c5",
      "c6",
      "c7",
      "c8",
      "c9",
      "c10",
      "c11",
      "c12",
      "c13",
      "c14",
      "c15",
      "c16",
      "c17",
      "c18",
      "c19",
      "c20",
      "c21",
      "c22",
      "c23",
      "c24",
      "c25",
      "c26",
      "c27",
      "c28",
      "c29",
      "c30",
      "c31",
      "c32",
      "c33",
      "c34",
      "c35",
      "c36",
      "c37",
      "c38",
      "c39",
      "c40",
      "c41",
      "c42",
      "c43",
      "c44",
      "c45",
      "c46",
      "c47",
      "c48",
      "c49",
      "c50",
      "c51",
      "c52",
      "c53",
      "c54",
      "c55",
      "c56",
      "c57",
      "c58",
      "c59",
      "c60",
      "c61",
      "c62",
      "c63",
      "c64",
      "c65",
      "c66",
      "c67",
      "c68",
      "c69",
      "c70",
      "c71",
      "c72"
    )
  
  x <- x %>% transmute(
    UNIT = as.character(c72),
    DATE = as.numeric(c1),
    DATE = as.Date(DATE, origin = "1899-12-30"),
    SHIFT = as.character(c2),
    DRILLER = as.character(c3),
    HOLE_ID = as.character(c5),
    FROM = as.numeric(c6),
    TO = as.numeric(c7),
    INTERVAL = as.numeric(c8),
    RECOVERED = as.numeric(c9),
    PERCENTAGE_REC = as.numeric(c10),
    CORE_REC = as.numeric(c11),
    TOTAL_LENGTH = as.numeric(c13),
    RQD = as.numeric(c14),
    TOTAL_HOURS = as.numeric(c15),
    HQ_0_to_100 = as.numeric(c21),
    HQ_100_to_200 = as.numeric(c22),
    HQ_200_to_300 = as.numeric(c23),
    HQ_300_to_400 = as.numeric(c24),
    HQ_400_to_500 = as.numeric(c25),
    NQ_0_to_100 = as.numeric(c28),
    NQ_100_to_200 = as.numeric(c29),
    NQ_200_to_300 = as.numeric(c30),
    NQ_300_to_400 = as.numeric(c31),
    NQ_400_to_500 = as.numeric(c32),
    TRAVEL_TIME = as.numeric(c41),
    SERVICING = as.numeric(c42),
    DRILLING = as.numeric(c43),
    BLASTING_HRS = as.numeric(c44),
    REAMING = as.numeric(c45),
    CASING = as.numeric(c46),
    MEETINGS = as.numeric(c47),
    PULLOUT_PULLDOWN = as.numeric(c48),
    WASHOUT_CONDITIONING = as.numeric(c49),
    RIG_MOVING = as.numeric(c50),
    RIG_SETTING_UP = as.numeric(c51),
    CEMENTING = as.numeric(c52),
    PLUGGING =as.numeric(c53),
    BIT_CHANGE = as.numeric(c54),
    HOLE_SURVEY = as.numeric(c55),
    JACK_CASING = as.numeric(c56),
    CEMENTING = as.numeric(c57),
    REDRILL = as.numeric(c58),
    TESTING = as.numeric(c59),
    SPT = as.numeric(c60),
    ORIENTATION = as.numeric(c61),
    JACK_CASING = as.numeric(c62),
    MIXING_MUD= as.numeric(c63),
    CEMENT_STANDBY = as.numeric(c64),
    STANDBY = as.numeric(c65),
    BREAKDOWN = as.numeric(c66),
    OTHERS = as.numeric(c67),
    HOUSEKEEPING_MOVEOUT  =as.numeric(c68),
    CONTROL_NO = as.numeric(c69),
    REMARKS = as.character(c71)
  )
  
  
  
  
  
  x <- x %>%
    tidyr::fill(DATE,SHIFT,DRILLER,TOTAL_HOURS)%>%
    filter(!is.na(HOLE_ID),!is.na(DATE)) %>% 
    mutate(TO = replace_na(TO,0))
  
  
  HOLE_DURATION = as.numeric(max(x$DATE)) - as.numeric(min(x$DATE)) 
  TOTAL_LENGTH = max(x$TO)
  RATE = TOTAL_LENGTH / HOLE_DURATION
  x[,53]<-  HOLE_DURATION
  x[,54]<-  as.numeric(TOTAL_LENGTH)
  x[,55]<-   as.numeric(RATE)
  
  x
}
if (y == 100) {
  x = read_xls(i,sheet = 1)
  
  x[, 72] <- x[3, 1]
  
  colnames(x) <-
    c(
      "c1",
      "c2",
      "c3",
      "c4",
      "c5",
      "c6",
      "c7",
      "c8",
      "c9",
      "c10",
      "c11",
      "c12",
      "c13",
      "c14",
      "c15",
      "c16",
      "c17",
      "c18",
      "c19",
      "c20",
      "c21",
      "c22",
      "c23",
      "c24",
      "c25",
      "c26",
      "c27",
      "c28",
      "c29",
      "c30",
      "c31",
      "c32",
      "c33",
      "c34",
      "c35",
      "c36",
      "c37",
      "c38",
      "c39",
      "c40",
      "c41",
      "c42",
      "c43",
      "c44",
      "c45",
      "c46",
      "c47",
      "c48",
      "c49",
      "c50",
      "c51",
      "c52",
      "c53",
      "c54",
      "c55",
      "c56",
      "c57",
      "c58",
      "c59",
      "c60",
      "c61",
      "c62",
      "c63",
      "c64",
      "c65",
      "c66",
      "c67",
      "c68",
      "c69",
      "c70",
      "c71",
      "c72"
    )
  
  x <- x %>% transmute(
    UNIT = as.character(c72),
    DATE = as.numeric(c1),
    DATE = as.Date(DATE, origin = "1899-12-30"),
    SHIFT = as.character(c2),
    DRILLER = as.character(c3),
    HOLE_ID = as.character(c5),
    FROM = as.numeric(c6),
    TO = as.numeric(c7),
    INTERVAL = as.numeric(c8),
    RECOVERED = as.numeric(c9),
    PERCENTAGE_REC = as.numeric(c10),
    CORE_REC = as.numeric(c11),
    TOTAL_LENGTH = as.numeric(c13),
    RQD = as.numeric(c14),
    TOTAL_HOURS = as.numeric(c15),
    HQ_0_to_100 = as.numeric(c21),
    HQ_100_to_200 = as.numeric(c22),
    HQ_200_to_300 = as.numeric(c23),
    HQ_300_to_400 = as.numeric(c24),
    HQ_400_to_500 = as.numeric(c25),
    NQ_0_to_100 = as.numeric(c28),
    NQ_100_to_200 = as.numeric(c29),
    NQ_200_to_300 = as.numeric(c30),
    NQ_300_to_400 = as.numeric(c31),
    NQ_400_to_500 = as.numeric(c32),
    TRAVEL_TIME = as.numeric(c41),
    SERVICING = as.numeric(c42),
    DRILLING = as.numeric(c43),
    BLASTING_HRS = as.numeric(c44),
    REAMING = as.numeric(c45),
    CASING = as.numeric(c46),
    MEETINGS = as.numeric(c47),
    PULLOUT_PULLDOWN = as.numeric(c48),
    WASHOUT_CONDITIONING = as.numeric(c49),
    RIG_MOVING = as.numeric(c50),
    RIG_SETTING_UP = as.numeric(c51),
    CEMENTING = as.numeric(c52),
    PLUGGING =as.numeric(c53),
    BIT_CHANGE = as.numeric(c54),
    HOLE_SURVEY = as.numeric(c55),
    JACK_CASING = as.numeric(c56),
    CEMENTING = as.numeric(c57),
    REDRILL = as.numeric(c58),
    TESTING = as.numeric(c59),
    SPT = as.numeric(c60),
    ORIENTATION = as.numeric(c61),
    JACK_CASING = as.numeric(c62),
    MIXING_MUD= as.numeric(c63),
    CEMENT_STANDBY = as.numeric(c64),
    STANDBY = as.numeric(c65),
    BREAKDOWN = as.numeric(c66),
    OTHERS = as.numeric(c67),
    HOUSEKEEPING_MOVEOUT  =as.numeric(c68),
    CONTROL_NO = as.numeric(c69),
    REMARKS = as.character(c71)
  )
  
  
  
  
  
  x <- x %>%
    tidyr::fill(DATE,SHIFT,DRILLER,TOTAL_HOURS)%>%
    filter(!is.na(HOLE_ID),!is.na(DATE)) %>% 
    mutate(TO = replace_na(TO,0))
  
  
  HOLE_DURATION = as.numeric(max(x$DATE)) - as.numeric(min(x$DATE)) 
  TOTAL_LENGTH = max(x$TO)
  RATE = TOTAL_LENGTH / HOLE_DURATION
  x[,53]<-  HOLE_DURATION
  x[,54]<-  as.numeric(TOTAL_LENGTH)
  x[,55]<-   as.numeric(RATE)
  
  x
}
if (y == 100) {
  x = read_xls(i,sheet = 1)
  
  x[, 72] <- x[3, 1]
  
  colnames(x) <-
    c(
      "c1",
      "c2",
      "c3",
      "c4",
      "c5",
      "c6",
      "c7",
      "c8",
      "c9",
      "c10",
      "c11",
      "c12",
      "c13",
      "c14",
      "c15",
      "c16",
      "c17",
      "c18",
      "c19",
      "c20",
      "c21",
      "c22",
      "c23",
      "c24",
      "c25",
      "c26",
      "c27",
      "c28",
      "c29",
      "c30",
      "c31",
      "c32",
      "c33",
      "c34",
      "c35",
      "c36",
      "c37",
      "c38",
      "c39",
      "c40",
      "c41",
      "c42",
      "c43",
      "c44",
      "c45",
      "c46",
      "c47",
      "c48",
      "c49",
      "c50",
      "c51",
      "c52",
      "c53",
      "c54",
      "c55",
      "c56",
      "c57",
      "c58",
      "c59",
      "c60",
      "c61",
      "c62",
      "c63",
      "c64",
      "c65",
      "c66",
      "c67",
      "c68",
      "c69",
      "c70",
      "c71",
      "c72"
    )
  
  x <- x %>% transmute(
    UNIT = as.character(c72),
    DATE = as.numeric(c1),
    DATE = as.Date(DATE, origin = "1899-12-30"),
    SHIFT = as.character(c2),
    DRILLER = as.character(c3),
    HOLE_ID = as.character(c5),
    FROM = as.numeric(c6),
    TO = as.numeric(c7),
    INTERVAL = as.numeric(c8),
    RECOVERED = as.numeric(c9),
    PERCENTAGE_REC = as.numeric(c10),
    CORE_REC = as.numeric(c11),
    TOTAL_LENGTH = as.numeric(c13),
    RQD = as.numeric(c14),
    TOTAL_HOURS = as.numeric(c15),
    HQ_0_to_100 = as.numeric(c21),
    HQ_100_to_200 = as.numeric(c22),
    HQ_200_to_300 = as.numeric(c23),
    HQ_300_to_400 = as.numeric(c24),
    HQ_400_to_500 = as.numeric(c25),
    NQ_0_to_100 = as.numeric(c28),
    NQ_100_to_200 = as.numeric(c29),
    NQ_200_to_300 = as.numeric(c30),
    NQ_300_to_400 = as.numeric(c31),
    NQ_400_to_500 = as.numeric(c32),
    TRAVEL_TIME = as.numeric(c41),
    SERVICING = as.numeric(c42),
    DRILLING = as.numeric(c43),
    BLASTING_HRS = as.numeric(c44),
    REAMING = as.numeric(c45),
    CASING = as.numeric(c46),
    MEETINGS = as.numeric(c47),
    PULLOUT_PULLDOWN = as.numeric(c48),
    WASHOUT_CONDITIONING = as.numeric(c49),
    RIG_MOVING = as.numeric(c50),
    RIG_SETTING_UP = as.numeric(c51),
    CEMENTING = as.numeric(c52),
    PLUGGING =as.numeric(c53),
    BIT_CHANGE = as.numeric(c54),
    HOLE_SURVEY = as.numeric(c55),
    JACK_CASING = as.numeric(c56),
    CEMENTING = as.numeric(c57),
    REDRILL = as.numeric(c58),
    TESTING = as.numeric(c59),
    SPT = as.numeric(c60),
    ORIENTATION = as.numeric(c61),
    JACK_CASING = as.numeric(c62),
    MIXING_MUD= as.numeric(c63),
    CEMENT_STANDBY = as.numeric(c64),
    STANDBY = as.numeric(c65),
    BREAKDOWN = as.numeric(c66),
    OTHERS = as.numeric(c67),
    HOUSEKEEPING_MOVEOUT  =as.numeric(c68),
    CONTROL_NO = as.numeric(c69),
    REMARKS = as.character(c71)
  )
  
  
  
  
  
  x <- x %>%
    tidyr::fill(DATE,SHIFT,DRILLER,TOTAL_HOURS)%>%
    filter(!is.na(HOLE_ID),!is.na(DATE)) %>% 
    mutate(TO = replace_na(TO,0))
  
  
  HOLE_DURATION = as.numeric(max(x$DATE)) - as.numeric(min(x$DATE)) 
  TOTAL_LENGTH = max(x$TO)
  RATE = TOTAL_LENGTH / HOLE_DURATION
  x[,53]<-  HOLE_DURATION
  x[,54]<-  as.numeric(TOTAL_LENGTH)
  x[,55]<-   as.numeric(RATE)
  
  x
}

############# pawer less ###########




DDR_READ<- function(i) {
  x = read_xls(i,sheet = 1)
  y =  ncol(x)
  if (y == 71) {71}
  if (y == 70) {70}
}



df_all <- lapply(file.list_all[c(1,10,20)], DDR_READ) %>%
  # bind_rows %>%
  as.data.frame()

df_ncol <- lapply(file.list_all[c(1,56)], DDR_NCOL_1) %>%
  bind_rows %>%
  as.data.frame()

df_2022 <- lapply(file.list_2022, DDR_READ) %>%
  bind_rows %>%
  as.data.frame()

df_2021 <- lapply(file.list_2021[1:25], DDR_READ_2021) %>%
  bind_rows %>%
  as.data.frame()

ncol_2021 <- lapply(file.list_all,DDR_NCOL)
ncol_2021 

df_2020 <- lapply(file.list_2020,DDR_READ) %>%
  bind_rows %>%
  as.data.frame()

df_final <- bind_rows(df_2022,df_2021,df_2020)



write_xlsx(df_final,"D:\\16_Exploration\\10_DDR\\df_final.xlsx")


