#########################################################################
# Simple attribution model - linear, first click, last click
#########################################################################

rm(list = ls())
library(dplyr)
library(lubridate)
options(warn=-1)

orders <- read.csv("ga_ordersmodel.csv",stringsAsFactors = F, colClasses = c("character"))
sessions <- read.csv("ga_sessionsmodel.csv",stringsAsFactors = F, colClasses = c("character"))


# data prep
sessions_prep <- sessions %>% mutate(`source_medium` = paste(source, medium, sep="/")) %>%
  select(-source, -medium, -profile_id, -session_id) %>%
  mutate(order_amount = NA,
         order_id = NA)
orders_prep <- orders  %>% 
  mutate(source_medium = NA, datetime = order_datetime, is_paid_traffic = NA, campaign = NA, keyword = NA, content = NA) %>% 
  select(-profile_id, -session_id, -order_datetime)

orders_prep <- orders_prep[,c(1,6,7,8,9,5,4,3,2)]



data <- rbind(sessions_prep, orders_prep)

# change class
data$datetime <- as.POSIXct(strptime(data$datetime, format = "%Y-%m-%d %H:%M:%S"))
data$order_amount <- as.numeric(data$order_amount)

# unbounce - empty revenue sequences or sequences with zero revenue
data_unbounce <- data %>% 
  group_by(client_id) %>%
  mutate(filter = ifelse(sum(as.numeric(order_amount), na.rm = T) > 0, 1, 0)) %>%
  filter(filter != 0) %>%
  select(-filter) %>%
  mutate(count = n())


# label chronological order
foo <- function(x) {y <- length(x[is.na(x) ==T]) } 
  group_by(client_id) %>%
  mutate(row_number = row_number(),
         count = ifelse(is.na(source_medium) == F , n() -foo(source_medium), NA ),
         count = ifelse(source_medium == "other/other", 1, count)) 

#########################################################################
# sequences with more than one transaction
# - for every transaction create new client_id sequence
#########################################################################

# get duplicated ids
data_dupid <- data_labeled %>%
  group_by(client_id) %>%
  filter(is.na(source_medium)) %>%
  summarise(nas = n())  %>%
  filter(nas > 1)

# split data into duplicated and unique data sets
data_duplicated <- data_labeled[grep(paste(data_dupid$client_id, collapse="|"),  data_labeled$client_id), ]
data_unique <- data_labeled[-grep(paste(data_dupid$client_id, collapse="|"),  data_labeled$client_id), ]


# for duplicate rows
foo2 <- function(x) {y <- length(unique(x))}

cop <- function(x) {
  poc <- foo(x$source_medium)
  rest <- data.frame(datetime = x$datetime,
                     source_medium = x$source_medium,
                     order_amount = x$order_amount,
                     order_id = x$order_id,
                     count = x$count,
                     row_number = x$row_number,
                     is_paid_traffic = x$is_paid_traffic,
                     campaign = x$campaign,
                     keyword = x$keyword,
                     content = x$content,
                     stringsAsFactors = F)
  client_id <- c()
  for (i in 1:poc) {
    id <- paste(x$client_id, "_", i, sep = "")
    client_id <- c(client_id,id)
  }
  client_id <- data.frame(client_id = client_id, stringsAsFactors = F)
  oth <- rest[rep(seq_len(nrow(rest)), poc), ]
  y <- cbind(client_id , oth)
}



cop2 <- function(x) {
  u <- unique(x$delete[x$delete != 0]) 
  out <- c()
  occur <- 1
  for (k in 1:nrow(x)) {
    if(x$delete[k] == u){
      out[k] <- occur
      occur <- occur+1
    }else{out[k] <- 0}
  }
  y <- cbind(x, out)
}


# reduplicate data
data_remdup <- data_duplicated %>%
  group_by(client_id) %>%
  do(cop(.)) %>%
  mutate(delete = ifelse(is.na(order_amount) == F, foo(count), 0)) %>%
  do(cop2(.)) %>%
  filter(out == strsplit(client_id, "_")[[1]][2] | out == 0) %>%
  select(-delete, -out) %>%
  filter(sum(as.numeric(order_amount), na.rm = T) > 0)

#########################################################################
#  source "other"
#########################################################################

data_connected <- rbind(data_remdup, data_unique)


data_clear <- data_connected %>%
  group_by(client_id) %>%
  mutate(source_medium = ifelse(n() == 1 & is.na(source_medium) == T, "other", source_medium),
         count = ifelse(source_medium == "other", 1, count)) 

#########################################################################
# lookup window
#########################################################################
safe.ifelse <- function(cond, yes, no) structure(ifelse(cond, yes, no), class = class(yes))
data_ready_window <- data.frame()

windows <- c(1,7,14,30,90)  #,7,14,30,90

for (i in 1:length(windows)) {
  
  data_timeselect <- data_clear  %>%
    group_by(client_id) %>%
    mutate(bordertime = safe.ifelse(is.na(source_medium) ==T, datetime,NA),
           bordertime2 = safe.ifelse(is.na(bordertime) == T, bordertime[!is.na(bordertime)], bordertime),
           bordertime3 = as.POSIXct(bordertime2) - days(windows[i])) %>%
    filter((as.POSIXct(datetime) > as.POSIXct(bordertime3)) & (as.POSIXct(datetime) <= as.POSIXct(bordertime2))) %>%
    select(-bordertime, -bordertime2, -bordertime3)
  
  # print(nrow(data_timeselect))

  
  # revenue
  data_revenue <- data_timeselect %>%
    group_by(client_id) %>%
    mutate(revenue = sum(as.numeric(order_amount), na.rm = T )) %>%
    select(-order_amount)
  
  
  # order_id
  data_ready <- data_revenue %>%
    group_by(client_id) %>%
    mutate(order_id = ifelse(is.na(order_id) == T, order_id[!is.na(order_id)], order_id),
           time_window = windows[i])
  
  data_ready_hourstoorder <- data_ready  %>%
    group_by(client_id) %>%
    mutate(datemax = max(datetime),
           datemin = min(datetime),
           hourstoorder = round(difftime(as.POSIXct(datemax), as.POSIXct(datemin), units = "hours")))
  
  data_ready_window <- bind_rows(data_ready_window, data_ready_hourstoorder)
}



write.csv(data_ready_window, file="attribution_prep.csv", row.names = FALSE)








# start building different models based on clean data

rm(list = ls())
library(dplyr)
library(lubridate)

data_ready <- read.csv(file = "attribution_prep.csv", stringsAsFactors = F)


#########################################################################
# last click
#########################################################################

data_lastclick <- data_ready %>%
  group_by(client_id,time_window) %>%
  filter(is.na(source_medium) == F ) %>% 
  mutate(last_source = ifelse(row_number == max(row_number), revenue , 0), 
         row_number = row_number(),
         transactions = ifelse(last_source == 0, 0, 1)) %>%
  filter(last_source > 0) %>%
  mutate(type = "last click") %>%
  select(-last_source)

#########################################################################
# first click
#########################################################################

data_firstclick <- data_ready %>% 
  group_by(client_id, time_window) %>% 
  filter(is.na(source_medium) == F) %>%
  mutate(first_source = ifelse(row_number == min(row_number), revenue , 0),
         row_number = row_number(),
         transactions = ifelse(first_source == 0, 0, 1))  %>%
  filter(first_source > 0) %>%
  mutate(type = "first click") %>%
  select(-first_source)

#########################################################################
# linear
#########################################################################

# assign revenue to right source
data_linear <- data_ready %>% 
  group_by(client_id, time_window) %>% 
  filter(is.na(source_medium) == F) %>%
  mutate(linear_source =  (revenue / n()) ,
         row_number = row_number(),
         transactions = 1/ n()) %>% 
  filter(linear_source > 0) %>%
  mutate(revenue = linear_source, type = "linear") %>%
  select(-linear_source)


#########################################################################
# last non direct 
#########################################################################


LND_tagged <- data_ready %>%
  group_by(client_id,time_window) %>%
  filter(is.na(source_medium) == F) %>%
  mutate( last_direct = ifelse(grepl("(direct)",source_medium) & row_number == max(row_number), 1, NA), 
    last_direct_sum = ifelse(sum(last_direct, na.rm = T ) > 0, 1, NA )) 

# the paths ending on direct
# LND_direct
# LND_nondirect

# divide into last direct / other last clcik
LND_direct <- LND_tagged %>% filter(!is.na(last_direct_sum)) #last_direct_sum == 1
LND_nondirect <- LND_tagged %>% filter(is.na(last_direct_sum)) #last_direct_sum == NA




# paths ending at direct
# D1. paths that end as direct, but before that they have other sources
# D2. paths that are only direct
  
D1 <- LND_direct %>%
  group_by(client_id, time_window) %>%
  mutate( D1 = ifelse(!grepl("(direct)", source_medium), 1, NA), 
          D1_sum = ifelse(sum(D1, na.rm = T ) > 0, 1, NA ))  %>%
  filter(!is.na(D1_sum))


D2 <- LND_direct %>%
  group_by(client_id, time_window) %>%
  mutate( D2 = ifelse(!grepl("(direct)", source_medium), NA, 1),
          D2_sum = ifelse(is.na(sum(D2)), NA, 1)) %>%
  filter(!is.na(D2_sum))
  
#################
# ATTRIBUTE
#################
  
  
# normal last click
LND_out <- rbind(LND_nondirect, D2) %>%
    group_by(client_id,time_window) %>%
    select( -last_direct, -last_direct_sum) %>% 
    mutate(LND_source = ifelse(row_number == max(row_number), revenue , 0))%>%
    filter(LND_source > 0) 


# last non direct attribute
D1_out <- D1 %>%
  group_by(client_id, time_window) %>%
  filter(!grepl("(direct)", source_medium)) %>%
  filter(row_number == max(row_number))
  
# join and unify
data_Last_Non_Direct <- rbind(D1_out, LND_out)  %>% 
  select(-last_direct, - last_direct_sum, - D1,  -D1_sum, -D2, -D2_sum, - LND_source) %>%
  mutate(type = "LND click",
         row_number = row_number(),
  transactions = 1)


#########################################################################
# output
#########################################################################

out <- rbind(data_linear, data_firstclick, data_lastclick, data_Last_Non_Direct)

write.csv(out, file="ga_attribution_simple.csv", row.names = FALSE)
