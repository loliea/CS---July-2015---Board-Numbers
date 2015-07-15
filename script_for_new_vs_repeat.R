library(RMySQL)
library(dplyr)
library(lubridate)
library(jsonlite)
library(reshape2)

##--------- 1 Extract the data from MySQL
## Create the connection to DB
credit_sesameDB <- dbConnect(MySQL(), user = "root", db="credit_sesame")
## Read the table that contains the revenue
revenue <- dbReadTable(credit_sesameDB, "SalesFileUploadHistoryRecord")

##--------- 2 Tidy the dataset
## Reformat the date into date format
revenue <- transform(revenue, saleDate = ymd_hms(saleDate))
## Only keep April to June sales
revenue_amj <- filter(revenue, saleDate >= ymd_hms("2015-04-01 00:00:01")
                                                   & saleDate <= ymd_hms("2015-06-30 23:59:59"))
## Create a new column without time
names(revenue_amj)[9] <- "saleDateTime"
revenue_amj <- mutate(revenue_amj, saleDate = ymd(paste(year(saleDateTime), month(saleDateTime), day(saleDateTime), sep="-")))

#--------- 3 Extract the data in the JSON field
## - Extract the JSON data
JSON <- revenue_amj$propertiesJson
## Concatenate the JSON data from the different rows
JSON2 <- ""
for (i in 1:length(test)) {  #length(test)
  JSON2 <- paste0(JSON2, JSON[i], sep=",")
}
## Convert to to JSON format
JSON3 <- toJSON(JSON2)
## Clean the JSON before translating into data frame
JSON3 <- gsub('\\[\"',"[",JSON3)
JSON3 <- gsub(',\"\\]',"]",JSON3)
JSON3 <- gsub('\\\\',"",JSON3)
## Convert JSON to data frame
JSON4 <- fromJSON(JSON3, simplifyDataFrame = TRUE)
## Keep only the fields we care about
JSON5 <- JSON4[, c(58, 71, 1, 50, 11, 61, 66, 33, 53, 25, 92, 30, 78, 77, 9, 19)]
## Assign correct field names
names(JSON5) <- c("amountReported", "timeClicked", "verticalName", "loanAmount", "prodVendorID", "pageName", "store", "storeType", "vendorName", "PPCCampaignName", "prodBannerVersion", "userToken2", "pageObjectName", "referralType", "numDaysSinceReg", "regCompleteDate")
## Convert the time click from second to date
JSON5 <- transform(JSON5, timeClicked = as.POSIXct(timeClicked, origin = "1970-01-01"))
## Convert regCompleteDate to date format
JSON5 <- transform(JSON5, regCompleteDate = ymd(regCompleteDate))

##--------- 4 Combine back into the main dataset
## Attach the converted JSON data to the original dataset
revenue_amj_all <- cbind(revenue_amj, JSON5)

## --------- 5 Analysis
## regCompleteDate between 20150401 and 20150630
revenue_amj_all <- cbind(revenue_amj_all, typeUser = rep("aaa", nrow(revenue_amj_all)))
revenue_amj_new <- filter(revenue_amj_all, regCompleteDate >= ymd("150401"))
revenue_amj_exist <- filter(revenue_amj_all, regCompleteDate < ymd("150401"))
revenue_amj_nonRegPub <- filter(revenue_amj_all, is.na(regCompleteDate) & storeType %in% "Public")
revenue_amj_nonReg <- filter(revenue_amj_all, is.na(regCompleteDate) & !(storeType %in% "Public"))

hist(strptime(as.character(revenue_amj_nonReg$timeClicked), "%Y-%m-%d"), breaks = "weeks", format = "%d-%m")
table(revenue_amj_nonReg$storeType, revenue_amj_nonReg$store)

revenue_amj_new$typeUser <- "New"
revenue_amj_exist$typeUser <- "Existing"
revenue_amj_nonRegPub$typeUser <- "Non registered sublic site"
revenue_amj_nonReg$typeUser <- "Non registered"

## - GRAPH EXPLORATION - What happened in june with higest number of non registered on non public sites?
hist(strptime(as.character(revenue_amj_nonReg$timeClicked), "%Y-%m-%d"), breaks = "days", format = "%d-%m")
library(ggplot2)
#- Just non reg with Click Date
revenue_amj_nonReg_graph <- filter(revenue_amj_nonReg, revenue_amj_nonReg$timeClicked > "2015-06-05")
revenue_amj_nonReg_graph <- mutate(revenue_amj_nonReg_graph, dateClicked = as.character(strptime(as.character(revenue_amj_nonReg_graph$timeClicked), "%Y-%m-%d")))
ggplot(revenue_amj_nonReg_graph, aes(x=dateClicked, fill=vendorName)) + geom_histogram()
#- All users with Click Date
revenue_amj_all_graph <- filter(revenue_amj_all, timeClicked > "2015-06-05")
revenue_amj_all_graph <- mutate(revenue_amj_all_graph, dateClicked = as.character(strptime(as.character(timeClicked), "%Y-%m-%d")))
ggplot(revenue_amj_all_graph, aes(x=dateClicked, fill=vendorName)) + geom_histogram()
#- All users with Sales Date
revenue_amj_all_graph <- filter(revenue_amj_all, saleDate > "2015-06-05")
ggplot(revenue_amj_all_graph, aes(x=saleDate, fill=vendorName)) + geom_histogram()

revenue_amj_all_graph_noQuin <- filter(revenue_amj_all, timeClicked > "2015-06-05" & !(vendorName %in% "Quinstreet"))
ggplot(revenue_amj_all_graph_noQuin, aes(x=timeClicked, fill=vendorName)) + geom_histogram()

hist(revenue_amj_all$saleDate, breaks = "days", format = "%d-%m")
## - END GRAPH EXPLORATION

revenue_amj_all_type <- bind_rows(revenue_amj_new, revenue_amj_exist, revenue_amj_nonRegPub, revenue_amj_nonReg)
nrow(revenue_amj_all_type)

## -- breakdown of data by type of user and vendor
View(summarise(group_by(revenue_amj_all_type, typeUser, vendorName, verticalName), avg_rev = mean(amount), tot_rev = sum(amount), num = n()))
View(summarise(group_by(revenue_amj_all_type, typeUser, vendorName, verticalName, saleDate), 
               avg_rev = mean(amount), tot_rev = sum(amount),  num = n()))

## WRITE the summary to memory
x <- summarise(group_by(revenue_amj_all_type, typeUser, vendorName, verticalName, saleDate), 
               avg_rev = mean(amount), tot_rev = sum(amount),  num = n())
write.table(x, pipe("pbcopy"), sep="\t", row.names=FALSE, col.names=TRUE)

tot_by_vendor <- count(revenue_amj_all_type, vendorName)
names(tot_by_vendor) <- c("vendorName", "tot_cnt_vendor")
y <- inner_join(revenue_amj_all_type, tot_by_vendor, by = "vendorName")

## GRAPH BY VENDOR AND DATE
z <- summarise(group_by(y, vendorName, saleDate), 
               num = n(), pct_tot = n()/max(tot_cnt_vendor))
write.table(z, pipe("pbcopy"), sep="\t", row.names=FALSE, col.names=TRUE)
q <- summarise(group_by(y, vendorName,))
ggplot(z, aes(saleDate, num, color=vendorName)) + geom_line()  ##THE GRAPH


range(revenue_amj_new$amount)

table(revenue_amj_exist$vendorName, revenue_amj_exist$verticalName)

## time to purchase
summary(revenue_amj_new$numDaysSinceReg)
# Exclude data where no registration date or registration after June 30th
revenue_amj_all_clean <- filter(revenue_amj_all, !(is.na(regCompleteDate)))
revenue_amj_all_clean <- filter(revenue_amj_all_clean, regCompleteDate<ymd("150630"))

# Keep only the purchase where regCompleteDate > saleDateTime
## number millisecond in one day
nday <- 24*60*60*1000
nhour <- 60*60*1000
nminute <- 60*1000
revenue_amj_all_reg_good <- filter(revenue_amj_all_clean, (saleDateTime - regCompleteDate) >=0)

# Click
range((revenue_amj_all_reg_good$timeClicked - revenue_amj_all_reg_good$regCompleteDate)/1000) ## in sec

# Sales 
range((revenue_amj_all_reg_good$saleDateTime - revenue_amj_all_reg_good$regCompleteDate)/nhour) ## in hours
summary(as.numeric(revenue_amj_all_reg_good$saleDateTime - revenue_amj_all_reg_good$regCompleteDate)/nhour)
boxplot(as.numeric(revenue_amj_all_reg_good$saleDateTime - revenue_amj_all_reg_good$regCompleteDate)/nhour)

summarise(group_by(revenue_amj_all_reg_good, vendorName), 
          avg_time_to_purch = mean(as.numeric(saleDateTime - regCompleteDate)/nhour), 
          num = n(),
          tot_Sales = sum(amount))

nb_trans_by_user <- summarise(group_by(revenue_amj_all_type, userToken, typeUser, vendorName, store), nb_trans = n())
nb_trans_by_user.2 <- summarise(group_by(nb_trans_by_user, typeUser, nb_trans, vendorName, store), n())
write.table(nb_trans_by_user.2, pipe("pbcopy"), sep="\t", row.names=FALSE, col.names=TRUE)


user_multiple_trans <- filter(nb_trans_by_user, nb_trans>1 & nchar(userToken) == 36)
user_multiple_trans_list <- inner_join(user_multiple_trans, revenue_amj_all, by = "userToken")
user_multiple_trans_list <- user_multiple_trans_list[,-9]

