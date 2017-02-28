#!/usr/bin/env Rscript

require(Xmisc)
require("RPostgreSQL")
require(TTR)
require(quantmod)
require(ggplot2)

Sys.setlocale(, "zh_CN.UTF-8")

cur_path = '/Users/grant/workspace/afxt'
cur_date = Sys.Date()
cur_type = 'top_industry'

new_path = paste(cur_path, 'chart', sep='/')
if (!dir.exists(new_path)){
  dir.create(new_path)
}

new_path = paste(new_path, cur_date, sep='/')
if (!dir.exists(new_path)){
  dir.create(new_path)
}

new_path = paste(new_path, cur_type, sep='/')
if (!dir.exists(new_path)){
  dir.create(new_path)
}

setwd(new_path)

# create a connection
# save the password that we can "hide" it as best as we can by collapsing it
pw <- {
  ""
}

# loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")
# creates a connection to the postgres database
# note that "con" will be used later in each connection to the database
con <- dbConnect(drv, dbname = "fs",
                 host = "nile", port = 5432,
                 user = "aa", password = pw)
rm(pw) # removes the password

# check for the cartable



# TRUE
code_dfs <- dbGetQuery(con,
                       paste("
                             SELECT '行业指数' as index, industry, date, p_change FROM top_industry
                             WHERE update_date=", "'", cur_date, "' 
                                   AND industry in (
                                        SELECT industry FROM top_industry WHERE date = (select max(date) from top_industry)
                                        ORDER BY p_change DESC limit 10
                                   )
                             UNION ALL
                             SELECT '上证指数' as index, '上证指数' as industry, date, p_change from stock_daily where code = 'sh' and date >= (select min(date) from top_industry)
                             ",
                             sep=""))
print (code_dfs)
#ggplot(data=code_dfs, aes(x=date, y=p_change, color=industry)) +
#  geom_line() + ggtitle("热门行业统计") + theme(text = element_text(family = "Kaiti TC"))
  # + facet_grid(industry ~ index, labeller="label_both", margins=FALSE) 
ggplot(data=code_dfs, aes(x=date, y=p_change, fill=industry)) + geom_bar(stat="identity") + ggtitle("热门行业统计") + 
  theme(text = element_text(family = "Kaiti TC"))


ggsave(file=paste('top_industry.png', sep=""), width = 16, height = 9)

