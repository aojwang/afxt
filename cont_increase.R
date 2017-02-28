#!/usr/bin/env Rscript

require(Xmisc)
require("RPostgreSQL")
require(TTR)
require(quantmod)
require(ggplot2)

Sys.setlocale(, "zh_CN.UTF-8")

cur_path = '/Users/grant/workspace/afxt'
cur_date = Sys.Date()
cur_type = 'cont_increase'

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
                             SELECT '行业指数'::TEXT as index, industry, s.date, s.p_change - t.p_change as p_change
                             FROM top_industry s left join 
                             (SELECT date, p_change 
                              from stock_daily 
                              where code = 'sh' and date >= (select min(date) from top_industry)) t
                             on s.date = t.date
                             WHERE update_date=", "'", cur_date, "' 
                                   AND industry in (
                                        SELECT industry FROM top_industry WHERE date = (select max(date) from top_industry)
                                        ORDER BY p_change DESC limit 20
                                   )
                             ",
                             sep=""))
print (code_dfs)
ggplot(data=code_dfs, aes(x=date, y=p_change, color=industry)) +
  geom_line() +
  geom_point() +
  ggtitle("热门行业趋势研究") +
  theme(text = element_text(family = "Kaiti TC"), 
        strip.text.x = element_text(size = 14, face="bold"),
        strip.text.y = element_text(size = 14, angle = -90, face="bold")) +
  facet_grid(industry ~index, margins=FALSE)
  
#  labeller="label_both"
#ggplot(data=code_dfs, aes(x=date, y=p_change, fill=industry)) + geom_bar(stat="identity", position=position_dodge()) + ggtitle("热门行业统计") + 
#  theme(text = element_text(family = "Kaiti TC"))


ggsave(file=paste('top_industry.png', sep=""), width = 16, height = 40)

