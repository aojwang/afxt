#!/usr/bin/env Rscript

require(Xmisc)
require("RPostgreSQL")
require(TTR)
require(quantmod)
parser <- ArgumentParser$new()
parser$add_argument(
  '--type',type='character',
  help='type of recommendation'
)
parser$add_argument(
  '--date',type='character',
  help='YYYY-MM-DD'
)
print (parser$get_args()$type)
print (parser$get_args()$date)

cur_path = '/Users/grant/workspace/afxt'
cur_date = Sys.Date()
cur_type = 'up_trend_low'

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
con <- dbConnect(drv, dbname = "stock",
                 host = "localhost", port = 5432,
                 user = "aa", password = pw)
rm(pw) # removes the password

# check for the cartable



# TRUE
code_dfs <- dbGetQuery(con,
                       paste("
                       SELECT code FROM stock_recommend 
                       WHERE update_time=", "'", cur_date, "' AND reason=",
                             "'", cur_type, "'",
                       sep="")
                       )
print (code_dfs)
for (code in code_dfs$code)
{
  dfs <- dbGetQuery(con, 
                    paste("select s.code, s.date, s.close, s.open, s.high, s.low, s.volume
                          from stock_daily s
                          where s.code =", "'", code, "'", 
                          " and s.date >= '2015-05-01'",
                          sep=""))
  
  industry_dfs <- dbGetQuery(con, 
                          paste("select name, industry
                          from stock_info t
                          where code =", "'", code, "'",
                                sep=""))
  code_name = paste(code, industry_dfs$name, industry_dfs$industry, sep="-")
  print (code_name)
  rownames(dfs) = dfs$date
  mtx = data.matrix(dfs)
  mtx.XKP = as.xts(mtx)
  candleChart(mtx.XKP, up.col='red', dn.col='green', 
              name=paste(code, "MA5(blue), MA10(red), MA20(green), MA30(orange)", sep="-"), theme=chartTheme('white'))
  addMACD(col='red')
  addSMA(5, col='blue')
  addSMA(10, col='red')
  addSMA(20, col='green')
  addSMA(30, col='orange')
  dev.copy(pdf, paste(code_name, "pdf", sep="."), width=16, height=9)
  dev.off()
  dev.flush()
}