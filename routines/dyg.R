library(dygraphs)
library(xts)
library(htmlwidgets)
library(plyr)
homedir = "/media/yujiex/work/ROCIS/ROCIS/DataBySensor/Dylos/concat_gen_spe/round_9/"
a = list.files(path=homedir, pattern="*.csv")
loc = "O"
filterLoc <- function(x) {
    length(grep(sprintf("_%s_", loc), x)) > 0
}
print(length(a))
b <- Filter(filterLoc, a)
print(length(b))
l = length(b)
acc = cbind()
for (i in 1:length(b)) {
## for (i in 1:1) {
    print(i)
    print(b[i])
    file <- sprintf("%s%s", homedir, b[i])
    ## print(file)
    name <- strsplit(b[i], "_")[[1]][1]
    ## print(name)
    newname <- sprintf("%s_Small", name)
    ## print(newname)
    df <- read.csv(file)
    df <- rename(df, c("Small"=newname))
    df$date = as.POSIXlt(df$Date.Time)
    df <- df[df$date > as.POSIXlt("2016-06-30 00:00:00"),]
    ## print(names(df))
    timeseries = xts(df[newname], df$date)
    acc <- cbind(acc, timeseries)
    head(df)
}
path = "/media/yujiex/work/ROCIS/ROCIS/DataBySensor/Dylos/across/round_9"
dygraph(acc, main = "Outdoor Small particle count") %>% dyOptions(logscale = TRUE) %>% saveWidget(sprintf("%s/%s_Small.html", path, loc), selfcontained=FALSE,libdir=path)
