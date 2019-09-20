
require(cronR)
# update_NEFT daily schedule
cmd <- cron_rscript("schedule.R",
                    rscript_log = file.path(normalizePath("schedule"),"logs_update_NEFT"),
                    rscript_args = "style_codes/manual_update_NEFT.R")

cron_add(cmd, frequency = 'daily',  at='7PM', id = 'update_neft', description = 'Sqoop-IO : Update NEFT', tags = c('NEFT','UPDATE'))
rm(cmd)

# update_RTGS daily schedule
cmd <- cron_rscript("schedule.R",
                    rscript_log = file.path(normalizePath("schedule"),"logs_update_RTGS"),
                    rscript_args = "style_codes/manual_update_RTGS.R")

cron_add(cmd, frequency = 'daily',  at='11PM', id = 'update_rtgs', description = 'Sqoop-IO : Update RTGS', tags = c('RTGS','UPDATE'))
rm(cmd)