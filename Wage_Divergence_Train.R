##### Loading Libraries ######
library(RJDBC)
library(stargazer)
library(caret)
library(rJava)

options("scipen" = 999)
options(java.parameters = "-Xmx4g")

######## Set Environment variables/Working Directory/Config
model_root = paste(Sys.getenv('app_root'), 'models/Annuities_Wage_Divergence/', sep="")
setwd(model_root)
config_root = paste(Sys.getenv('app_root'), 'configs/Config_File_Dev.R', sep="")
source(config_root)


###################################
# Load data (should be an SQL call)
###################################
print("Load Data")

drv <- RJDBC::JDBC(driverClass = Driver_Class, classPath=Class_Path, " ")
con <- RJDBC::dbConnect(drv, JDBC_Conn, userid, pass)

Wage_Divergence <- dbGetQuery(con, "select * from in_wag_divergence WHERE monthly_wag > 0 AND wps_monthly_wag > 0 ")

dbDisconnect(con)

#CONVERTING CATEGORICAL ATTRIBUTES INTO FACTORS
Wage_Divergence$OCCUP_THREE_DIGIT  <- as.factor(as.character(Wage_Divergence$OCCUP_THREE_DIGIT ))
Wage_Divergence$LOCATION_NM <- as.factor(as.character(Wage_Divergence$LOCATION_NM))
Wage_Divergence$IS_SAUDI_IND <- as.factor(as.character(Wage_Divergence$IS_SAUDI_IND))
Wage_Divergence$SEX <- as.factor(as.character(Wage_Divergence$SEX))
Wage_Divergence$INDUSTRY_DESC <- as.factor(as.character(Wage_Divergence$INDUSTRY_DESC))
Wage_Divergence$WAGE_VARIANCE <- as.factor(as.character(Wage_Divergence$WAGE_VARIANCE))



##Data Set for Logistic Regression
WAGE_DIV_stg1 <- Wage_Divergence

## Removing not required columns for Stage1 Training
WAGE_DIV_stg1$WPS_MONTHLY_WAG<- NULL
WAGE_DIV_stg1$SIMIS_MONTHLY_WAG<- NULL
WAGE_DIV_stg1$WPS_SIMIS_WAGE_DIFF<-NULL
WAGE_DIV_stg1$ABS_WPS_SIM_WAGE_DIFF<-NULL


# Removing observations that can have NAs in any available attribute
WAGE_DIV_stg1 <- WAGE_DIV_stg1[complete.cases(WAGE_DIV_stg1),]


#BUILDING THE Logistic MODEL to get probability of variance happening between simis and WPS

STG1_LOGISTIC<- glm(WAGE_VARIANCE ~  TOTAL_EXP_IN_DAYS + OCCUP_THREE_DIGIT
              + ENG_EXP_IN_DAYS + ESTB_SIZE + LOCATION_NM + IS_SAUDI_IND 
              + SEX + AGE + INDUSTRY_DESC, data = WAGE_DIV_stg1, family = "binomial")

# Removing few arguments of STG1_Logistic to reduce the space occupied by the r object.
STG1_LOGISTIC$model <- NULL
STG1_LOGISTIC$qr$qr <- NULL
#### Save Model ########
save(STG1_LOGISTIC, file="wage_div_stg1.robj")



#############STAGE 2 #######################
#DIVIDING THE DATA SET INTO TWO -> +VE VARIANCE AND -VE VARIANCE
#Taking only -ve variance as we are interested in Wage Inflation

########MODEL FOR -VE VARIANCE######################################################
####################################################################################

WAGE_DIV_NEGATIVE <- Wage_Divergence[Wage_Divergence$WPS_SIMIS_WAGE_DIFF < 0,]

##ignoring the diff between 0 to 1

WAGE_DIV_NEGATIVE<- WAGE_DIV_NEGATIVE[WAGE_DIV_NEGATIVE$ABS_WPS_SIM_WAGE_DIFF >= 1,]

# Eliminate any records with NA/NULL values
WAGE_DIV_NEGATIVE<-WAGE_DIV_NEGATIVE[complete.cases(WAGE_DIV_NEGATIVE),]

# Creating Log of absolute wage difference
WAGE_DIV_NEGATIVE$LOG_ABS_WAGE_DIFF <- log(WAGE_DIV_NEGATIVE$ABS_WPS_SIM_WAGE_DIFF)

#Building Stage2 log normal regression model to get the severity of variance
STG2_MODEL<- lm(LOG_ABS_WAGE_DIFF ~  TOTAL_EXP_IN_DAYS + OCCUP_THREE_DIGIT
                 + ENG_EXP_IN_DAYS + ESTB_SIZE + LOCATION_NM + IS_SAUDI_IND 
                 + SEX + AGE + INDUSTRY_DESC + INDUSTRY_DESC:LOCATION_NM,data = WAGE_DIV_NEGATIVE)

# Removing few arguments of STG2_Model to reduce the space occupied by the r object.
STG2_MODEL$model <- NULL
STG2_MODEL$qr$qr <- NULL

#### Save Model ########
save(STG2_MODEL, file="wage_div_stg2.robj")

