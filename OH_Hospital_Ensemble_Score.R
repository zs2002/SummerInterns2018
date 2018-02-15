##### BEGIN
# DESC This script performs ensemble score model to include 
#         - Input
#         - FM_ADMIN.WC_OUT_OVERUTILIZATION
#         - FM_ADMIN.WC_OUT_TRMT_OVERAGE
#         - FM_ADMIN.WC_OUT_TRMT_DURATION
#         - FM_ADMIN.WC_OUT_HOSPITAL_TRMT_SCORE
#         - FM_ADMIN.WC_FM_IN_OH_GEN
#         - Output
#           WC_FM_OUT_OH_HOS_ENS
#           DESC The output is a 'Probability' predicted probability score for each hospital.
# DESC This is Revision 1
# Last modified By: zos
# Last modified in: 21Sep2017 
# Notes: Added Incremental data load for new scoring
 #       Added retreive old score when new scoring is not found for any base model
##### END

##### Script to Score ensemble model for OH in Hospital level ######
######## Set Environment variables/Working Directory/Config - start
model_name <- "OH_Ensemble_Hospital"
model_root <- paste(paste(Sys.getenv('app_root'), 'models\\', sep=""), model_name, sep="")
setwd(model_root) 

#Source Config Directory, medianPowerTransform.R and OH_Hospital_Ensemble_Help.R 
source(paste(Sys.getenv('app_root'), 'configs/Config_File_Dev.R', sep=""))                                           
source(paste(Sys.getenv('app_root'), 'models/r_libraries/medianPowerTransform.R', sep=""))                                      
# source(file.path(paste(Sys.getenv('app_root'), 'models/r_libraries', sep=""),"medianPowerTransform.R"))   #Source medianPowerTransform.R file
source(paste(model_root,"\\OH_Hospital_Ensemble_Helper.R", sep=""))     #Source OH_Hospital_Ensemble_Help.R file

##### Load/Install Libraries #########
library(RJDBC)
library(dplyr)
library(rJava)
Sys.getlocale()
Sys.setlocale(category = "LC_ALL", locale = "Arabic")
######## Set Environment variables/Working Directory/Config - end

mstr.ErrMsg <- tryCatch({                                          #tryCatch for Exception Handling
  TimeStampRun_Start <- Sys.time()
  print(paste("Score Run started at ",TimeStampRun_Start))
  
  ###################################
  # Load trained model 
  ###################################
  load("Ens_Hos.robj")

  ###################################
  # Load data (should be an SQL call)
  ###################################
  print("full or incremental data")
  
  drv <- RJDBC::JDBC(driverClass = Driver_Class, classPath = Class_Path, " ")
  con_JDBC <- RJDBC::dbConnect(drv, JDBC_Conn, userid, pass)
  
  tbl_hn <- dbGetQuery(con_JDBC, "SELECT DISTINCT CREATED_DT FROM OUT_OH_HOS_ENS")
  if(nrow(tbl_hn)==0){
    print("Full data load")
    scoring_data  <- scoring_data_full(con_JDBC)
  } else {
    print("Incremental data load")
    scoring_data <- scoring_data_incremental(con_JDBC)
  }
  dbDisconnect(con_JDBC)

  # Continue only if there are records and no NAs
  if(anyNA(scoring_data,recursive = F)){
    print("Found no new cases to score")
  } else {                                            # Score Hospital level dataset
      ###################################
      ###### Scoring OH Overutilization dataset
      ###################################
      print("Scoring OH Overutilization dataset")
      # Select last Injury score & group by hospital code
      oh_Trmt_Overutil <- group_by_hospital_cd(select_last_injury_score(scoring_data$oh_Trmt_Overutil[complete.cases(scoring_data$oh_Trmt_Overutil[ ,2:4]),]), 'MEAN_OU_PROBABILITY', 'WEIGHTED_AVG')
      # Generating Scores
      ou_score_final <- score_ou(oh_Trmt_Overutil)
      print("Scoring OH Overutilization dataset - End")

      ######################################
      ######## Scoring OH Treatment Cost Overage dataset
      ######################################
      print("Scoring OH Treatment Cost Overage dataset")
      # Calculations for the residual
      trmt_Cost_Overage <- scoring_data$trmt_Cost_Overage[complete.cases(scoring_data$trmt_Cost_Overage[ , 2:5]),]
      trmt_Cost_Overage$CO_SCORE_RESEDUAL <- log(trmt_Cost_Overage$ACTUAL_COST+1) - log(trmt_Cost_Overage$PREDICTED_COST+1)
      # Select last Injury score & group by hospital code after calculate the weighted residual and Weighted Mean
      trmt_Cost_Overage <- group_by_hospital_cd(select_last_injury_score(trmt_Cost_Overage), 'CO_SCORE_RESEDUAL', 'CO_SCORE_WEIGHTED_AVG')
      # Generating Scores 
      co_score_final <- score_co(trmt_Cost_Overage)
      print("Scoring OH Treatment Cost Overage dataset - end")

      ######################################
      ####### Scoring OH Treatment Duration dataset
      ######################################
      print("Scoring OH Treatment Duration dataset")
      # Remove rows with missing scores
      trmt_Dur_Overage <- scoring_data$trmt_Dur_Overage[complete.cases(scoring_data$trmt_Dur_Overage[ , 2:5]),]
      # Calculations residual from actual and predicted log
      trmt_Dur_Overage$TD_SCORE_RESEDUAL <- log(trmt_Dur_Overage$ACTUAL_DURATION+1) - log(trmt_Dur_Overage$PREDICTED_DURATION+1)
      # Select last Injury score & group by hospital code after calculate the weighted residual and weighted mean
      trmt_Dur_Overage <- group_by_hospital_cd(select_last_injury_score(trmt_Dur_Overage), 'TD_SCORE_RESEDUAL', 'TD_SCORE_WEIGHTED_AVG')
      # Generating Scores 
      td_score_final <- score_td(trmt_Dur_Overage)
      print("Scoring OH Treatment Duration dataset - end")
    
      ###################################
      ###### Scoring OH Hospital Treatment Distribution dataset
      ###################################
      print("Scoring OH Hospital Treatment Distribution dataset")
      # Remove rows with missing scores
      hosp_Trmt_Dist <- scoring_data$hosp_Trmt_Dist[complete.cases(scoring_data$hosp_Trmt_Dist[ ,1:2]),]
      names(hosp_Trmt_Dist)[2] = "WEIGHTED_SCORE"
      # Select last Injury score & group by hospital code after calculate the weighted residual and weighted mean
      hosp_Trmt_Dist <- select_last_hospital_score(hosp_Trmt_Dist)
      hosp_Trmt_Dist$CREATED_DT <- NULL
      # Generating Scores - avg weight for this model = 1
      hd_score_final <- score_hd(hosp_Trmt_Dist)
      print("Scoring OH Hospital Treatment Distribution dataset - end")
      
      #############################################
      ### Combined Score for OH Ensemble Hospital 
      #############################################
      combined_score1 <- merge(x = ou_score_final[c("HOSPITAL_CD","OU_SCORE_ADJ")], y = co_score_final[c("HOSPITAL_CD","CO_SCORE_ADJ_M")], by = c("HOSPITAL_CD"), all = TRUE)
      combined_score2 <- merge(x = td_score_final[c("HOSPITAL_CD","TD_SCORE_ADJ_M")], y = hd_score_final[c("HOSPITAL_CD","HD_SCORE_ADJ")], by = c("HOSPITAL_CD"), all = TRUE)
      combined_score <- merge(x = combined_score1, y = combined_score2, by = c("HOSPITAL_CD"), all = TRUE)
      
      # Renaming columns
      colnames(combined_score)[2] <- "OU_SCORE"
      colnames(combined_score)[3] <- "CO_SCORE"
      colnames(combined_score)[4] <- "TD_SCORE"
      colnames(combined_score)[5] <- "HD_SCORE"
  
      ###### Retreive Old Score when any base models doesn't have new score #######
      ###### Load latest scores of base models from the ensemble model output table ######
      drv <- RJDBC::JDBC(driverClass = Driver_Class, classPath = Class_Path, " ")
      con_JDBC <- RJDBC::dbConnect(drv, JDBC_Conn, userid, pass)
      # get previous latest base models scores
      latest_base_model_scores <-  latest_base_score(con_JDBC)
      dbDisconnect(con_JDBC)
      
      ###### Merge latest base model scores from the ensemble model output table with scored data obtained in current model run
      combined_score <- merge(x = combined_score, y = latest_base_model_scores[c("HOSPITAL_CD","OU_SCORE_F","TD_SCORE_F","TO_SCORE_F","HD_SCORE_F")], by.x = "HOSPITAL_CD", by.y = "HOSPITAL_CD", all.x = TRUE)
    
      ###### Replace current NULL values scores of base model with latest non-NULL scores from the ensemble model output table
      combined_score$OU_SCORE[is.na(combined_score$OU_SCORE)] <- combined_score$OU_SCORE_F[is.na(combined_score$OU_SCORE)]
      combined_score$TD_SCORE[is.na(combined_score$TD_SCORE)] <- combined_score$TD_SCORE_F[is.na(combined_score$TD_SCORE)]
      combined_score$CO_SCORE[is.na(combined_score$CO_SCORE)] <- combined_score$TO_SCORE_F[is.na(combined_score$CO_SCORE)]
      combined_score$HD_SCORE[is.na(combined_score$HD_SCORE)] <- combined_score$HD_SCORE_F[is.na(combined_score$HD_SCORE)]
      
      # Remove previous scores 
      combined_score$OU_SCORE_F <-  NULL
      combined_score$TO_SCORE_F <-  NULL
      combined_score$TD_SCORE_F <-  NULL
      combined_score$HD_SCORE_F <-  NULL
      ###### Retreive Old Score when any base models doesn't have new score - End #######
      
      ###### Load latest scores of base models from the ensemble model output table -End ######
      
      # Updating column format
      combined_score$OU_SCORE <- as.numeric(combined_score$OU_SCORE)
      combined_score$CO_SCORE <- as.numeric(combined_score$CO_SCORE)
      combined_score$TD_SCORE <- as.numeric(combined_score$TD_SCORE)
      combined_score$HD_SCORE <- as.numeric(combined_score$HD_SCORE)
    
      # Calculating combined score and attaching creationtimestamp to it
      combined_score$COMBINED_SCORE <- rowMeans(x = combined_score[c("OU_SCORE","CO_SCORE","TD_SCORE","HD_SCORE")], na.rm = T)
      # Setting NA for cases where only 1 model is populating score 
      # combined_score_del$COMBINED_SCORE[rowSums(is.na(combined_score_del[2:5]))>=3] <- list(NULL)
      combined_score$COMBINED_SCORE[rowSums(is.na(combined_score[2:5]))>=3] <-  `is.na<-`(combined_score$COMBINED_SCORE[rowSums(is.na(combined_score[2:5]))>=3])
      # calcualte only at least two scores presents
      #combined_score_del <- combined_score_del[rowSums(is.na(combined_score_del[2:5]))<=2, ]
    
      ###################################
      # Save new scores into an output table 
      ###################################
      if(nrow(combined_score)==0){
        print("No records to insert into output table")
      } else {
        print("Populating output table")
        combined_score$CREATED_DT <- as.character.Date(format(Sys.time(),"%m/%d/%Y %H:%M:%S"))
        # Insert into DB
        drv <- RJDBC::JDBC(driverClass = Driver_Class, classPath = Class_Path, " ")
        con_JDBC <- RJDBC::dbConnect(drv, JDBC_Conn, userid, pass)
        dbWriteTable(con_JDBC, 'OUT_OH_HOS_ENS', combined_score, append = T, overwrite = F)
        dbDisconnect(con_JDBC)
      }
}
  TimeStampRun_End = Sys.time()
  print(paste("Score Run ended at ",TimeStampRun_End))
  #errors(mstr.ErrMsg <- "")                          #Check If we made it here, no errors were caught
}
, warnings = function(warning_cond) {
  #warning-handler-code
  message("Warning message:")
  message(warning_cond)
  return(NA)
}, errors = function(error_cond) {
  #error-handler-code
  message("error message:")
  message(error_cond)
  return(NA)
}, finally={
  #cleanup-code
  rm(con_JDBC)
  gc()
})
