######### RIDL API FUNCTIONS

library(dplyr)

########
# API SPECIFICATIONS AND ENUMERATORS
########

# get json with schema
ridl_schema_dataset <- jsonlite::fromJSON("https://raw.githubusercontent.com/okfn/ckanext-unhcr/master/ckanext/unhcr/schemas/dataset.json") #, flatten = TRUE)
ridl_schema_container <- jsonlite::fromJSON("https://raw.githubusercontent.com/okfn/ckanext-unhcr/master/ckanext/unhcr/schemas/data_container.json")

# resources fields
ridl_schema_container_fields <- ridl_schema_container$fields$field_name
ridl_schema_container_fields_required <- ridl_schema_container$fields$field_name[ridl_schema_container$fields$required %in% TRUE]
ridl_schema_container_fields_with_enum <-  ridl_schema_container$fields$field_name[! ridl_schema_container$fields$choices %in% list(NULL)]


# dataset fields
ridl_schema_dataset_fields <- ridl_schema_dataset$dataset_fields$field_name
ridl_schema_dataset_fields_required <- ridl_schema_dataset$dataset_fields$field_name[ridl_schema_dataset$dataset_fields$required %in% TRUE]
ridl_schema_dataset_fields_required <- union("owner_org", ridl_schema_dataset_fields_required) # owner should be specified as required
ridl_schema_dataset_fields_with_enum <-  ridl_schema_dataset$dataset_fields$field_name[! ridl_schema_dataset$dataset_fields$choices %in% list(NULL)]

# resources fields
ridl_schema_resource_fields <- ridl_schema_dataset$resource_fields$field_name
ridl_schema_resource_fields_required <- ridl_schema_dataset$resource_fields$field_name[ridl_schema_dataset$resource_fields$required %in% TRUE]
ridl_schema_resource_fields_with_enum <-  ridl_schema_dataset$resource_fields$field_name[! ridl_schema_dataset$resource_fields$choices %in% list(NULL)]


# Function to create an enumerator given the field name and the schema
enumerator_create <- function(field_name, schema){
  
  # get the choiches for that field
  choices <- schema$choices[ schema$field_name == field_name ][[1]]
  
  # iterate over choiches and add them to the enumerator list
  enum <- list()
  for(i in 1:length(choices$value)){
    
    value <- choices$value[i]
    label <- choices$label[i]
    enum[[label]] <- value
  }
  
  return(enum)
}

# Enumerators for dataset fields
for (field in ridl_schema_dataset_fields_with_enum) {
  variable_name <- paste0("ridl_enum_dataset_", field)
  enumerator <- enumerator_create(field_name = field, schema = ridl_schema_dataset$dataset_fields)
  assign(variable_name, enumerator)
}

# Enumerators for resource fields
for (field in ridl_schema_resource_fields_with_enum) {
  variable_name <- paste0("ridl_enum_resource_", field)
  enumerator <- enumerator_create(field_name = field, schema = ridl_schema_dataset$resource_fields)
  assign(variable_name, enumerator)
}

# Enumerators for container fields
for (field in ridl_schema_container_fields_with_enum) {
  variable_name <- paste0("ridl_enum_container_", field)
  enumerator <- enumerator_create(field_name = field, schema = ridl_schema_container$fields)
  assign(variable_name, enumerator)
}



########
# CONNECTION
########

# setup RIDL connection. If user_acceptance_testing is true, https://ridl-uat.unhcr.org/ will be used instead
ridl_setup <- function(user_acceptance_testing = TRUE, ridl_api_key){
  
  ridl_https_address <- if(user_acceptance_testing) {
    "https://ridl-uat.unhcr.org/api/"
  }else{
    "https://ridl.unhcr.org/api/"
  }
  Sys.setenv("RIDL_API_KEY" = ridl_api_key, "RIDL_API_ADDRESS" = ridl_https_address)
  
}

# get current API key
ridl_api_key <- function(){
  return(Sys.getenv("RIDL_API_KEY"))
}

# get current API action url
ridl_api_action_url <- function(){
  return(paste0(Sys.getenv("RIDL_API_ADDRESS"), "action/"))
}


########
# ACTIONS
########

# gets a RIDL generic action 
ridl_get_action <- function(action, paramenters_list = NULL, print_error = TRUE) {
  
  # append action to api url
  api_url <- paste0(ridl_api_action_url(), action)
  
  # remove elements that contain NA
  paramenters_list <- paramenters_list[! paramenters_list %in% list(NA)]
  
  # get action. Header contains api key, body the parameters
  # GET returns a response object
  # content gets the content of the response object
  received_content <- httr::GET(url = api_url,
                                 config = httr::add_headers("Authorization" = ridl_api_key()),
                                 query = paramenters_list) %>% httr::content() 
  
  # if response is only one element, it is probably a bad request
  if(length(received_content) == 1){
    message("\nRequest failed with: ", api_url)
    message(received_content)
    return(NULL)
  }
  
  # if not success print error
  if( ! received_content$success %in% TRUE & print_error){
    message("The parameters body list was not correctly received")
    print(paramenters_list)
    message("Request failed with: ", api_url)
    message("\nThe following error was returned:")
    print(received_content$error)
  }
  
  # return result, NULL if an error occured
  received_result <- received_content$result
  return(received_result)
  
}


# posts a RIDL generic action 
ridl_post_action <- function(action, paramenters_list = NULL, print_error = TRUE) {
  
  # append action to api url
  api_url <- paste0(ridl_api_action_url(), action)
  
  # remove elements that contain NA
  paramenters_list <- paramenters_list[! paramenters_list %in% list(NA)]
  
  # set encode
  encode <- "json"
  if(action %in% c("cloudstorage_initiate_multipart", "cloudstorage_upload_multipart", "cloudstorage_finish_multipart")){
    encode <- c("multipart", "form", "json", "raw")
  }
  
  # post action. Header contains api key, body the parameters
  # POST returns a response object
  # content gets the content of the response object
  received_content <- httr::POST(url = api_url,
                                 config = httr::add_headers("Authorization" = ridl_api_key()),
                                 body = paramenters_list, encode = encode) %>% httr::content() 
  
  # if response is only one element, it is probably a bad request
  if(length(received_content) == 1){
    message("\nRequest failed with: ", api_url)
    message(received_content)
    return(NULL)
  }
  
  # if not success print error
  if( ! received_content$success %in% TRUE & print_error){
    message("The parameters body list was not correctly received")
    print(paramenters_list)
    message("Request failed with: ", api_url)
    message("\nThe following error was returned:")
    print(received_content$error)
  }
  
  # return result, NULL if an error occured
  received_result <- received_content$result
  return(received_result)
}


########
# CONTAINER
########

# return a list with empty elements to be filled for container
ridl_container_template <- function(){
  
  container <- list()
  
  # Required
  container$name <- NA # REQUIRED;  the name of the container
  container$title <- NA # REQUIRED;
  container$country <- NA # REQUIRED;ENUM
  container$geographic_area <- NA # REQUIRED;
  container$description <- NA #
  container$sectoral_area <- NA # ENUM
  container$population <- NA #
  container$tag_string <- NA #
  
  return(container)
}

# get list of containers
ridl_container_list <- function(){
  
  parameters <- list()
  parameters$permission <- "read"
  
  r <- ridl_get_action("organization_list_for_user", parameters)
  
  if(!is.null(r)){
    cond <- sapply(r, function(x){x$type == "data-container"})
    r <- r[cond]
  }
  
  return(r)
}

# get a container metadata given name or id
ridl_container_show <- function(container_name_or_id){
  
  parameters <- list()
  parameters$id <- container_name_or_id
  
  r <- ridl_get_action("organization_show", parameters)
  return(r)
}

# check if container exits
ridl_container_exist <- function(container_name_or_id){
  
  parameters_list <- list()
  parameters_list$id <- container_name_or_id
  
  # get container
  r <- ridl_get_action("organization_show", parameters_list, FALSE)
  dataset_exist <- ! is.null(r)
  
  return(dataset_exist)
}

# create container
ridl_container_create <- function(container_parameters_list, container_parent_name){
  
  container_parameters_list$type <- "data-container"
  container_parameters_list$groups <- list(list(name = container_parent_name))
  r <- ridl_post_action("organization_create", container_parameters_list)
  
  return(r)
}

# update container
ridl_container_update <- function(container_parameters_list, container_parent_name){
  
  container_parameters_list$type <- "data-container"
  container_parameters_list$groups <- list(list(name = container_parent_name))
  r <- ridl_post_action("organization_update", container_parameters_list)
  
  return(r)
}

# create a new resource or replace it if already exists
ridl_container_create_or_update <- function(container_parameters_list, container_parent_name){
  
  # currently create overwrites like update
  ridl_container_create(container_parameters_list, container_parent_name)
  
}


########
# DATASET
########

# return a list with empty elements to be filled for a resource of type attachment (reports, questionnaire, etc)
ridl_dataset_template <- function(){
  
  dataset <- list()
  
  # Required
  dataset$owner_org <- NA # REQUIRED;  the name of the container
  dataset$name <- NA # REQUIRED; the name will be used in the last part of the url to identify the dataset; it is also a unique identifier
  dataset$title <- NA # REQUIRED; the title of the dataset
  dataset$notes <- NA # REQUIRED; the abstract/description of the dataset
  dataset$visibility <- NA # REQUIRED; ENUM; specify if the dataset is private or not
  dataset$external_access_level <- NA # REQUIRED; ENUM;
  dataset$data_collector <- NA # REQUIRED; 
  dataset$keywords <- NA # REQUIRED; ENUM;
  dataset$unit_of_measurement <- NA # REQUIRED; Household/individual
  dataset$data_collection_technique <- NA # REQUIRED; ENUM; face to face etc.
  dataset$archived <- NA # REQUIRED; ENUM;
  
  # not required
  dataset$short_title <- NA #
  dataset$tag_string <- NA #
  dataset$url <- NA #
  dataset$private <- NA #
  dataset$data_sensitivity <- NA # ENUM;
  dataset$original_id <- NA #
  dataset$sampling_procedure <- NA #
  dataset$operational_purpose_of_data <- NA # ENUM;
  dataset$'hxl-ated' <- NA #
  dataset$process_status <- NA # ENUM;
  dataset$identifiability <- NA # ENUM;
  dataset$geog_coverage <- NA #
  dataset$linked_datasets <- NA #
  dataset$admin_notes <- NA #
  dataset$sampling_procedure_notes <- NA # ENUM;
  dataset$response_rate_notes <- NA #
  dataset$data_collection_notes <- NA #
  dataset$weight_notes <- NA #
  dataset$clean_ops_notes <- NA #
  dataset$data_accs_notes <- NA #
  
  return(dataset)
}

# create a resource if it does not exist
ridl_dataset_create <- function(dataset_parameters_list){
  
  # create dataset
  r <- ridl_post_action("package_create", dataset_parameters_list)
  
  return(r)
  
}

# replace a resource that already exist
ridl_dataset_update <- function(dataset_parameters_list){
  
  # create dataset
  r <- ridl_post_action("package_update", dataset_parameters_list)
  
  return(r)
  
}

# create a dataset or, if already present, replace it
ridl_dataset_create_or_update <- function(dataset_parameters_list){
  
  # get the dataset given that name
  dataset_name <- dataset_parameters_list$name
  dataset_exist <- ridl_dataset_exist(dataset_name)
  
  # use create or update accordingly
  if( ! dataset_exist){
    r <- ridl_dataset_create(dataset_parameters_list)
    return(r)
  }else{
    r <- ridl_dataset_update(dataset_parameters_list)
    return(r)
  }
}

# check if dataset exits
ridl_dataset_exist <- function(dataset_name_or_id){
  
  dataset_parameters_list <- list()
  dataset_parameters_list$id <- dataset_name_or_id
  
  # get dataset
  r <- ridl_get_action("package_show", dataset_parameters_list, FALSE)
  dataset_exist <- ! is.null(r)
  
  return(dataset_exist)
}

# get a dataset
ridl_dataset_show <- function(dataset_name_or_id){
  
  dataset_parameters_list <- list()
  dataset_parameters_list$id <- dataset_name_or_id
  
  # get dataset
  r <- ridl_get_action("package_show", dataset_parameters_list)
  
  return(r)
}



########
# RESOURCES
########

# return a list with empty elements to be filled for a resource of type attachment (reports, questionnaire, etc)
ridl_resource_template_attachment <- function(){

  resource <- list()
  resource$type <- "attachment" # in RIDL resources can be attachment or data; do not modify
  resource$id <- NA # specify only if you want to update a resource that already exists
  resource$package_id <- NA # REQUIRED; the id OR the name of the dataset that will receive the resource; 
  resource$name <- NA # name of the resource
  resource$description <- NA # description of the resource, appears under the title
  resource$file_type <- NA # Enumerator; the type of file, like other, questionnaire, report etc.

  # not necessary
  #resource$format <- NULL # the file type (pdf, csv, etc.); it is not needed because it gets recognised automatically
  #resource$url_type <- "upload" # always set as "upload" to upload a file; will set it in another function


  return(resource)
}

# return a list with empty elements to be filled for a resource of type attachment (reports, questionnaire, etc)
ridl_resource_template_data <- function(){
  
  resource <- list()
  resource$type <- "data" # in RIDL resources can be attachment or data; do not modify
  resource$id <- NA # specify only if you want to update a resource that already exists
  resource$package_id <- NA # REQUIRED; the id OR the name of the dataset that will receive the resource; 
  resource$name <- NA # Name of the resource 
  resource$description <- NA #  description of the resource, appears under the title
  resource$date_range_start <- NA # REQUIRED; data collection start date; format "yyyy-mm-dd"
  resource$date_range_end <- NA # REQUIRED; data collection end date; format "yyyy-mm-dd"
  resource$version <- NA # REQUIRED; version of the file, for example 1, 2, 2.1 etc.
  resource$identifiability <- NA # Enumerator; level of identifiability like  "anonymized_enclave"  "personally_identifiable" "anonymized_enclave" "anonymized_scientific"  "anonymized_public" 
  resource$process_status  <- NA # Enumerator; the state of the data like "anonymized" "raw" "cleaned" "anonymized"
  resource$file_type <- NA  # Enumerator; the type of file like  "microdata" 

  # these are managed by other functions when creating the resource to avoid manual mistakes
  #resource$url_type <- "upload" # if set to upload, uploads an actual file from pc, otherwise will just put a link
  #resource$url <- NULL #"test.csv" # path to file if uploaded, url of web resource if not uploaded
  #resource$upload <-  # should be used to upload 
  
  return(resource)
}

# get a resource
ridl_resource_show <- function(resource_id){
  
  parameters <- list()
  parameters$id <- resource_id
  
  r <- ridl_get_action("resource_show", parameters)
  
  return(r)
}


# create a resource if it does not exist
ridl_resource_create <- function(resource_parameters_list, file_path){
  
  # add the file name to the body so that it correspond to the uploaded file and the link work
  resource_parameters_list$url <- fs::path_file(file_path)
  
  # specify that the url as upload, otherwise it will be a link to the internet
  resource_parameters_list$url_type <- "upload"
  
  # create metadata
  r <- ridl_post_action("resource_create", resource_parameters_list)
  
  # upload file
  ridl_resource_upload_file(r$id, file_path)
  
  return(r)
}

# replace an existing resource
ridl_resource_update <- function(resource_parameters_list, file_path){
  
  # add the file name to the body so that it correspond to the uploaded file and the link work
  resource_parameters_list$url <- fs::path_file(file_path)
  
  # specify that the url as upload, otherwise it will be a link to the internet
  resource_parameters_list$url_type <- "upload"
  
  # create metadata
  r <- ridl_post_action("resource_update", resource_parameters_list)
  
  # upload file
  ridl_resource_upload_file(r$id, file_path)
  
  return(r)
}

# create a new resource or replace it if already exists
ridl_resource_create_or_update <- function(resource_parameters_list, file_path){
  
  # currently create overwrites like update
  ridl_resource_create(resource_parameters_list, file_path)
  
}

# returns the first id found for a resource with a given name
ridl_resource_get_id_from_name <- function(dataset, resource_name){
  
  for(resource in dataset$resources){
    if(resource$name == resource_name){
      resource_id <- resource$id
      return(resource_id)
    }
  }
  return(NULL)
}


# upload a file for a give resource, if a file already present, it will be replaced
ridl_resource_upload_file <- function(resource_id, file_path){
  
  body1 <- list(id = resource_id, 
                name = fs::path_file(file_path), 
                size = as.numeric(fs::file_size(file_path)))
  
  r1 <- ridl_post_action("cloudstorage_initiate_multipart", body1)
  #print(r1)
  
  # get upload id
  upload_id <- r1$id
  
  body2 <- list(id = resource_id,
                uploadId = upload_id,
                partNumber = 1,
                upload = httr::upload_file(file_path)
  )
  
  r2 <- ridl_post_action("cloudstorage_upload_multipart", body2)
  #print(r2)
  
  body3 <- list(id = resource_id,
                uploadId = upload_id,
                save_action = "go-dataset-complete"
  )
  
  r3 <- ridl_post_action("cloudstorage_finish_multipart", body3)
  #print(r3)
}



#############Example

#ridl_setup(user_acceptance_testing = TRUE, ridl_api_key = "XXXXXXXX_a_ridl_api_key_XXXXXXXX")

# # create dataset
# a_dataset <- ridl_dataset_template()
# a_dataset$owner_org <- "chad"
# a_dataset$name <- "test_create_dataset_chad"
# a_dataset$title <- "This is a test dataset"
# a_dataset$notes <- "My abstract"
# a_dataset$visibility <- ridl_enum_dataset_visibility$Private
# a_dataset$external_access_level <- ridl_enum_dataset_external_access_level$`Not available`
# a_dataset$data_collector <- paste( "Action contre la faim", "UNHCR", sep = ",")
# a_dataset$keywords <- NULL  #NULL#z_keys#"6"#(ridl_enum_dataset_keywords$`Food Security`, ridl_enum_dataset_keywords$`Community Services`)
# a_dataset$unit_of_measurement <- "Household and individual"
# a_dataset$data_collection_technique <- ridl_enum_dataset_data_collection_technique$`Face-to-face interview`
# a_dataset$archived <- ridl_enum_dataset_archived$Yes
# a_dataset$tag_string <- NULL
# a_dataset$keywords <- list(ridl_enum_dataset_keywords$`Health`, ridl_enum_dataset_keywords$`Water Sanitation Hygiene`)
# a_dataset$tag_string <- list("a_tag_10", "a_tag_15")
# 
# a_dataset_response <- ridl_dataset_create_or_update(a_dataset)
# 
# a_dataset_show <- ridl_dataset_show("test_create_dataset_chad")
# 

# # create attchment
# a_attachment <- ridl_resource_template_attachment()
# a_attachment$package_id <- "test_create_dataset_chad"
# a_attachment$name <- "test_resource_attachment"
# a_attachment$file_type <- ridl_enum_resource_file_type$`Infographics & Dashboard`
# 
# a_resource_response <- ridl_resource_create_or_update(resource_parameters_list = a_attachment, file_path = "sens_test_file.txt")
# a_resource_show <- ridl_resource_show(a_resource_response$id)


# create container
# a_container <- ridl_container_template()
# a_container$name <- "a_container_test"
# a_container$title <- "A test container2"
# a_container$country <- ridl_enum_container_country$Burundi
# a_container$geographic_area <- "A geographic area"
# a_container$description <- "A description"
# a_container$population <- "Population of interest"
# a_container$sectoral_area <- list(ridl_enum_container_sectoral_area$`Health`, ridl_enum_container_sectoral_area$Protection)
# a_container$tag_string <- paste("a_tag1", "a_tag2", sep = ",")
# 
# a_container_response <- ridl_container_create(a_container, "burundi")
# 
# a_container_show<- ridl_container_show("a_container_test")


