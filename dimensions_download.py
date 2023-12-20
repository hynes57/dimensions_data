import boto3, dimcli, json, logging, time #, #retrying
from requests.exceptions import HTTPError

# CLIENTS
s3 = boto3.client('s3')

sns = boto3.client('sns')

dimcli.login(key="792DDFAFCCA7478D8F37159F274A2783", endpoint="https://app.dimensions.ai/api/dsl/v2")
dsl = dimcli.Dsl()

# GLOBALS
phone_number = '+447709521591'
topic_arn = 'arn:aws:sns:eu-west-2:713538054121:dimensions-download'
bucket_name = 'sdl-dimensions'
checkpoint_file = 'checkpoint.json'
requests_per_batch = 1000
query_limit = 50000
countries = ["AL", "AD", "AM", "AT", "AZ", "BY", "BE", "BA", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "GE", "DE", "GR", "HU", "IS", "IE", "IT", "KZ", "XK", "LV", "LI", "LT", "LU", "MT", "MD", "MC", "ME", "NL", "MK", "NO", "PL", "PT", "RO", "RU", "SM", "RS", "SK", "SI", "ES", "SE", "CH", "TR", "UA", "GB", "VA"]

fields_dict = {
    "clinical_trials" : "abstract+acronym+active_years+altmetric+associated_grant_ids+brief_title+category_bra+category_for+category_for_2020+category_hra+category_hrcs_hc+category_hrcs_rac+category_icrp_cso+category_icrp_ct+category_rcdc+conditions+date_inserted+dimensions_url+end_date+funder_countries+funders+gender+id+interventions+investigators+linkout+mesh_terms+phase+publication_ids+publications+registry+research_orgs+researchers+score+start_date+study_arms+study_designs+study_eligibility_criteria+study_maximum_age+study_minimum_age+study_outcome_measures+study_participants+study_type+title",
    "datasets" : "associated_grant_ids+associated_publication+associated_publication_id+authors+category_bra+category_for+category_for_2020+category_hra+category_hrcs_hc+category_hrcs_rac+category_icrp_cso+category_icrp_ct+category_rcdc+category_sdg+date+date_created+date_embargo+date_inserted+date_modified+description+dimensions_url+doi+figshare_url+funder_countries+funders+id+journal+keywords+language_desc+language_title+license_name+license_url+repository+research_org_cities+research_org_countries+research_org_states+research_orgs+researchers+score+title+year",
    "grants" : "abstract+active_year+category_bra+category_for+category_for_2020+category_hra+category_hrcs_hc+category_hrcs_rac+category_icrp_cso+category_icrp_ct+category_rcdc+category_sdg+category_uoa+concepts+concepts_scores+date_inserted+dimensions_url+end_date+foa_number+funder_org_acronym+funder_org_cities+funder_org_countries+funder_org_name+funder_org_states+funder_orgs+funding_aud+funding_cad+funding_chf+funding_cny+funding_currency+funding_eur+funding_gbp+funding_jpy+funding_nzd+funding_schemes+funding_usd+id+investigators+keywords+language+language_title+linkout+original_title+project_numbers+research_org_cities+research_org_countries+research_org_names+research_org_state_codes+research_orgs+researchers+score+start_date+start_year+title",
    "patents" : "abstract+additional_filters+application_number+assignee_cities+assignee_countries+assignee_names+assignee_state_codes+assignees+associated_grant_ids+category_bra+category_for+category_for_2020+category_hra+category_hrcs_hc+category_hrcs_rac+category_icrp_cso+category_icrp_ct+category_rcdc+claims_amount+cpc+current_assignee_names+current_assignees+date+date_inserted+dimensions_url+expiration_date+family_count+family_id+federal_support+filing_date+filing_status+funder_countries+funders+granted_date+granted_year+id+inventor_names+inventors+ipcr+jurisdiction+kind+legal_status+orange_book+original_assignee_names+original_assignees+priority_date+priority_year+publication_date+publication_ids+publication_year+publications+reference_ids+researchers+score+times_cited+title+year",
    "policy_documents" : "category_bra+category_for+category_for_2020+category_hra+category_hrcs_hc+category_hrcs_rac+category_icrp_cso+category_icrp_ct+category_rcdc+date_inserted+dimensions_url+id+linkout+publication_ids+publications+publisher_org+publisher_org_city+publisher_org_country+publisher_org_state+score+title+year",
    "publications" : "abstract+acknowledgements+altmetric+altmetric_id+arxiv_id+authors+authors_count+book_doi+book_series_title+book_title+category_bra+category_for+category_for_2020+category_hra+category_hrcs_hc+category_hrcs_rac+category_icrp_cso+category_icrp_ct+category_rcdc+category_sdg+category_uoa+clinical_trial_ids+concepts+concepts_scores+date+date_inserted+date_online+date_print+dimensions_url+doi+editors+field_citation_ratio+funder_countries+funders+funding_section+id+isbn+issn+issue+journal+journal_lists+journal_title_raw+linkout+mesh_terms+open_access+pages+pmcid+pmid+proceedings_title+publisher+recent_citations+reference_ids+referenced_pubs+relative_citation_ratio+research_org_cities+research_org_countries+research_org_country_names+research_org_names+research_org_state_codes+research_org_state_names+research_orgs+researchers+resulting_publication_doi+score+source_title+subtitles+supporting_grant_ids+times_cited+title+type+volume+year",
    "reports" : "abstract+authors+category_bra+category_for+category_for_2020+category_hra+category_hrcs_hc+category_hrcs_rac+category_icrp_cso+category_icrp_ct+category_rcdc+category_sdg+category_uoa+concepts+concepts_scores+date+date_inserted+doi+external_ids+funder_details+funder_orgs+funder_orgs_countries+id+keywords+linkout+publisher_details+publisher_orgs+publisher_orgs_countries+research_org_cities+research_org_countries+research_org_states+research_orgs+responsible_orgs+responsible_orgs_cities+responsible_orgs_countries+responsible_orgs_details+responsible_orgs_states+score+title+year",
    "source_titles" : "id+issn+issn_electronic+issn_print+journal_lists+linkout+score+sjr+snip+start_year+title+type",
}



# FUNCTIONS

def read_checkpoint(s3_client, bucket_name, checkpoint_file):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=checkpoint_file)
        checkpoint_data = json.loads(response['Body'].read().decode('utf-8'))
        return checkpoint_data
    except s3_client.exceptions.NoSuchKey:
        # If the checkpoint file does not exist, return None or an empty dictionary
        return None

def get_total_records(dimensions_client, data_type):
    # Replace with your method of fetching data from the API
    data = dimensions_client.query(f"search {data_type} return {data_type}[id] limit 1")
    return data.data['_stats']['total_count']

def retry_if_timeout_error(exception):
    """Return True if we should retry (in this case when it's a 408 Timeout Error), False otherwise"""
    return isinstance(exception, HTTPError) and exception.response.status_code == 408

#@retry(stop_max_attempt_number=3, wait_fixed=10000, retry_on_exception=retry_if_timeout_error)
def process_batch(s3_client, dimensions_client, data_type, country, year, checkpoint_data, bucket_name):
    max_retries = 3
    retry_delay = 10 #seconds

    conditions_dict = {
        "clinical_trials" : f"(funder_countries=\"{country}\" and (start_date>=\"{str(year)}-01-01\" and start_date<=\"{str(year)}-12-31\"))",
        "datasets" : f"(funder_countries=\"{country}\" or research_org_countries=\"{country}\") and year={year}",
        "grants" : f"(funder_org_countries=\"{country}\" or research_org_countries=\"{country}\") and start_year={year}",
        "patents" : f"(assignee_countries=\"{country}\" or funder_countries=\"{country}\" or inventor_countries=\"{country}\") and year={year})",
        "policy_documents" : f"(publisher_org_country=\"{country}\" and year={year})",
        "publications" : f"(funder_countries=\"{country}\" or research_org_countries=\"{country}\") and year={year}",
        "reports" : f"(funder_orgs_countries=\"{country}\" or publisher_orgs_countries=\"{country}\" or research_org_countries=\"{country}\" or responsible_orgs_countries=\"{country}\") and year={year}",
        "source_titles" : f"(start_year={year})"
    }

    for attempt in range(max_retries):
        try:
            query = f"search {data_type} where {conditions_dict[data_type]} return {data_type}[{fields_dict[data_type]}] limit {requests_per_batch} skip {checkpoint_data['types_progress'][data_type]['skip']}"
            #logging.info(f"Querying {data_type} for {country}, {year} with query: \r\n{query}")
            data = dimensions_client.query(query)

            if 'errors' in data.data.keys(): 
                error = data['errors']['query']['header']
                logging.warning(f"Error {data_type}, {country}, {year}. Error: {error}")
                checkpoint_data["error_queries"].append({'type': data_type, 'country': country, 'year': year, 'query': query, 'error': error})
                return  # Skip further processing for this batch

            # Check query size
            data_count = data.data['_stats']['total_count']
            checkpoint_data["types_progress"][data_type]["total_records"] = data_count

            if data_count > query_limit:
                logging.warning(f"Query size for {data_type}, {country}, {year} exceeds 50,000 records")
                checkpoint_data["oversized_queries"].append({'type': data_type, 'country': country, 'year': year, 'query': query})
                return  # Skip further processing for this batch
            
            if data_count == 0:
                logging.info(f"No records found for {data_type}, {country}, {year}")
                return  # Skip further processing for this batch

            # Convert data to JSONL string
            jsonl_data = "\n".join(json.dumps(record) for record in data.data[data_type])
            
            # Upload to S3
            path = f"{data_type}/{data_type}_{country}_{year}_{checkpoint_data['types_progress'][data_type]['batch_number']}.jsonl"
            s3_client.put_object(Body=jsonl_data, Bucket=bucket_name, Key=path)
            logging.info(f"Successfully uploaded {data_type}, batch {checkpoint_data['types_progress'][data_type]['batch_number']} to S3 at {path}")
            break  # Break out of retry loop if successful

        except HTTPError as http_err:
            if http_err.response.status_code == 408:
                logging.warning(f"Timeout error for {data_type}, {country}, {year} on attempt {attempt + 1}: {http_err}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)  # Wait before retrying
                    continue
                else:
                    logging.warning(f"Skipping due to error for {data_type}, {country}, {year}. Error: 408")
                    checkpoint_data["error_queries"].append({'type': data_type, 'country': country, 'year': year, 'query': query, 'error': 408})
                    continue  # Skip further processing for this batch
            elif http_err.response.status_code == 502:
                logging.warning(f"502 Bad Gateway error for {data_type}, {country}, {year} on attempt {attempt + 1}: {http_err}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)  # Wait before retrying
                    continue
                else:
                    sns.publish(
                        #PhoneNumber=phone_number,
                        TopicArn=topic_arn,
                        Subject='Script Stopping - 502 Error',
                        MessageStructure='string',
                        Message=f"Script Stopping. 502 Bad Gateway error for {data_type}, {country}, {year} on attempt {attempt + 1}: {http_err}"
                    )
                    raise  # Reraise the exception after final attempt
            else:
                logging.error(f"HTTP error processing batch {checkpoint_data['types_progress'][data_type]['batch_number']} for {data_type}: {http_err}")
                sns.publish(
                    #PhoneNumber=phone_number,
                    TopicArn=topic_arn,
                    Subject=f'Script Stopping - {http_err} Error',
                    MessageStructure='string',
                    Message=f"Script Stopping. HTTP error processing batch {checkpoint_data['types_progress'][data_type]['batch_number']} for {data_type}: {http_err}"
                )
                raise

        except Exception as e:
            logging.error(f"Error processing batch {checkpoint_data['types_progress'][data_type]['batch_number']} for {data_type}: {e}")
            sns.publish(
                #PhoneNumber=phone_number,
                TopicArn=topic_arn,
                Subject='Script Stopping - Unhandled Error',
                MessageStructure='string',
                Message=f"Script Stopping. Error processing batch {checkpoint_data['types_progress'][data_type]['batch_number']} for {data_type}: {e}"
            )
            raise  # Reraise the exception for other errors

def save_to_s3(s3_client, data, bucket_name, path):
    s3_client.put_object(Body=data, Bucket=bucket_name, Key=path)

def update_checkpoint(s3_client, checkpoint_data, bucket_name, checkpoint_file):
    # Update checkpoint file in S3
    s3_client.put_object(
        Body=json.dumps(checkpoint_data, indent=4), 
        Bucket=bucket_name, 
        Key=checkpoint_file
    )

# MAIN
# Configure logging to display messages to the console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read the checkpoint file
logging.info(f"Reading checkpoint file {checkpoint_file} from S3 bucket {bucket_name}")
checkpoint_data = read_checkpoint(s3_client=s3, bucket_name=bucket_name, checkpoint_file=checkpoint_file)
if not checkpoint_data:
    raise Exception("Checkpoint file not found or is empty.")

for data_type in checkpoint_data["types_progress"]:
    if checkpoint_data["types_progress"][data_type]["completed"]:
        continue

    current_type = data_type
    progress = checkpoint_data["types_progress"][current_type]
    min_year = progress.get("min_year", 2000)  # Default to 2000 if not set
    max_year = progress.get("max_year", 2023)  # Default to current year if not set

    last_processed_country = progress.get("last_processed_country", None)
    last_processed_year = progress.get("current_year", None)
    #progress['total_records'] = get_total_records(dsl, current_type)
    #logging.info(f"Processing {progress['total_records']} records for {current_type}")

    for year in range(min_year, max_year + 1): # fyi - range is exclusive of the last number
        if year < last_processed_year:
            continue
        #checkpoint_data["types_progress"][current_type]["current_year"] = year
        for country in countries:
            # Skip countries that have already been processed for the current year
            if year == last_processed_year and last_processed_country is not None and countries.index(country) <= countries.index(last_processed_country):
                continue
            #checkpoint_data["types_progress"][current_type]["current_country"] = country
            while not progress["completed"]:
                start_time = time.time()
                
                process_batch(s3_client=s3, dimensions_client=dsl, data_type=current_type, checkpoint_data=checkpoint_data, country=country, year=year, bucket_name=bucket_name)

                # Update checkpoint data and rate limit handling
                progress["batch_number"] += 1
                progress["skip"] += requests_per_batch  # Adjust based on batch size
                checkpoint_data["types_progress"][current_type] = progress
                checkpoint_data["current_type"] = current_type  # Update current type being processed
                if progress["skip"] >= progress["total_records"]:
                    progress["completed"] = True
                update_checkpoint(s3_client=s3, bucket_name=bucket_name, checkpoint_file=checkpoint_file, checkpoint_data=checkpoint_data)

                # Rate limiting
                elapsed_time = time.time() - start_time
                if elapsed_time < 0.6:  # Adjust to manage 100 requests per minute
                    logging.info(f"Sleeping for {0.6 - elapsed_time} seconds to manage rate limit")
                    time.sleep(0.6 - elapsed_time)

                logging.info(f"Completed batch {progress['batch_number']-1} for {current_type} for country: {country} for year: {year} in {elapsed_time} seconds")

            # After completing all batches for the country-year, update the checkpoint
            progress["last_processed_country"] = country
            progress["current_year"] = year
            progress["completed"] = False  # Reset for the next country-year
            progress["batch_number"] = 0
            progress["skip"] = 0
            update_checkpoint(s3_client=s3, bucket_name=bucket_name, checkpoint_file=checkpoint_file, checkpoint_data=checkpoint_data)

        # Log completion of a year for a data type
        logging.info(f"Completed processing {current_type} for the year {year}")

        # Reset last processed country after completing a year
        progress["last_processed_country"] = None
    
     # Mark type as completed after processing all countries and years
    progress["completed"] = True
    update_checkpoint(s3_client=s3, bucket_name=bucket_name, checkpoint_file=checkpoint_file, checkpoint_data=checkpoint_data)
    logging.info(f"Completed processing all countries and years for {current_type}")
    sns.publish(
        #PhoneNumber=phone_number,
        TopicArn=topic_arn,
        Subject=f'{current_type} Complete',
        MessageStructure='string',
        Message=f"Completed processing all countries and years for {current_type}"
    )