# import pandas as pd
# import json

# # Load CSV files into DataFrames
# events_df = pd.read_csv('events_info.csv')
# companies_df = pd.read_csv('company_info.csv')
# contacts_df = pd.read_csv('company_contact_info.csv')
# attendees_df = pd.read_csv('event_attendees.csv')
# employees_df = pd.read_csv('company_employee_info.csv')

# def getFilteredData(filters): 
#     df_map = {
#         "events_info": events_df,
#         "company_info": companies_df,
#         "company_contact_info": contacts_df,
#         "event_attendees": attendees_df,
#         "employee_info": employees_df
#     }

#     dfs_to_filter_on = list(filters["company_info"])

#     print(dfs_to_filter_on, "KAKAKK")

#     df_to_filter_on = list(filters.keys())[0]
#     columns_to_filter_on = list(filters[df_to_filter_on].keys())[0]
#     filter_to_search_on = list(filters[df_to_filter_on].values())[0]
#     df = df_map.get(df_to_filter_on)

#     is_events_info_df = df_to_filter_on == "events_info"
#     entity_url = "event_url" if is_events_info_df else "company_url"

#     if df is None:
#         print(f"No DataFrame found for filter type: {df_to_filter_on}")
#         return None
    
#     # Apply the filter to the DataFrame
#     if columns_to_filter_on in df.columns:
#         filtered_df = df[df[columns_to_filter_on] == filter_to_search_on]
#         list_entity_urls = filtered_df[entity_url].tolist()
#         return get_event_attendees(list_entity_urls, filtered_df, is_events_info_df, df_to_filter_on)
#     else:
#         print(f"Column {columns_to_filter_on} not found in DataFrame for filter type: {df_to_filter_on}")
#         return None
    
# def get_event_attendees(list_entity_urls, filtered_df, is_events_info_df, df_to_filter_on):
#     print(list_entity_urls)
#     string_url_column_to_search_on = "event_url" if is_events_info_df else "company_url"
#     # Get data from event attendees based on the event URLs
#     filtered_attendees = attendees_df[attendees_df[string_url_column_to_search_on].isin(list_entity_urls)]
#     company_urls = filtered_attendees["company_url"].unique().tolist()
#     if is_events_info_df:
#         # Get companies that match the filtered attendees
#         filtered_companies = companies_df[companies_df["company_url"].isin(company_urls)]

#         # Get employees that work for the filtered companies
#         filtered_employees = employees_df[employees_df["company_url"].isin(company_urls)]
#         print("hahah",filtered_df, filtered_companies, filtered_employees)
        
#         result_json = {
#             "events_info": filtered_df.to_dict(orient="records"),
#             "company_info": filtered_companies.to_dict(orient="records"),
#             "employee_info": filtered_employees.to_dict(orient="records")
#         }
#         return result_json

#     else:
#         filtered_companies = None
#         is_employees_df = True if df_to_filter_on == "employee_info" else False
#         if(is_employees_df):
#             filtered_companies = companies_df[companies_df["company_url"].isin(company_urls)]
#         # Get events URLs from the filtered attendees
#         event_urls = filtered_attendees["event_url"].unique().tolist()

#         # Get companies that match the filtered attendees
#         filtered_events = events_df[events_df["event_url"].isin(event_urls)]

#         # Get employees that work for the filtered companies
#         filtered_employees = employees_df[employees_df["company_url"].isin(company_urls)]
#         print("hahah",filtered_df, filtered_events, filtered_employees)

#         result_json = {
#             "events_info": filtered_events.to_dict(orient="records"),
#             "company_info": filtered_companies.to_dict(orient="records") if is_employees_df else filtered_df.to_dict(orient="records"),
#             "employee_info": filtered_employees.to_dict(orient="records") if not is_employees_df else filtered_df.to_dict(orient="records")
#         }
#         return result_json
    
# # print(getFilteredData({"company_info": {
# #     "company_country": "USA"
# # }}))