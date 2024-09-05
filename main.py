import pandas as pd
import json

# Load CSV files into DataFrames
events_df = pd.read_csv('events_info.csv')
companies_df = pd.read_csv('company_info.csv')
contacts_df = pd.read_csv('company_contact_info.csv')
attendees_df = pd.read_csv('event_attendees.csv')
employees_df = pd.read_csv('company_employee_info.csv')

df_map = {
    "events_info": events_df,
    "company_info": companies_df,
    "company_contact_info": contacts_df,
    "event_attendees": attendees_df,
    "employee_info": employees_df
}

def getFilteredDataMultiple(filters):
    company_info_filters = list(filters.get("company_info", [])) if "company_info" in filters else []
    events_info_filters = list(filters.get("events_info", [])) if "events_info" in filters else []
    employee_info_filters = list(filters.get("employee_info", [])) if "employee_info" in filters else []

    # apply filters
    if company_info_filters:
        filtered_items_by_companies = getFilteredItems(df_map["company_info"],"company_info", company_info_filters)
    else:
        filtered_items_by_companies = pd.DataFrame()

    if events_info_filters:
        filtered_items_by_events = getFilteredItems(df_map["events_info"], "events_info", events_info_filters)
    else:
        filtered_items_by_events = pd.DataFrame()

    if employee_info_filters:
        filtered_items_by_employees = getFilteredItems(df_map["employee_info"], "employee_info", employee_info_filters)
    else:
        filtered_items_by_employees = pd.DataFrame()

    # Handle empty DataFrames smartly
    combined_data_for_companies = None
    combined_data_for_events = None
    combined_data_for_employees = None

    if not filtered_items_by_companies.empty:
        combined_data_for_companies = getDataUsingEventAttendees("company_info", filtered_items_by_companies, True)

    if not filtered_items_by_events.empty:
        combined_data_for_events = getDataUsingEventAttendees("events_info", filtered_items_by_events, False)

    if not filtered_items_by_employees.empty:
        combined_data_for_employees = getDataUsingEventAttendees("employee_info", filtered_items_by_employees, True)

    does_company_filter_exist = True if company_info_filters else False
    print(does_company_filter_exist)

    final_companies = combine_and_drop_duplicates(
        "company_info",
        combined_data_for_companies or pd.DataFrame(),
        combined_data_for_events or pd.DataFrame(),
        combined_data_for_employees or pd.DataFrame()
    )

    final_events = combine_and_drop_duplicates(
        "events_info",
        combined_data_for_companies or pd.DataFrame(),
        combined_data_for_events or pd.DataFrame(),
        combined_data_for_employees or pd.DataFrame()
    )

    final_employees = combine_and_drop_duplicates(
        "employee_info",
        combined_data_for_companies or pd.DataFrame(),
        combined_data_for_events or pd.DataFrame(),
        combined_data_for_employees or pd.DataFrame()
    )
    print("FINAL", type(final_companies))
    
    final_filtered_companies = getFilteredItems(final_companies, "company_info", company_info_filters)
    final_filtered_events = getFilteredItems(final_events, "events_info", events_info_filters)
    final_filtered_employees = getFilteredItems(final_employees, "employee_info", employee_info_filters)

    return {
        "events_info": final_filtered_events.to_dict(orient="records"),
        "company_info": final_filtered_companies.to_dict(orient="records"),
        "employee_info": final_filtered_employees.to_dict(orient="records"),
    }



def combine_and_drop_duplicates(df_key, df_one, df_two, df_three):
    print("ataha", type(df_one), type(df_two), type(df_three))

    data_from_df_one = pd.DataFrame()
    data_from_df_two = pd.DataFrame()
    data_from_df_three = pd.DataFrame()

    if(data_from_df_one is not True):
        data_from_df_one = df_one.get(df_key, pd.DataFrame())
    
    if(data_from_df_two is not True):
        data_from_df_two = df_two.get(df_key, pd.DataFrame())
    
    if(data_from_df_three.empty is not True):
        data_from_df_three = df_three.get(df_key, pd.DataFrame())

    # Combine dataframes
    combined_df = pd.concat([data_from_df_one, data_from_df_two, data_from_df_three], ignore_index=True)
    # remove duplicates here
    combined_df = combined_df.drop_duplicates(subset="id")

    return combined_df

def getFilteredItems(df, df_key, filters):
    filter_conditions = []
    
    for filter in filters:
        column = filter['key']
        value = filter['value']
        
        # Create condition for this filter
        condition = df[column] == value
        filter_conditions.append(condition)
    
    # Combine all conditions with '&' (AND operator)
    if filter_conditions:
        combined_condition = filter_conditions[0]
        for condition in filter_conditions[1:]:
            combined_condition &= condition
        
        filtered_df = df[combined_condition]
    else:
        filtered_df = df
    
    return filtered_df

def getDataUsingEventAttendees(df_key, filtered_df, is_company_info):
    url_column = "company_url" if is_company_info else "event_url"

    entity_urls = filtered_df[url_column].tolist()
    event_attendees_df = df_map['event_attendees']

    filtered_attendees = event_attendees_df[event_attendees_df[url_column].isin(entity_urls)]

    company_urls = filtered_attendees["company_url"].unique().tolist()
    event_urls = filtered_attendees["event_url"].unique().tolist()

    if(df_key == "company_info"):
        filtered_events = events_df[events_df["event_url"].isin(event_urls)]
        filtered_employees = employees_df[employees_df["company_url"].isin(company_urls)]
        print("TAHA 1")


        result = {
            "events_info": filtered_events,
            "company_info": filtered_df,
            "employee_info": filtered_employees
        }
        return result
    elif(df_key == "employee_info"):
        filtered_events = events_df[events_df["event_url"].isin(event_urls)]
        filtered_companies = companies_df[companies_df["company_url"].isin(company_urls)]
        print("TAHA 2")

        result = {
            "events_info": filtered_events,
            "company_info": filtered_companies,
            "employee_info": filtered_df
        }
        return result
    elif(df_key == "events_info"):
        print("TAHA 3")
        filtered_employees = employees_df[employees_df["company_url"].isin(company_urls)]
        filtered_companies = companies_df[companies_df["company_url"].isin(company_urls)]

        result = {
            "events_info": filtered_df,
            "company_info": filtered_companies,
            "employee_info": filtered_employees
        }
        return result

    return filtered_attendees

data = getFilteredDataMultiple(
   {
    "company_info": [
        {
            "key": "company_country",
            "value": "India"
        }
    ],
    "events_info": [
        {
            "key": "event_country",
            "value": "Japan"
        }
    ]
}
)

# print(data)