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

    events = filtered_items_by_events
    companies = filtered_items_by_companies
    employees = filtered_items_by_employees

    print(events, companies)
    filteredItems = {
        "events_info": events,
        "company_info": companies,
        "employee_info": employees
    }

    data = getDataUsingEventAttendees(filteredItems)
    # print(type(data), data)
    return data

import pandas as pd

def getFilteredItems(df, df_key, filters, condition_type="AND"):
    filter_conditions = []

    # Group filters by column to handle multiple values
    column_filters = {}
    for filter in filters:
        column = filter['key']
        value = filter['value']
        
        if column not in column_filters:
            column_filters[column] = []
        column_filters[column].append(value)

    # Create conditions for each column
    for column, values in column_filters.items():
        if len(values) > 1:
            condition = df[column].isin(values)
        else:
            condition = df[column] == values[0]
        filter_conditions.append(condition)
    
    # Combine conditions based on condition_type
    if filter_conditions:
        if condition_type == "AND":
            combined_condition = filter_conditions[0]
            for condition in filter_conditions[1:]:
                combined_condition &= condition
        elif condition_type == "OR":
            combined_condition = filter_conditions[0]
            for condition in filter_conditions[1:]:
                combined_condition |= condition
        else:
            raise ValueError("Invalid condition type. Use 'AND' or 'OR'.")
        
        filtered_df = df[combined_condition]
    else:
        filtered_df = df
    
    return filtered_df

def get_entity_urls(df, entity):
    if(df.empty):
        return []
    else:
        return df[entity].tolist()

def handleCompanyAndEmployeeUrls(company_urls, employee_company_urls):
    if len(company_urls) > 0 and len(employee_company_urls) == 0:
        return company_urls
    elif len(company_urls) == 0 and len(employee_company_urls) > 0:
        return employee_company_urls
    else:
        # Find the intersection of both lists and return it as a list
        common_urls = list(set(company_urls).intersection(employee_company_urls))
        return common_urls


def getDataUsingEventAttendees(data):
    # filtered data frames
    events = data["events_info"]
    companies = data["company_info"]
    employees = data["employee_info"]

    companies_df = df_map["company_info"]
    employees_df = df_map["employee_info"]
    events_df = df_map["events_info"]

    print(companies,"TAHA MAZARI")

    # instantiate event attendees df
    event_attendees_df = df_map['event_attendees']
    filtered_event_attendees_df = pd.DataFrame()

    # get entity urls
    event_urls = get_entity_urls(events, "event_url")
    company_urls = get_entity_urls(companies, "company_url")
    employee_company_urls = get_entity_urls(employees, "company_url")

    print("AKAKAK",events)

    combined_employee_and_company_urls = handleCompanyAndEmployeeUrls(company_urls, employee_company_urls)

    if(len(event_urls) > 0 and len(combined_employee_and_company_urls) == 0):
        print("CAME HERE 1")
        filtered_event_attendees_df = event_attendees_df[event_attendees_df["event_url"].isin(event_urls)]
        event_attendees_company_urls = filtered_event_attendees_df["company_url"].tolist()

        filtered_companies = companies_df[companies_df["company_url"].isin(event_attendees_company_urls)]
        filtered_employees = employees_df[employees_df["company_url"].isin(event_attendees_company_urls)]
        
        return {
            "events_info": events.to_dict(orient="records"),
            "company_info": filtered_companies.to_dict(orient="records"),
            "employee_info": filtered_employees.to_dict(orient="records")
        }
    elif(len(combined_employee_and_company_urls) > 0 and len(event_urls) == 0):
        print("CAME HERE 2")
        filtered_event_attendees_df = event_attendees_df[event_attendees_df["company_url"].isin(combined_employee_and_company_urls)]
        event_attendees_event_urls = filtered_event_attendees_df["event_url"].tolist()

        filtered_events = events_df[events_df["event_url"].isin(event_attendees_event_urls)]
        
        # this line is a game changer
        filtered_companies = companies_df[companies_df["company_url"].isin(combined_employee_and_company_urls)]
        filtered_employees = employees_df[employees_df["company_url"].isin(combined_employee_and_company_urls)]

        return {
            "events_info": filtered_events.to_dict(orient="records"),
            "company_info": filtered_companies.to_dict(orient="records"),
            "employee_info": filtered_employees.to_dict(orient="records")
        }
    elif(len(event_urls) > 0 and len(combined_employee_and_company_urls) > 0):
        print("CAME HERE 3")
        # Merge DataFrames to find matching event URLs
        merged_events = pd.merge(event_attendees_df, events, left_on='event_url', right_on='event_url', how='inner')

        # Merge the result with companies to find matching company URLs
        merged_all = pd.merge(merged_events, companies, left_on='company_url', right_on='company_url', how='inner')

        # get merged entity urls
        merged_event_urls = get_entity_urls(merged_all, "event_url")
        merged_company_urls = get_entity_urls(companies, "company_url")

        filtered_events = events_df[events_df["event_url"].isin(merged_event_urls)]
        filtered_companies = companies_df[companies_df["company_url"].isin(merged_company_urls)]
        filtered_employees = employees_df[employees_df["company_url"].isin(merged_company_urls)]

        final_employees = filtered_employees

        if(not employees.empty):
            final_employees = pd.merge(filtered_employees, employees, on=list(filtered_employees.columns), how="inner")

        return {
            "events_info": filtered_events.to_dict(orient="records"),
            "company_info": filtered_companies.to_dict(orient="records"),
            "employee_info": final_employees.to_dict(orient="records")
        }

    return True

data = getFilteredDataMultiple(
{
    "company_info": [
        {
            "key": "company_industry",
            "value": "Technology"
        },
        {
            "key": "company_industry",
            "value": "Automative"
        },
        {
            "key": "company_industry",
            "value": "Automotive"
        },
        {
            "key": "company_industry",
            "value": "Oil & Gas"
        },
        {
            "key": "company_country",
            "value": "USA"
        }
    ],
    "events_info": [
        {
            "key": "event_city",
            "value": "London"
        },
        {
            "key": "event_city",
            "value": "San Francisco"
        }
        ,
        {
            "key": "event_city",
            "value": "Sans"
        }
    ],
    "employee_info": [
        {
            "key": "person_seniority",
            "value": "Manager"
        },
        {
            "key": "person_seniority",
            "value": "Manager"
        }
    ]
}
)